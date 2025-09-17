// jobs/aggregateOracleStalls.js
// Detect stalled oracle submissions (per validator per contract).
// ✅ Catches *inactive* validators (no recent samples) as stalled.
// ✅ Uses datasource medians to judge market movement (consistent with DS stalls).
// ✅ Populates oracle_stall_state + oracle_health_rollup for summaries.
// ✅ Owner alerts (per-validator) with optional realtime DM on transitions.
// Call right after your oracle snapshot ingest finishes: await aggregateOracleStalls(client, runId)

require('dotenv').config();

const { getDb } = require('../db');
const db = getDb();

/* ---------------- global debug ---------------- */
const on = v => ['1','true','yes','on'].includes(String(v).toLowerCase());
const DEBUG = on(process.env.DEBUG_ALL);
const log = (...a) => { if (DEBUG) console.log(...a); };

/* ---------------- deps ---------------- */
const {
  // per-validator owner alerts
  selOpenAlertByValidator, insAlertForValidator, resolveAlertById,
  // shared lookups
  selContractLabel,
  // market move basis: datasource prices by run
  selPricesForRuns,
} = require('../db/statements');

const { bumpOracleRollup } = require('../services/rollup');
const { isAdmin, disableDMIfNonAdmin } = require('../utils/dmPolicy');
const { sendWindowSummariesIfDue } = require('../services/windowSummary');

/* ---------------- config / thresholds ---------------- */
const { getAggregationConfig, logActiveAggregationConfig } = require('../config/thresholds');
const AGG = getAggregationConfig();
logActiveAggregationConfig(AGG);
const {
  FRESHNESS_SEC,
  STALL_FLAT_PCT,
  STALL_MARKET_MOVE_PCT,
  STALL_MIN_SPAN_SEC,
} = AGG;

// Consecutive-hit thresholds (support both env names)
function pickInt(names, def) {
  for (const n of names) {
    const raw = process.env[n];
    if (raw != null && String(raw).trim() !== '') {
      const v = Number(raw);
      if (Number.isFinite(v) && v >= 0) return v;
    }
  }
  return def;
}
const OPEN_HITS  = pickInt(['ORACLE_STALL_OPEN_CONSEC','ORACLE_STALL_HITS_OPEN'], 3);
const CLEAR_HITS = pickInt(['ORACLE_STALL_CLEAR_CONSEC','ORACLE_STALL_HITS_CLEAR'], 2);

// Optional realtime owner DMs on transitions (default OFF; your .env sets 0)
const ORACLE_REALTIME_DM = on(process.env.ORACLE_REALTIME_DM);

/* ---------------- statements (local) ---------------- */

// All active contracts
const selActiveContracts = db.prepare(`
  SELECT chain_id, address AS contract_address
  FROM contracts
  WHERE active = 1
`);

// All validators attached to a contract (even if inactive)
const selValidatorsForContract = db.prepare(`
  SELECT validator_address
  FROM validator_contracts
  WHERE chain_id = ? AND contract_address = ?
`);

// Last 3 oracle snapshots for one validator@contract (newest→oldest)
const selOracleLast3 = db.prepare(`
  SELECT run_id, timestamp, price
  FROM oracle_price_snapshots
  WHERE chain_id = ?
    AND contract_address = ?
    AND validator_address = ?
  ORDER BY timestamp DESC
  LIMIT 3
`);

// Most recent oracle snapshot (for inactivity age check)
const selOracleLast1 = db.prepare(`
  SELECT run_id, timestamp, price
  FROM oracle_price_snapshots
  WHERE chain_id = ?
    AND contract_address = ?
    AND validator_address = ?
  ORDER BY timestamp DESC
  LIMIT 1
`);

// Owners of a validator
const selOwnersForValidator = db.prepare(`
  SELECT u.discord_id, u.discord_name, u.accepts_dm
  FROM validator_owners vo
  JOIN users u ON u.discord_id = vo.discord_id
  WHERE vo.chain_id = ? AND vo.validator_address = ?
`);

// Stall state
const selOracleStallState = db.prepare(`
  SELECT status, consec_bad, consec_good, first_bad_run_id, last_seen_run_id
  FROM oracle_stall_state
  WHERE chain_id = ? AND contract_address = ? AND validator_address = ?
`);

const insOracleStallState = db.prepare(`
  INSERT INTO oracle_stall_state
    (chain_id, contract_address, validator_address, status, consec_bad, consec_good, first_bad_run_id, last_seen_run_id)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`);

const updOracleStallState = db.prepare(`
  UPDATE oracle_stall_state
  SET status = ?, consec_bad = ?, consec_good = ?, first_bad_run_id = ?, last_seen_run_id = ?
  WHERE chain_id = ? AND contract_address = ? AND validator_address = ?
`);

const updAlertExtra = db.prepare(`UPDATE alerts SET extra = ? WHERE id = ?`);

/* ---------------- tiny helpers ---------------- */
function median(nums) { if (!nums.length) return NaN; const a=[...nums].sort((x,y)=>x-y); const m=Math.floor(a.length/2); return a.length%2?a[m]:(a[m-1]+a[m])/2; }
function mean(nums)   { return nums.length ? nums.reduce((s,x)=>s+x,0)/nums.length : NaN; }
function pctDiff(a,b) { if (!Number.isFinite(a)||!Number.isFinite(b)||b===0) return Infinity; return Math.abs(a-b)/Math.abs(b); }
const fmtPct = (n,d=2)=> !Number.isFinite(n) ? 'n/a' : `${(n*100).toFixed(d)}%`;
function fmtSpan(sec){ if(!Number.isFinite(sec))return'n/a'; if(sec<3600)return `${Math.round(sec/60)}m`; return `${(sec/3600).toFixed(1)}h`; }
const nowISO = () => new Date().toISOString();

function getPairLabel(chain_id, contract_address){
  const r = selContractLabel.get(chain_id, contract_address);
  return r && r.label ? r.label : null;
}

function safeJson(s) { try { return JSON.parse(s); } catch { return {}; } }

/* ---------------- alert helpers (per owner) ---------------- */
function openOrUpdateOwnerAlert({ discord_id, chain_id, contract_address, validator_address, severity, message, extra }) {
  const alertType = 'ORACLE_STALL';
  const open = selOpenAlertByValidator.get(discord_id, chain_id, validator_address, alertType);
  if (!open) {
    insAlertForValidator.run(
      discord_id, chain_id, validator_address, alertType, severity, message,
      JSON.stringify({ contract_address, ...(extra ?? {}) })
    );
    return { opened: true, id: null };
  } else {
    const merged = { ...(open.extra ? safeJson(open.extra) : {}), contract_address, ...(extra || {}) };
    updAlertExtra.run(JSON.stringify(merged), open.id);
    return { opened: false, id: open.id };
  }
}

function resolveOwnerAlert({ discord_id, chain_id, validator_address }) {
  const alertType = 'ORACLE_STALL';
  const open = selOpenAlertByValidator.get(discord_id, chain_id, validator_address, alertType);
  if (open) resolveAlertById.run(open.id);
}

/* ---------------- state helpers ---------------- */
function upsertStateBad({ chain_id, contract_address, validator_address, runId }) {
  const row = selOracleStallState.get(chain_id, contract_address, validator_address);
  if (!row) {
    const hits = 1;
    const status = (hits >= OPEN_HITS) ? 'stalled' : 'candidate';
    insOracleStallState.run(
      chain_id, contract_address, validator_address,
      status, hits, 0, /*first_bad_run_id*/ runId, /*last_seen_run_id*/ runId
    );
    return { prev: null, next: { status, consec_bad: hits, consec_good: 0, first_bad_run_id: runId, last_seen_run_id: runId } };
  }
  const hits = (Number(row.consec_bad) || 0) + 1;
  const firstBad = row.first_bad_run_id ?? row.last_seen_run_id ?? runId;
  const nextStatus = (row.status === 'stalled' || hits >= OPEN_HITS) ? 'stalled' : 'candidate';
  updOracleStallState.run(
    nextStatus, hits, 0, firstBad, runId,
    chain_id, contract_address, validator_address
  );
  return { prev: row, next: { status: nextStatus, consec_bad: hits, consec_good: 0, first_bad_run_id: firstBad, last_seen_run_id: runId } };
}

function upsertStateGood({ chain_id, contract_address, validator_address, runId }) {
  const row = selOracleStallState.get(chain_id, contract_address, validator_address);
  if (!row) {
    insOracleStallState.run(
      chain_id, contract_address, validator_address,
      'ok', 0, 1, null, runId
    );
    return { prev: null, next: { status: 'ok', consec_bad: 0, consec_good: 1, first_bad_run_id: null, last_seen_run_id: runId }, cleared: false };
  }
  const goods = (Number(row.consec_good) || 0) + 1;
  const inBadSequence = row.first_bad_run_id != null;
  const clears = inBadSequence && goods >= CLEAR_HITS;
  const nextStatus = clears ? 'ok' : (row.status === 'ok' ? 'ok' : 'candidate');
  updOracleStallState.run(
    nextStatus, 0, goods, row.first_bad_run_id, runId,
    chain_id, contract_address, validator_address
  );
  return { prev: row, next: { status: nextStatus, consec_bad: 0, consec_good: goods, first_bad_run_id: row.first_bad_run_id, last_seen_run_id: runId }, cleared: clears };
}

/* ---------------- core detection (per validator) ---------------- */
async function detectForValidator(client, runId, chain_id, contract_address, validator_address) {
  const pairLabel = getPairLabel(chain_id, contract_address) || contract_address;

  // Inactivity first: treat "no recent snapshot" as stalled
  const last1 = selOracleLast1.get(chain_id, contract_address, validator_address);
  const lastTsMs = last1 ? Date.parse(last1.timestamp) : NaN;
  const ageSec = Number.isFinite(lastTsMs) ? Math.floor((Date.now() - lastTsMs) / 1000) : Infinity;

  if (ageSec > FRESHNESS_SEC) {
    const state = upsertStateBad({ chain_id, contract_address, validator_address, runId });

    // Rollup (inactivity → dev/span/median may be null)
    bumpOracleRollup({
      runId,
      chain_id,
      contract_address,
      validator_address,
      isStalled: true,
      dev_pct: null,
      span_sec: null,
      median_now: null,
      price: null,
    });

    // Alert/DM only on transition to 'stalled'
    const justBecameStalled = (state.prev?.status !== 'stalled' && state.next.status === 'stalled');
    if (justBecameStalled) {
      const message = `Oracle inactive for ${pairLabel}; last submission > ${Math.round(ageSec/3600)}h ago.`;
      for (const { discord_id, discord_name, accepts_dm } of selOwnersForValidator.all(chain_id, validator_address)) {
        const { opened } = openOrUpdateOwnerAlert({
          discord_id,
          chain_id,
          contract_address,
          validator_address,
          severity: 'warning',
          message,
          extra: {
            reason: 'inactivity',
            last_seen_ts: last1?.timestamp ?? null,
            last_seen_run_id: last1?.run_id ?? null,
            age_sec: ageSec,
            first_bad_run_id: state.next.first_bad_run_id,
            consecutive: state.next.consec_bad,
          }
        });

        if (ORACLE_REALTIME_DM && opened && accepts_dm === 1 && client) {
          try {
            const u = await client.users.fetch(discord_id);
            await u.send(
              [
                `🚨 **Oracle Stalled (Inactive)**`,
                `• Pair: ${pairLabel}`,
                `• Validator: \`${validator_address}\` (${discord_name || discord_id})`,
                `• Last submission: ${last1?.timestamp ?? 'n/a'} (~${Math.round(ageSec/3600)}h ago)`,
                `\u200B`
              ].join('\n')
            );
            console.log(`📣 DM’d owner ${discord_name || discord_id} for inactive oracle @ ${validator_address}`);
          } catch (e) {
            console.warn(`❌ Could not DM owner ${discord_name || discord_id} (inactive): ${e.message}`);
            if (disableDMIfNonAdmin(discord_id) && !isAdmin(discord_id)) {
              console.log(`🔧 accepts_dm -> 0 (Discord 50007; non-admin) for ${discord_id}`);
            }
          }
        }
      }
    }

    if (DEBUG) log('[oracle][inactive-stall]', { chain_id, contract_address, validator_address, ageSec, FRESHNESS_SEC, status: state.next.status });
    return; // inactivity handled; no need to evaluate flat-range
  }

  // Otherwise, evaluate flat range vs market move (needs 3 samples over span)
  const last3 = selOracleLast3.all(chain_id, contract_address, validator_address);
  if (last3.length < 3) return;

  const times = last3.map(r => Date.parse(r.timestamp));
  const spanSec = (Math.max(...times) - Math.min(...times)) / 1000;
  if (spanSec < STALL_MIN_SPAN_SEC) return;

  const prices = last3.map(r => Number(r.price)).filter(Number.isFinite);
  if (prices.length < 3) return;

  const pMin = Math.min(...prices);
  const pMax = Math.max(...prices);
  const pMidBase = (pMin + pMax) / 2;
  const flatRangePct = pMidBase > 0 ? (pMax - pMin) / pMidBase : 0;

  // Market move: use datasource medians per earliest/latest run (consistent basis)
  const earliestRun = last3[last3.length - 1].run_id;
  const latestRun   = last3[0].run_id;
  const marketRows  = selPricesForRuns.all(chain_id, contract_address, earliestRun, latestRun);
  const byRun = new Map();
  for (const r of marketRows) {
    if (!byRun.has(r.run_id)) byRun.set(r.run_id, []);
    byRun.get(r.run_id).push(Number(r.price));
  }
  if (!byRun.has(earliestRun) || !byRun.has(latestRun)) return;

  const medEarly = median((byRun.get(earliestRun) || []).filter(Number.isFinite));
  const medLate  = median((byRun.get(latestRun ) || []).filter(Number.isFinite));
  if (!Number.isFinite(medEarly) || !Number.isFinite(medLate) || medEarly <= 0) return;

  const marketMovePct = Math.abs(medLate - medEarly) / medEarly;
  const isStalled = (flatRangePct <= STALL_FLAT_PCT) && (marketMovePct >= STALL_MARKET_MOVE_PCT);

  if (DEBUG) log('[oracle][eval]', {
    chain_id, contract_address, validator_address,
    spanSec, flatRangePct, marketMovePct,
    STALL_FLAT_PCT, STALL_MARKET_MOVE_PCT, isStalled
  });

  if (isStalled) {
    const stalled_price = mean(prices);
    const median_now    = medLate;
    const dev_pct       = pctDiff(stalled_price, median_now);

    const state = upsertStateBad({ chain_id, contract_address, validator_address, runId });

    // Rollup STALL
    bumpOracleRollup({
      runId,
      chain_id,
      contract_address,
      validator_address,
      isStalled: true,
      dev_pct,
      span_sec: spanSec,
      median_now,
      price: stalled_price,
    });

    // Alert extras refresh for any open alerts
    const owners = selOwnersForValidator.all(chain_id, validator_address);
    for (const { discord_id } of owners) {
      const open = selOpenAlertByValidator.get(discord_id, chain_id, validator_address, 'ORACLE_STALL');
      if (!open) continue;
      const ex = open.extra ? safeJson(open.extra) : {};
      const merged = {
        ...ex,
        contract_address,
        pair: pairLabel,
        stalled_price,
        median_now,
        dev_pct,
        flat_range_pct: STALL_FLAT_PCT,
        market_move_pct: STALL_MARKET_MOVE_PCT,
        span_sec: spanSec,
        first_bad_run_id: state.next.first_bad_run_id,
        last_seen_run_id: runId,
        consecutive: state.next.consec_bad
      };
      updAlertExtra.run(JSON.stringify(merged), open.id);
    }

    const justBecameStalled = (state.prev?.status !== 'stalled' && state.next.status === 'stalled');
    if (justBecameStalled) {
      const message = `Oracle stalled for ${pairLabel} over ~${fmtSpan(spanSec)}; market moved ${fmtPct(marketMovePct)}.`;
      for (const { discord_id, discord_name, accepts_dm } of owners) {
        const { opened } = openOrUpdateOwnerAlert({
          discord_id,
          chain_id,
          contract_address,
          validator_address,
          severity: 'warning',
          message,
          extra: {
            source: 'oracle',
            pair: pairLabel,
            contract_address,
            stalled_price,
            median_now,
            dev_pct,
            flat_range_pct: STALL_FLAT_PCT,
            market_move_pct: STALL_MARKET_MOVE_PCT,
            span_sec: spanSec,
            first_bad_run_id: state.next.first_bad_run_id,
            last_seen_run_id: runId,
            consecutive: state.next.consec_bad
          }
        });

        if (ORACLE_REALTIME_DM && opened && accepts_dm === 1 && client) {
          try {
            const u = await client.users.fetch(discord_id);
            await u.send(
              [
                `🚨 **Oracle Stalled**`,
                `• Pair: ${pairLabel}`,
                `• Validator: \`${validator_address}\` (${discord_name || discord_id})`,
                `• Span: ~${fmtSpan(spanSec)} | Market move: ${fmtPct(marketMovePct)}`,
                `• Price vs median: ${(dev_pct * 100).toFixed(2)}% off`,
                `\u200B`
              ].join('\n')
            );
            console.log(`📣 DM’d owner ${discord_name || discord_id} for oracle stall @ ${validator_address}`);
          } catch (e) {
            console.warn(`❌ Could not DM owner ${discord_name || discord_id} (stall): ${e.message}`);
            if (disableDMIfNonAdmin(discord_id) && !isAdmin(discord_id)) {
              console.log(`🔧 accepts_dm -> 0 (Discord 50007; non-admin) for ${discord_id}`);
            }
          }
        }
      }
    }
  } else {
    // Healthy
    const price_now = mean(prices);
    const dev_now   = pctDiff(price_now, medLate);
    const state = upsertStateGood({ chain_id, contract_address, validator_address, runId });

    // Rollup OK
    bumpOracleRollup({
      runId,
      chain_id,
      contract_address,
      validator_address,
      isStalled: false,
      dev_pct: dev_now,
      span_sec: spanSec,
      median_now: medLate,
      price: price_now,
    });

    if (state.cleared) {
      for (const { discord_id, discord_name, accepts_dm } of selOwnersForValidator.all(chain_id, validator_address)) {
        const open = selOpenAlertByValidator.get(discord_id, chain_id, validator_address, 'ORACLE_STALL');
        if (!open) continue;

        // Update final extras then resolve
        const ex = open.extra ? safeJson(open.extra) : {};
        ex.last_dev_pct = dev_now;
        ex.last_span_sec = spanSec;
        ex.resolved_run_id = runId;
        ex.resolved_at_iso = nowISO();
        try { updAlertExtra.run(JSON.stringify(ex), open.id); } catch {}
        resolveOwnerAlert({ discord_id, chain_id, validator_address });

        if (ORACLE_REALTIME_DM && accepts_dm === 1 && client) {
          try {
            const u = await client.users.fetch(discord_id);
            await u.send(
              [
                `✅ **Oracle Stall Cleared**`,
                `• Pair: ${pairLabel}`,
                `• Validator: \`${validator_address}\` (${discord_name || discord_id})`,
                `\u200B`
              ].join('\n')
            );
            console.log(`📣 DM’d owner ${discord_name || discord_id} oracle stall resolved @ ${validator_address}`);
          } catch (e) {
            console.warn(`❌ Could not DM owner ${discord_name || discord_id} (resolved): ${e.message}`);
            if (disableDMIfNonAdmin(discord_id) && !isAdmin(discord_id)) {
              console.log(`🔧 accepts_dm -> 0 (Discord 50007; non-admin) for ${discord_id}`);
            }
          }
        }
      }
    }
  }
}

/* ---------------- orchestrator ---------------- */
async function aggregateOracleStalls(client, runId) {
  if (!Number.isFinite(runId)) return;

  const t0 = Date.now();
  let validatorsChecked = 0;

  const contracts = selActiveContracts.all();
  if (!contracts.length && DEBUG) log('[oracle] no active contracts');

  for (const { chain_id, contract_address } of contracts) {
    const validators = selValidatorsForContract.all(chain_id, contract_address).map(r => r.validator_address);
    if (!validators.length) continue;

    for (const v of validators) {
      await detectForValidator(client, runId, chain_id, contract_address, v);
      validatorsChecked++;
    }
  }

  if (DEBUG) log(`[oracle] sweep done: validators=${validatorsChecked}, ms=${Date.now() - t0}`);

  // Kick summaries; ledger prevents dupes if both DS+Oracle call this.
  await sendWindowSummariesIfDue(client, runId);
}

module.exports = { aggregateOracleStalls };

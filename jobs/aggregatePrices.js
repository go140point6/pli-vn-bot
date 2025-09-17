// jobs/aggregatePrices.js
// Aggregates datasource prices, filters outliers, detects stalls.
// - Writes price_aggregates
// - OUTLIER:<source> → alerts table only (no DM), open/resolve, rollup note
// - DS_STALL:<source> → alerts table only; rollup bump
//
// Call after a run completes: await aggregateAndDetect(client, runId)

require('dotenv').config();

const { getDb } = require('../db');
const {
  selAdmins,
  selOpenAlert, insAlert, resolveAlertById,
  selActiveContractsWithFreshData,
  selFreshSnapshotsForContract,
  insAggregate,
  selLast3ForSourceContract,
  selPricesForRuns,
  selSourcesForContract,
  selContractLabel,
} = require('../db/statements');

const db = getDb();

/* ---------- global debug (single toggle) ---------- */
// Turn on with DEBUG_ALL=1 (or true/yes/on) in .env
const envFlag = k => ['1','true','yes','on'].includes(String(process.env[k]).toLowerCase());
const DEBUG = envFlag('DEBUG_ALL');
const log = (...args) => { if (DEBUG) console.log(...args); };

/* ---------- thresholds ---------- */
const { getAggregationConfig, logActiveAggregationConfig } = require('../config/thresholds');
const AGG = getAggregationConfig();
if (DEBUG) logActiveAggregationConfig(AGG); // print once when DEBUG_ALL is enabled

const {
  OUTLIER_PCT,
  FRESHNESS_SEC,
  STALL_FLAT_PCT,
  STALL_MARKET_MOVE_PCT,
  STALL_MIN_SPAN_SEC,
  QUORUM_MIN_USED,
} = AGG;

/* ---------- rollup helpers (new rollup.js API) ---------- */
const {
  bumpDatasourceRollup,
  noteDatasourceOutlierHit,
} = require('../services/rollup');

/* ---------- consecutive-run thresholds (env-overridable) ---------- */
const STALL_OPEN_CONSEC  = Number.isFinite(Number(process.env.STALL_OPEN_CONSEC))
  ? Number(process.env.STALL_OPEN_CONSEC) : 3;
const STALL_CLEAR_CONSEC = Number.isFinite(Number(process.env.STALL_CLEAR_CONSEC))
  ? Number(process.env.STALL_CLEAR_CONSEC) : 3;

/* ---------- helpers ---------- */
const toISO = (d) => new Date(d).toISOString();
function median(nums) { if (!nums.length) return null; const a=[...nums].sort((x,y)=>x-y); const m=Math.floor(a.length/2); return a.length%2?a[m]:(a[m-1]+a[m])/2; }
function mean(nums)   { if (!nums.length) return null; return nums.reduce((a,b)=>a+b,0)/nums.length; }
function pctDiff(a,b) { if (!Number.isFinite(a)||!Number.isFinite(b)||b===0) return Infinity; return Math.abs(a-b)/Math.abs(b); }
function fmtPct(n,d=2){ if(!Number.isFinite(n))return'n/a'; return `${(n*100).toFixed(d)}%`; }
function fmtSpan(sec){ if(!Number.isFinite(sec))return'n/a'; if(sec<3600)return `${Math.round(sec/60)}m`; return `${(sec/3600).toFixed(1)}h`; }
function getPairLabel(chain_id, contract_address){ const r = selContractLabel.get(chain_id, contract_address); return r && r.label ? r.label : null; }

/* ---------- admin alerts (DB only here) ---------- */
function openAdminContractAlert({ alertType, chain_id, contract_address, severity, message, extra }) {
  const admins = selAdmins.all();
  const openedIds = [];
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
    if (!open) {
      insAlert.run(discord_id, chain_id, contract_address, alertType, severity, message, extra ?? null);
      openedIds.push(discord_id);
    }
  }
  return openedIds;
}
function resolveAdminContractAlert({ alertType, chain_id, contract_address }) {
  const admins = selAdmins.all();
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
    if (open) resolveAlertById.run(open.id);
  }
}

const updAlertExtra = db.prepare(`UPDATE alerts SET extra = ? WHERE id = ?`);

/* ---------- datasource stall state statements ---------- */
const selDSState = db.prepare(`
  SELECT chain_id, contract_address, datasource_name,
         first_seen_run_id, opened_run_id, last_seen_run_id,
         consec_stalls, consec_ok, is_open
  FROM datasource_stall_state
  WHERE chain_id = ? AND contract_address = ? AND datasource_name = ?
`);

const insDSStateInitial = db.prepare(`
  INSERT INTO datasource_stall_state
    (chain_id, contract_address, datasource_name,
     first_seen_run_id, opened_run_id, last_seen_run_id,
     consec_stalls, consec_ok, is_open)
  VALUES (?, ?, ?, ?, NULL, ?, ?, ?, 0)
`);

const updDSStateOnStall = db.prepare(`
  UPDATE datasource_stall_state
  SET last_seen_run_id = ?,
      consec_stalls    = consec_stalls + 1,
      consec_ok        = 0
  WHERE chain_id = ? AND contract_address = ? AND datasource_name = ?
`);

const updDSStateOnOk = db.prepare(`
  UPDATE datasource_stall_state
  SET last_seen_run_id = ?,
      consec_ok        = consec_ok + 1,
      consec_stalls    = 0
  WHERE chain_id = ? AND contract_address = ? AND datasource_name = ?
`);

const markDSStateOpen = db.prepare(`
  UPDATE datasource_stall_state
  SET is_open = 1, opened_run_id = ?
  WHERE chain_id = ? AND contract_address = ? AND datasource_name = ? AND is_open = 0
`);

const markDSStateClosed = db.prepare(`
  UPDATE datasource_stall_state
  SET is_open = 0
  WHERE chain_id = ? AND contract_address = ? AND datasource_name = ? AND is_open = 1
`);

/* ---------- outliers (open/resolve + rollup note) ---------- */
function manageOutlierAlerts({ runId, chain_id, contract_address, outliers, suppressLog = false }) {
  const nowOutlier = new Set(outliers.map(o => o.source));
  const allSrc = selSourcesForContract.all(chain_id, contract_address).map(r => r.datasource_name);

  for (const source of allSrc) {
    const alertType = `OUTLIER:${source.toLowerCase()}`;
    if (nowOutlier.has(source)) {
      const message = `Source ${source} deviated > ${(OUTLIER_PCT * 100).toFixed(2)}% from median for contract ${contract_address}.`;
      openAdminContractAlert({
        alertType,
        chain_id,
        contract_address,
        severity: 'warning',
        message,
        extra: JSON.stringify({ source, outlier_pct: OUTLIER_PCT }),
      });
    } else {
      resolveAdminContractAlert({ alertType, chain_id, contract_address });
    }
  }

  // Per-outlier rollup + refresh "extra" on open alerts
  if (outliers.length) {
    const admins = selAdmins.all();
    for (const o of outliers) {
      try {
        // NEW API: pass dev_pct (not deviation_pct); median/price unknown → omit
        noteDatasourceOutlierHit({
          runId,
          chain_id,
          contract_address,
          datasource_name: o.source,
          dev_pct: o.deviation,
        });
      } catch (e) {
        console.warn(`↪︎ rollup note failed for outlier ${o.source} @ ${contract_address}: ${e.message}`);
      }

      const alertType = `OUTLIER:${o.source.toLowerCase()}`;
      for (const { discord_id } of admins) {
        const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
        if (!open) continue;
        let extra = {};
        try { extra = open.extra ? JSON.parse(open.extra) : {}; } catch { extra = {}; }
        extra.source = o.source;
        extra.outlier_pct = OUTLIER_PCT;        // threshold
        extra.last_deviation_pct = o.deviation; // observed
        extra.last_run_id = runId;
        updAlertExtra.run(JSON.stringify(extra), open.id);
      }
    }
  }

  if (!suppressLog && outliers.length === 0) {
    log(`✅ No outliers for ${contract_address}@${chain_id}`);
  }
}

/* ---------- aggregation ---------- */
function aggregateContractFromFresh({ runId, chain_id, contract_address, cutoffEpoch }) {
  const rows = selFreshSnapshotsForContract.all(chain_id, contract_address, cutoffEpoch);
  if (!rows.length) return null;

  const bySource = new Map();
  for (const r of rows) {
    if (!bySource.has(r.datasource_name)) {
      bySource.set(r.datasource_name, { price: Number(r.price), timestamp: r.timestamp, run_id: r.run_id });
    }
  }

  const entries = [...bySource.entries()].map(([source, v]) => ({ source, price: v.price, timestamp: v.timestamp, run_id: v.run_id }));
  const allPrices = entries.map(e => e.price).filter(Number.isFinite);
  if (allPrices.length < 1) return null;

  const med = median(allPrices);
  if (!Number.isFinite(med) || med <= 0) return null;

  const used = [], outliers = [];
  for (const e of entries) {
    const dev = pctDiff(e.price, med);
    if (dev > OUTLIER_PCT) outliers.push({ ...e, deviation: dev });
    else used.push(e);
  }

  if (used.length < QUORUM_MIN_USED) {
    console.warn(`⚠️  Insufficient quorum after outlier filter for ${contract_address} on chain ${chain_id}: used=${used.length}, needed=${QUORUM_MIN_USED}`);
    manageOutlierAlerts({ runId, chain_id, contract_address, outliers, suppressLog: true });
    return null;
  }

  const usedPrices = used.map(u => u.price);
  const m = mean(usedPrices);
  const window_start = toISO(Math.min(...used.map(u => Date.parse(u.timestamp))));
  const window_end   = toISO(Math.max(...used.map(u => Date.parse(u.timestamp))));
  const discarded_sources = JSON.stringify(outliers.map(o => o.source));

  insAggregate.run(runId ?? null, chain_id, contract_address, window_start, window_end, med, m, entries.length, used.length, discarded_sources);
  manageOutlierAlerts({ runId, chain_id, contract_address, outliers });

  // NEW: credit OK hits for all *used* (non-outlier) sources this run/window
  for (const u of used) {
    try {
      bumpDatasourceRollup({
      runId,
      chain_id,
      contract_address,
      datasource_name: u.source,
      isStalled: false,
      dev_pct: pctDiff(u.price, med), // deviation from window median
      span_sec: null,                 // not evaluating stall here
      median_now: med,
      price: u.price,
      });
    } catch (e) {
      console.warn(`↪︎ rollup ok-hit bump failed for ${u.source} @ ${contract_address}: ${e.message}`);
    }
  }

  if (outliers.length) {
    const details = outliers.map(o => `${o.source} (${(o.deviation * 100).toFixed(2)}%)`).join(', ');
    console.warn(`⚠️  Outliers dropped for ${contract_address}@${chain_id}: ${details}`);
  }

  return { chain_id, contract_address, median: med, mean: m, source_count: entries.length, used_sources: used.length, window_start, window_end };
}

/* ---------- stall detection (tracks state + bumps rollup) ---------- */
function detectStallsForContract(runId, chain_id, contract_address) {
  const pairLabel = getPairLabel(chain_id, contract_address);
  const sources = selSourcesForContract.all(chain_id, contract_address).map(r => r.datasource_name);

  for (const source of sources) {
    const last3 = selLast3ForSourceContract.all(chain_id, contract_address, source);
    if (last3.length < 3) continue;

    const times = last3.map(r => Date.parse(r.timestamp));
    const spanSec = (Math.max(...times) - Math.min(...times)) / 1000;
    if (spanSec < STALL_MIN_SPAN_SEC) continue;

    const prices = last3.map(r => Number(r.price)).filter(Number.isFinite);
    if (prices.length < 3) continue;

    const pMin = Math.min(...prices);
    const pMax = Math.max(...prices);
    const pMidBase = (pMin + pMax) / 2;
    const flatRangePct = pMidBase > 0 ? (pMax - pMin) / pMidBase : 0;

    const oldestRun = last3[last3.length - 1].run_id;
    const latestRun = last3[0].run_id;
    const marketRows = selPricesForRuns.all(chain_id, contract_address, oldestRun, latestRun);
    const byRun = new Map();
    for (const r of marketRows) {
      if (!byRun.has(r.run_id)) byRun.set(r.run_id, []);
      byRun.get(r.run_id).push(Number(r.price));
    }
    if (!byRun.has(oldestRun) || !byRun.has(latestRun)) continue;

    const medEarly = median(byRun.get(oldestRun).filter(Number.isFinite));
    const medLate  = median(byRun.get(latestRun).filter(Number.isFinite));
    if (!Number.isFinite(medEarly) || !Number.isFinite(medLate) || medEarly <= 0) continue;

    const marketMovePct = Math.abs(medLate - medEarly) / medEarly;
    const isStalled = (flatRangePct <= STALL_FLAT_PCT) && (marketMovePct >= STALL_MARKET_MOVE_PCT);

    // --- state row
    let st = selDSState.get(chain_id, contract_address, source);
    if (!st) {
      const consec_stalls = isStalled ? 1 : 0;
      const consec_ok     = isStalled ? 0 : 1;
      const first_seen    = isStalled ? runId : null;
      insDSStateInitial.run(
        chain_id, contract_address, source,
        first_seen, runId, consec_stalls, consec_ok
      );
      st = selDSState.get(chain_id, contract_address, source);
    } else {
      if (isStalled) {
        updDSStateOnStall.run(runId, chain_id, contract_address, source);
      } else {
        updDSStateOnOk.run(runId, chain_id, contract_address, source);
      }
      st = selDSState.get(chain_id, contract_address, source);
    }

    const alertType = `DS_STALL:${source.toLowerCase()}`;
    let openAtEnd = !!st.is_open;

    if (isStalled) {
      if (!st.is_open && st.consec_stalls >= STALL_OPEN_CONSEC) {
        markDSStateOpen.run(runId, chain_id, contract_address, source);
        openAtEnd = true;

        const message =
          `Source ${source} appears stalled for ${pairLabel ?? contract_address} over ~${fmtSpan(spanSec)}; market moved ${fmtPct(marketMovePct)}.`;

        openAdminContractAlert({
          alertType,
          chain_id,
          contract_address,
          severity: 'warning',
          message,
          extra: JSON.stringify({
            source,
            pair: pairLabel,
            contract_address,
            stalled_price: mean(prices),
            median_now: medLate,
            dev_pct: pctDiff(mean(prices), medLate),
            flat_range_pct: STALL_FLAT_PCT,
            market_move_pct: STALL_MARKET_MOVE_PCT,
            span_sec: spanSec,
            first_seen_run_id: st.first_seen_run_id ?? runId,
            opened_run_id: runId,
            last_seen_run_id: runId,
            consecutive: st.consec_stalls
          })
        });
      }

      // If already open, refresh "extra"
      if (openAtEnd || st.consec_stalls >= STALL_OPEN_CONSEC) {
        const admins = selAdmins.all();
        for (const { discord_id } of admins) {
          const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
          if (!open) continue;
          let extra = {};
          try { extra = open.extra ? JSON.parse(open.extra) : {}; } catch { extra = {}; }
          const priceNow = mean(prices);
          extra.source = source;
          extra.pair = pairLabel;
          extra.contract_address = contract_address;
          extra.stalled_price = priceNow;
          extra.median_now = medLate;
          extra.dev_pct = pctDiff(priceNow, medLate);
          extra.flat_range_pct = STALL_FLAT_PCT;
          extra.market_move_pct = STALL_MARKET_MOVE_PCT;
          extra.span_sec = spanSec;
          extra.first_seen_run_id = st.first_seen_run_id ?? extra.first_seen_run_id ?? runId;
          extra.opened_run_id = extra.opened_run_id ?? st.opened_run_id ?? runId;
          extra.last_seen_run_id = runId;
          extra.consecutive = st.consec_stalls;
          updAlertExtra.run(JSON.stringify(extra), open.id);
        }
      }
    } else {
      // Not stalled: resolve if enough consecutive OK
      if (st.is_open && st.consec_ok >= STALL_CLEAR_CONSEC) {
        markDSStateClosed.run(chain_id, contract_address, source);
        openAtEnd = false;
        resolveAdminContractAlert({ alertType, chain_id, contract_address });
      } else {
        openAtEnd = !!st.is_open;
      }
    }

    // 🔸 bump datasource rollup after state/open/close decisions (NEW API shape)
    try {
      const priceNow = mean(prices);
      bumpDatasourceRollup({
        runId,
        chain_id,
        contract_address,
        datasource_name: source,
        isStalled,
        dev_pct: pctDiff(priceNow, medLate),
        span_sec: spanSec,
        median_now: medLate,
        price: priceNow,
      });
      if (DEBUG) log('[ds-rollup-bump]', {
        chain_id, contract_address, source,
        isStalled, spanSec, priceNow, median_now: medLate
      });
    } catch (e) {
      console.warn(`↪︎ rollup bump failed for ${source} @ ${contract_address}: ${e.message}`);
    }
  }
}

/* ---------- public orchestrator ---------- */
async function aggregateAndDetect(client, runId = null) {
  const cutoffEpoch = Math.floor(Date.now() / 1000) - Number(FRESHNESS_SEC);
  if (DEBUG) log('[aggregateAndDetect:start]', { runId, cutoffEpoch });

  // 1) Aggregate & outliers
  const targets = selActiveContractsWithFreshData.all(cutoffEpoch);
  if (!targets.length) console.log('ℹ️  No fresh snapshots found for active contracts within freshness window.');
  if (DEBUG) log('[aggregate:targets]', { count: targets.length });

  let aggCount = 0;
  for (const t of targets) {
    const res = aggregateContractFromFresh({ runId, chain_id: t.chain_id, contract_address: t.contract_address, cutoffEpoch });
    if (res) aggCount++;
  }
  console.log(`📊 Aggregation complete: wrote ${aggCount} price_aggregate row(s).`);

  // 2) Stall detection (DB only; summaries are scheduled elsewhere)
  for (const t of targets) {
    detectStallsForContract(runId, t.chain_id, t.contract_address);
  }

  console.log('🌀 Datasource stall sweep done.');
  if (DEBUG) log('[aggregateAndDetect:end]', { runId });
}

module.exports = { aggregateAndDetect };

// jobs/aggregatePrices.js
// Aggregates datasource prices, filters outliers, detects stalls.
// - Writes price_aggregates
// - OUTLIER:<source> → alerts table only (no DM), open/resolve
// - DS_STALL:<source> → alerts table + one DM per admin per run with a summary of NEWLY opened stalls
//
// Call after a run completes: await aggregateAndDetect(client, runId)

const { getDb } = require('../db');
const {
  // admins & alerts
  selAdmins, setAcceptsDM,
  selOpenAlert, insAlert, resolveAlertById,
  // aggregation helpers
  selActiveContractsWithFreshData,
  selFreshSnapshotsForContract,
  insAggregate,
  selLast3ForSourceContract,
  selPricesForRuns,
  selSourcesForContract,
  // labels
  selContractLabel,
} = require('../db/statements');

const db = getDb();

// thresholds (prod or TEST_* overrides)
const { getAggregationConfig, logActiveAggregationConfig } = require('../config/thresholds');
const AGG = getAggregationConfig();
logActiveAggregationConfig(AGG);
const {
  OUTLIER_PCT,
  FRESHNESS_SEC,
  STALL_FLAT_PCT,
  STALL_MARKET_MOVE_PCT,
  STALL_MIN_SPAN_SEC,
  QUORUM_MIN_USED,
} = AGG;

/* ----------------- helpers ----------------- */
const toISO = (d) => new Date(d).toISOString();

function median(nums) {
  if (!nums.length) return null;
  const arr = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}
function mean(nums) {
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}
function pctDiff(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return Infinity;
  return Math.abs(a - b) / Math.abs(b);
}
function fmtPct(n, digits = 2) {
  if (!Number.isFinite(n)) return 'n/a';
  return `${(n * 100).toFixed(digits)}%`;
}
function fmtPrice(n, digits = 6) {
  if (!Number.isFinite(n)) return 'n/a';
  return Number(n).toFixed(digits);
}
function fmtSpan(sec) {
  if (!Number.isFinite(sec)) return 'n/a';
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  return `${(sec / 3600).toFixed(1)}h`;
}
function getPairLabel(chain_id, contract_address) {
  const r = selContractLabel.get(chain_id, contract_address);
  return r && r.label ? r.label : null;
}

/* ----------------- alert helpers (admins only) ----------------- */
/** Open an admin alert per admin if not already open.
 *  Returns array of discord_ids for whom a NEW alert was opened now. */
function openAdminContractAlert({ alertType, chain_id, contract_address, severity, message, extra }) {
  const admins = selAdmins.all(); // { discord_id, discord_name, accepts_dm }
  const openedFor = [];
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
    if (!open) {
      insAlert.run(discord_id, chain_id, contract_address, alertType, severity, message, extra ?? null);
      openedFor.push(discord_id);
    }
  }
  return openedFor;
}
function resolveAdminContractAlert({ alertType, chain_id, contract_address }) {
  const admins = selAdmins.all();
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, chain_id, contract_address, alertType);
    if (open) resolveAlertById.run(open.id);
  }
}

/* ----------------- aggregation & outliers ----------------- */
function manageOutlierAlerts(chain_id, contract_address, outliers, suppressLog = false) {
  const sourcesNowOutlier = new Set(outliers.map(o => o.source));
  const srcRows = selSourcesForContract.all(chain_id, contract_address);
  const allSources = srcRows.map(r => r.datasource_name);

  for (const source of allSources) {
    const alertType = `OUTLIER:${source.toLowerCase()}`;
    if (sourcesNowOutlier.has(source)) {
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

  if (!suppressLog && outliers.length === 0) {
    console.log(`✅ No outliers for ${contract_address}@${chain_id}`);
  }
}

function aggregateContractFromFresh({ runId, chain_id, contract_address, cutoffEpoch }) {
  const rows = selFreshSnapshotsForContract.all(chain_id, contract_address, cutoffEpoch);
  if (!rows.length) return null;

  // newest per source
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

  const used = [];
  const outliers = [];
  for (const e of entries) {
    const dev = pctDiff(e.price, med);
    if (dev > OUTLIER_PCT) outliers.push({ ...e, deviation: dev });
    else used.push(e);
  }

  if (used.length < QUORUM_MIN_USED) {
    console.warn(`⚠️  Insufficient quorum after outlier filter for ${contract_address} on chain ${chain_id}: used=${used.length}, needed=${QUORUM_MIN_USED}`);
    manageOutlierAlerts(chain_id, contract_address, outliers, /*suppressLog*/ true);
    return null;
  }

  const usedPrices = used.map(u => u.price);
  const m = mean(usedPrices);
  const window_start = toISO(Math.min(...used.map(u => Date.parse(u.timestamp))));
  const window_end   = toISO(Math.max(...used.map(u => Date.parse(u.timestamp))));
  const discarded_sources = JSON.stringify(outliers.map(o => o.source));

  insAggregate.run(
    runId ?? null,
    chain_id,
    contract_address,
    window_start,
    window_end,
    med,
    m,
    entries.length,
    used.length,
    discarded_sources
  );

  manageOutlierAlerts(chain_id, contract_address, outliers);

  if (outliers.length) {
    const details = outliers.map(o => `${o.source} (${(o.deviation * 100).toFixed(2)}%)`).join(', ');
    console.warn(`⚠️  Outliers dropped for ${contract_address}@${chain_id}: ${details}`);
  }

  return { chain_id, contract_address, median: med, mean: m, source_count: entries.length, used_sources: used.length, window_start, window_end };
}

/* ----------------- stall detection (with per-admin fan-out) ----------------- */
function detectStallsForContract(chain_id, contract_address) {
  const pairLabel = getPairLabel(chain_id, contract_address);

  const stalledOpenedNow = [];
  const resolvedNow = [];

  const sources = selSourcesForContract.all(chain_id, contract_address).map(r => r.datasource_name);
  for (const source of sources) {
    const last3 = selLast3ForSourceContract.all(chain_id, contract_address, source);
    if (last3.length < 3) continue;

    // span check
    const times = last3.map(r => Date.parse(r.timestamp));
    const spanSec = (Math.max(...times) - Math.min(...times)) / 1000;
    if (spanSec < STALL_MIN_SPAN_SEC) continue;

    // flatness check (source own movement)
    const prices = last3.map(r => Number(r.price)).filter(Number.isFinite);
    if (prices.length < 3) continue;

    const pMin = Math.min(...prices);
    const pMax = Math.max(...prices);
    const pMidBase = (pMin + pMax) / 2;
    const flatRangePct = pMidBase > 0 ? (pMax - pMin) / pMidBase : 0;

    // market move check (earliest vs latest run median of ALL sources)
    const runIds = [ last3[last3.length - 1].run_id, last3[0].run_id ]; // [earliest, latest]
    const marketRows = selPricesForRuns.all(chain_id, contract_address, runIds[0], runIds[1]);
    const byRun = new Map();
    for (const r of marketRows) {
      if (!byRun.has(r.run_id)) byRun.set(r.run_id, []);
      byRun.get(r.run_id).push(Number(r.price));
    }
    if (!byRun.has(runIds[0]) || !byRun.has(runIds[1])) continue;

    const medEarly = median(byRun.get(runIds[0]).filter(Number.isFinite));
    const medLate  = median(byRun.get(runIds[1]).filter(Number.isFinite));
    if (!Number.isFinite(medEarly) || !Number.isFinite(medLate) || medEarly <= 0) continue;

    const marketMovePct = Math.abs(medLate - medEarly) / medEarly;
    const isStalled = flatRangePct <= STALL_FLAT_PCT && marketMovePct >= STALL_MARKET_MOVE_PCT;

    const alertType = `DS_STALL:${source.toLowerCase()}`;
    if (isStalled) {
      // compute DM fields
      const stalled_price = mean(prices);
      const median_now    = medLate;
      const dev_pct       = pctDiff(stalled_price, median_now);

      const message = `Source ${source} appears stalled for ${pairLabel ?? contract_address} over ~${fmtSpan(spanSec)}; market moved ${fmtPct(marketMovePct)}.`;
      const openedFor = openAdminContractAlert({
        alertType,
        chain_id,
        contract_address,
        severity: 'warning',
        message,
        extra: JSON.stringify({
          source,
          pair: pairLabel,
          stalled_price,
          median_now,
          dev_pct,
          flat_range_pct: STALL_FLAT_PCT,
          market_move_pct: STALL_MARKET_MOVE_PCT,
          span_sec: spanSec
        })
      });

      stalledOpenedNow.push({
        source, chain_id, contract_address, pair: pairLabel,
        stalled_price, median_now, dev_pct, span_sec: spanSec, market_move_pct: marketMovePct,
        openedFor,
      });
    } else {
      resolveAdminContractAlert({ alertType, chain_id, contract_address });
      resolvedNow.push({ source, chain_id, contract_address, pair: pairLabel });
    }
  }

  return { stalledOpenedNow, resolvedNow };
}

/* Build one DM payload per admin containing only items that opened for THEM this run. */
function buildStallFanoutPayload(stalledOpenedNow) {
  const admins = selAdmins.all(); // { discord_id, discord_name, accepts_dm }
  const byAdmin = new Map();
  for (const admin of admins) {
    byAdmin.set(admin.discord_id, { admin, items: [] });
  }
  for (const it of stalledOpenedNow) {
    for (const id of it.openedFor || []) {
      if (byAdmin.has(id)) byAdmin.get(id).items.push(it);
    }
  }
  return [...byAdmin.values()].filter(entry => entry.items.length > 0);
}

function renderStallDM(items) {
  // items for one admin
  const bySource = new Map();
  for (const it of items) {
    if (!bySource.has(it.source)) bySource.set(it.source, []);
    bySource.get(it.source).push(it);
  }

  const lines = [];
  lines.push(`🚨 **Datasource stalls detected (new this run)**`);
  lines.push(`These sources held (nearly) flat while the market moved ≥ ${fmtPct(STALL_MARKET_MOVE_PCT)}.`);
  lines.push(`(Flat range ≤ ${fmtPct(STALL_FLAT_PCT)}, span ≥ ${fmtSpan(STALL_MIN_SPAN_SEC)})`);

  for (const [source, arr] of bySource.entries()) {
    lines.push(`\n**${source}** — ${arr.length} new stall(s):`);
    lines.push('```');
    lines.push(`chain  pair        contract_address                              stalled      median_now   dev%   span   market`);
    for (const z of arr) {
      const chain = String(z.chain_id).padEnd(5, ' ');
      const pair  = String(z.pair || '').padEnd(11, ' ');
      const addr  = String(z.contract_address).padEnd(44, ' ');
      const sp    = String(fmtPrice(z.stalled_price)).padEnd(12, ' ');
      const mn    = String(fmtPrice(z.median_now)).padEnd(12, ' ');
      const dv    = String((z.dev_pct * 100).toFixed(2) + '%').padEnd(6, ' ');
      const span  = String(fmtSpan(z.span_sec)).padEnd(6, ' ');
      const mkt   = String((z.market_move_pct * 100).toFixed(2) + '%').padEnd(6, ' ');
      lines.push(`${chain} ${pair} ${addr} ${sp} ${mn} ${dv} ${span} ${mkt}`);
    }
    lines.push('```');
  }

  return lines.join('\n');
}

async function dmAdminsStallSummary(client, fanout) {
  if (!client) return;

  for (const { admin, items } of fanout) {
    const { discord_id, discord_name, accepts_dm } = admin;
    if (accepts_dm !== 1) {
      console.warn(`❌ Could not DM admin ${discord_name || discord_id}: DMs disabled`);
      continue;
    }

    const message = renderStallDM(items);

    try {
      const userObj = await client.users.fetch(discord_id);
      await userObj.send(message);
      console.log(`📣 Sent stall summary to admin ${discord_name || discord_id}`);
    } catch (e) {
      console.warn(`❌ Could not DM admin ${discord_name || discord_id}: ${e.message}`);
      try { setAcceptsDM.run(discord_id); console.log(`🔧 accepts_dm -> 0 for ${discord_id}`); }
      catch (dbErr) { console.error('❌ Failed to update accepts_dm:', dbErr.message); }
    }
  }
}

/* ----------------- public orchestrator ----------------- */
async function aggregateAndDetect(client, runId = null) {
  const cutoffEpoch = Math.floor(Date.now() / 1000) - Number(FRESHNESS_SEC);

  // 1) Aggregate & outliers
  const targets = selActiveContractsWithFreshData.all(cutoffEpoch);
  if (!targets.length) {
    console.log('ℹ️  No fresh snapshots found for active contracts within freshness window.');
  }

  let aggCount = 0;
  for (const t of targets) {
    const res = aggregateContractFromFresh({ runId, chain_id: t.chain_id, contract_address: t.contract_address, cutoffEpoch });
    if (res) aggCount++;
  }
  console.log(`📊 Aggregation complete: wrote ${aggCount} price_aggregate row(s).`);

  // 2) Stall detection (DM admins for NEW stalls only)
  const openedThisRun = [];
  for (const t of targets) {
    const { stalledOpenedNow /*, resolvedNow*/ } = detectStallsForContract(t.chain_id, t.contract_address);
    openedThisRun.push(...stalledOpenedNow);
  }

  if (openedThisRun.length) {
    const fanout = buildStallFanoutPayload(openedThisRun);
    if (fanout.length) {
      await dmAdminsStallSummary(client, fanout);
    } else {
      console.log('ℹ️  Stalls found but no *new* admin alerts opened (likely already open).');
    }
  } else {
    console.log('✅ No new datasource stalls detected.');
  }
}

module.exports = { aggregateAndDetect };
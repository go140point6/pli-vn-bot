// utils/getPrices.js
// Returns a blended PLI/USDT price (Number) using:
// - Latest datasource aggregate median from price_aggregates (outliers removed upstream)
// - Latest oracle median across validators from oracle_price_snapshots (contract-level aggregation reflected by median of last run)
// If both are present, returns their average; if only one is present, returns that one.
// Rounds to 4 decimals by default (configurable).

const { getDb } = require('../db');
const db = getDb();

// ---- Debug flag (shared convention: DEBUG_ALL=1|true|yes|on) ----
const DEBUG_ALL_RAW = String(process.env.DEBUG_ALL || '').trim();
const DEBUG_ALL =
  DEBUG_ALL_RAW === '1' ||
  /^true$/i.test(DEBUG_ALL_RAW) ||
  /^yes$/i.test(DEBUG_ALL_RAW) ||
  /^on$/i.test(DEBUG_ALL_RAW);

/* ---------------- Prepared statements ---------------- */

// Pick contract by (active PLI/USDT) preferring a specific chain if provided
const selActivePliOnChain = db.prepare(`
  SELECT chain_id, address
  FROM contracts
  WHERE active = 1 AND base = 'PLI' AND quote = 'USDT' AND chain_id = ?
  ORDER BY address ASC
  LIMIT 1
`);

const selAnyActivePli = db.prepare(`
  SELECT chain_id, address
  FROM contracts
  WHERE active = 1 AND base = 'PLI' AND quote = 'USDT'
  -- small preference for XDC mainnet if present
  ORDER BY (chain_id = 50) DESC, chain_id ASC, address ASC
  LIMIT 1
`);

const selLatestDsRunId = db.prepare(`
  SELECT run_id
  FROM datasource_price_snapshots
  WHERE chain_id = ? AND contract_address = ?
  ORDER BY run_id DESC
  LIMIT 1
`);

const selDsPricesForRun = db.prepare(`
  SELECT price
  FROM datasource_price_snapshots
  WHERE chain_id = ? AND contract_address = ? AND run_id = ?
`);

// For logging each datasource’s raw price (name + price)
const selDsPricesForRunWithSource = db.prepare(`
  SELECT datasource_name, price
  FROM datasource_price_snapshots
  WHERE chain_id = ? AND contract_address = ? AND run_id = ?
  ORDER BY datasource_name ASC
`);

// Latest datasource aggregate for that contract
const selLatestAgg = db.prepare(`
  SELECT median
  FROM price_aggregates
  WHERE chain_id = ? AND contract_address = ?
  ORDER BY window_end DESC
  LIMIT 1
`);

// Latest oracle run_id for that contract
const selLatestOracleRunId = db.prepare(`
  SELECT run_id
  FROM oracle_price_snapshots
  WHERE chain_id = ? AND contract_address = ?
  ORDER BY run_id DESC
  LIMIT 1
`);

// All oracle submissions for that contract at a given run_id
const selOraclePricesForRun = db.prepare(`
  SELECT price
  FROM oracle_price_snapshots
  WHERE chain_id = ? AND contract_address = ? AND run_id = ?
`);

/* ---------------- Helpers ---------------- */

function pickContract() {
  const envChain = process.env.PLI_PRICE_CHAIN_ID
    ? parseInt(process.env.PLI_PRICE_CHAIN_ID, 10)
    : undefined;

  if (Number.isInteger(envChain)) {
    const picked = selActivePliOnChain.get(envChain);
    if (picked) return picked;
  }
  return selAnyActivePli.get();
}

function medianOf(nums) {
  if (!nums || nums.length === 0) return null;
  const arr = nums.slice().sort((a, b) => a - b);
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 === 0 ? (arr[mid - 1] + arr[mid]) / 2 : arr[mid];
}

function roundToDecimals(n, decimals) {
  if (!Number.isFinite(n)) return n;
  const d = Number.isInteger(decimals) ? decimals : parseInt(decimals, 10);
  const places = Number.isFinite(d) ? d : 4; // default 4 dp
  return Number(n.toFixed(places));
}

/* ---------------- Public API ---------------- */
/**
 * getPrices(decimals?: number|string) -> Number
 * - decimals is optional; if provided, returns a Number rounded to that precision (default 4)
 */
async function getPrices(decimals) {
  // 1) pick the PLI/USDT contract
  const picked = pickContract();
  if (!picked) {
    throw new Error('No active PLI/USDT contract found in "contracts" table.');
  }
  const { chain_id, address: contract_address } = picked;

  // 2) datasource side: latest aggregate median (or fallback to snapshots)
  let dsMedian = null;
  const agg = selLatestAgg.get(chain_id, contract_address);
  if (agg && Number.isFinite(agg.median)) {
    dsMedian = Number(agg.median);
  } else {
    // Fallback: compute median from latest datasource run’s snapshots
    const dsRun = selLatestDsRunId.get(chain_id, contract_address);
    if (dsRun && Number.isFinite(dsRun.run_id)) {
      const dsRows = selDsPricesForRun.all(chain_id, contract_address, Number(dsRun.run_id));
      const dsPrices = dsRows.map(r => Number(r.price)).filter(Number.isFinite);
      const dsM = medianOf(dsPrices);
      if (dsM !== null) dsMedian = dsM;
    }
  }

  // 3) oracle side: median across validators at the latest run
  let orMedian = null;
  const latestRunRow = selLatestOracleRunId.get(chain_id, contract_address);
  if (latestRunRow && Number.isFinite(latestRunRow.run_id)) {
    const runId = Number(latestRunRow.run_id);
    const rows = selOraclePricesForRun.all(chain_id, contract_address, runId);
    const prices = rows.map(r => Number(r.price)).filter(Number.isFinite);
    const m = medianOf(prices);
    if (m !== null) orMedian = m;
  }

  // 4) blend or fallback
  let result = null;
  if (Number.isFinite(dsMedian) && Number.isFinite(orMedian)) {
    result = (dsMedian + orMedian) / 2;
  } else if (Number.isFinite(dsMedian)) {
    result = dsMedian;
  } else if (Number.isFinite(orMedian)) {
    result = orMedian;
  } else {
    throw new Error(`No price data available for PLI/USDT (chain_id=${chain_id}).`);
  }

  // -------- Per-datasource raw price logging (behind DEBUG_ALL) --------
  if (DEBUG_ALL) {
    try {
      const dsRun = selLatestDsRunId.get(chain_id, contract_address);
      if (dsRun && Number.isFinite(dsRun.run_id)) {
        const rows = selDsPricesForRunWithSource.all(chain_id, contract_address, Number(dsRun.run_id));
        if (rows.length) {
          rows.forEach(r => {
            const raw = Number(r.price);
            const val = Number.isFinite(raw) ? String(raw) : 'n/a';
            console.log(`[setPresence] datasource ${r.datasource_name}: ${val}`);
          });
        } else {
          console.log('[setPresence] datasource prices: (none found for latest run)');
        }
      } else {
        console.log('[setPresence] datasource prices: (no datasource runs found)');
      }
    } catch (e) {
      console.warn(`[setPresence] failed to log per-datasource prices: ${e.message}`);
    }
  }

  // -------- Summary logs (always on) --------
  const dpRaw = Number.isInteger(decimals) ? decimals : parseInt(decimals, 10);
  const dp = Number.isFinite(dpRaw) ? dpRaw : 4;
  const fmt = (n) => (Number.isFinite(n) ? n.toFixed(dp) : 'n/a');

  const rounded = roundToDecimals(result, decimals);

  console.log(`[setPresence] datasource median: ${fmt(dsMedian)}`);
  console.log(`[setPresence] oracle median:     ${fmt(orMedian)}`);
  console.log(`[setPresence] blended average:   ${fmt(rounded)}`);

  return rounded;
}

module.exports = { getPrices };

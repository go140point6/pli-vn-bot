// utils/getPrices.js
// Returns the latest aggregated median price for PLI/USDT as a Number.
// - Primary source: price_aggregates.median (newest window_end)
// - Contract selection: ENV chain override -> active PLI/USDT in contracts
// - Optional rounding to "decimals" without returning a string

const { getDb } = require('../db'); // <- your db/index.js singleton
const db = getDb();

// ----- Prepared statements -----

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

// Latest aggregate for that contract
const selLatestAgg = db.prepare(`
  SELECT median, mean, used_sources, source_count, window_start, window_end
  FROM price_aggregates
  WHERE chain_id = ? AND contract_address = ?
  ORDER BY window_end DESC
  LIMIT 1
`);

// Optional last-resort fallback if aggregates aren’t present yet (kept very simple)
const selRecentSnapshots = db.prepare(`
  SELECT price
  FROM datasource_price_snapshots
  WHERE chain_id = ? AND contract_address = ?
  ORDER BY timestamp DESC
  LIMIT 8
`);

// ----- Helpers -----
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
  const d = Number.isInteger(decimals) ? decimals : parseInt(decimals, 10) || 4;
  // Keep return type = Number (avoid string from toFixed)
  return Number(n.toFixed(d));
}

// ----- Public API -----
/**
 * getPrices(decimals?: number|string) -> Number
 * - decimals is optional; if provided, returns a Number rounded to that precision
 */
async function getPrices(decimals) {
  // 1) pick the PLI/USDT contract
  const picked = pickContract();
  if (!picked) {
    throw new Error('No active PLI/USDT contract found in "contracts" table.');
  }
  const { chain_id, address } = picked;

  // 2) try latest aggregate
  const agg = selLatestAgg.get(chain_id, address);
  if (agg && typeof agg.median === 'number') {
    return roundToDecimals(agg.median, decimals);
  }

  // 3) fallback: quick median of a few most recent datasource snapshots
  const snaps = selRecentSnapshots.all(chain_id, address);
  const prices = snaps.map(r => Number(r.price)).filter(Number.isFinite);
  const m = medianOf(prices);
  if (m !== null) return roundToDecimals(m, decimals);

  throw new Error(`No price data available for PLI/USDT (chain_id=${chain_id}).`);
}

module.exports = { getPrices };
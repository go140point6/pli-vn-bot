// datasources/common.js
require('dotenv').config();
const { selApiMeta } = require('../db/statements');

// throttle value (shared)
const THROTTLE_MS = parseInt(process.env.DATASOURCE_THROTTLE_MS || '500', 10);

// Supports "data.tickers[0].last" and "data.tickers.0.last"
function getNestedValue(obj, path) {
  if (!path || typeof path !== 'string') return undefined;
  const norm = path.replace(/\[(\d+)\]/g, '.$1'); // [0] -> .0
  return norm.split('.').reduce((acc, key) => (acc == null ? acc : acc[key]), obj);
}

function toNumOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function mustGetApiMeta(datasource) {
  const ds = String(datasource || '').toLowerCase().trim();
  const meta = selApiMeta.get(ds);
  if (!meta) throw new Error(`No API metadata for ${ds}`);
  const { base_url, response_path } = meta;
  if (!base_url || !String(base_url).trim()) throw new Error(`${ds}: base_url missing/blank`);
  if (!response_path || !String(response_path).trim()) throw new Error(`${ds}: response_path missing/blank`);
  return meta;
}

// Only CMC: inject CMC_API_KEY; others: allow blank or static JSON (no env substitution)
function buildHeaders(datasource, rawHeaders) {
  if (!rawHeaders) return {};

  // If already an object (future-proof), return as-is
  if (typeof rawHeaders === 'object') return rawHeaders;

  const s = String(rawHeaders).trim();
  if (!s || s.toLowerCase() === 'null') return {};

  if (String(datasource).toLowerCase() !== 'coinmarketcap') {
    try {
      return JSON.parse(s);
    } catch {
      console.error(`❌ ${datasource} header JSON parse error`);
      return {};
    }
  }

  // CoinMarketCap: substitute ${api_key} with env
  try {
    const json = s.replace(/\$\{api_key\}/g, process.env.CMC_API_KEY || '');
    return JSON.parse(json);
  } catch (e) {
    console.error(`❌ coinmarketcap header JSON parse error: ${e.message}`);
    return {};
  }
}

function fmtPair(row) {
  if (row.datasource_pair_id) return String(row.datasource_pair_id);
  if (row.base && row.quote) return `${String(row.base).toUpperCase()}_${String(row.quote).toUpperCase()}`;
  return '(unknown_pair)';
}

module.exports = {
  THROTTLE_MS,
  getNestedValue,
  toNumOrNull,
  mustGetApiMeta,
  buildHeaders,
  fmtPair,
};

// src/utils/chain.js
function parseChainId(v, fallback = null) {
  if (v === undefined || v === null || String(v).trim() === '') return fallback;
  const n = Number(String(v).trim());
  return Number.isInteger(n) ? n : fallback;
}

// Optional: lookup map you can grow over time
const KNOWN_CHAINS = {
  1: 'Ethereum',
  50: 'XDC',
  137: 'Polygon',
  // add more as needed
};

module.exports = { parseChainId, KNOWN_CHAINS };
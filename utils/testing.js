// utils/testing.js
function maybeForceStallPrice({ datasource, chain_id, contract_address, price }) {
  const src  = process.env.TEST_STALL_SOURCE?.trim().toLowerCase();
  const addr = process.env.TEST_STALL_CONTRACT?.trim().toLowerCase();
  const fixed = process.env.TEST_STALL_PRICE;

  if (!src || !addr || fixed == null) return price;              // feature off
  if (datasource.toLowerCase() !== src) return price;             // different source
  if ((contract_address || '').toLowerCase() !== addr) return price; // different pair

  const n = Number(fixed);
  return Number.isFinite(n) ? n : price;                          // invalid -> ignore
}

module.exports = { maybeForceStallPrice };
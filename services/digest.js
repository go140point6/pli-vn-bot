// services/digest.js
// A tiny helper to collect *newly opened* datasource errors per run,
// grouped by admin, then build one summary DM per admin.

function newRunDigest() {
  // Map<adminId, { adminName: string|null, items: Map<string, Item> }>
  // Item = { alertType, pair, contract, chainId, count }
  return new Map();
}

function _keyOf({ alertType, pair, contract, chainId }) {
  return `${alertType}|${chainId}|${contract}|${pair}`;
}

/**
 * Add an item to the per-run digest for a particular admin.
 * @param {Map} digest - from newRunDigest()
 * @param {{adminId:string, adminName?:string|null, alertType:string, pair:string, contract:string, chainId:number}} item
 */
function addToDigest(digest, { adminId, adminName = null, alertType, pair, contract, chainId }) {
  if (!(digest instanceof Map)) return;

  let bucket = digest.get(adminId);
  if (!bucket) {
    bucket = { adminName: adminName ?? null, items: new Map() };
    digest.set(adminId, bucket);
  } else if (adminName && !bucket.adminName) {
    // fill name the first time we see a non-empty value
    bucket.adminName = adminName;
  }

  const key = _keyOf({ alertType, pair, contract, chainId });
  const prev = bucket.items.get(key);
  if (prev) {
    prev.count += 1;
  } else {
    bucket.items.set(key, { alertType, pair, contract, chainId, count: 1 });
  }
}

/**
 * Build a human-friendly message for a single admin’s digest.
 * Accepts the admin's items Map from digest.get(adminId).items
 * @param {Map<string, {alertType:string, pair:string, contract:string, chainId:number, count:number}>} items
 */
function buildAdminDigestMessage(items) {
  if (!items || typeof items.size !== 'number' || items.size === 0) {
    return 'No new datasource errors this run.';
  }

  const lines = [];
  lines.push('⚠️ **New datasource errors this run**');
  lines.push('');

  for (const [, it] of items) {
    const short = it.contract ? `${it.contract.slice(0, 6)}…${it.contract.slice(-4)}` : '(no contract)';
    const countSuffix = it.count > 1 ? ` ×${it.count}` : '';
    lines.push(`• \`${it.pair}\` on chain ${it.chainId} @ ${short} — ${it.alertType}${countSuffix}`);
  }

  lines.push('');
  lines.push('You’ll stop receiving DMs for these exact alerts once they resolve (or if DMs are disabled).');
  lines.push(`\u200B`);
  return lines.join('\n');
}

module.exports = {
  newRunDigest,
  addToDigest,
  buildAdminDigestMessage,
};
// services/digest.js
function newRunDigest() { return new Map(); }

function addToDigest(digest, { adminId, adminName, alertType, pair, contract, chainId }) {
  if (!digest.has(adminId)) {
    digest.set(adminId, { adminName: adminName || null, items: new Map() });
  }
  const entry = digest.get(adminId);
  if (!entry.items.has(alertType)) {
    entry.items.set(alertType, { count: 0, pairs: new Set(), contracts: new Set(), chainIds: new Set() });
  }
  const it = entry.items.get(alertType);
  it.count += 1;
  it.pairs.add(pair);
  it.contracts.add(contract);
  it.chainIds.add(chainId);
}

function buildAdminDigestMessage(items) {
  const lines = [`⚠️ **Datasource ingestion errors** (new this run)`];
  for (const [alertType, info] of items) {
    const source = alertType.split(':')[1] || alertType;
    const examples = Array.from(info.pairs).slice(0, 5);
    const more = info.pairs.size > examples.length ? ` (+${info.pairs.size - examples.length} more)` : '';
    lines.push(`• **${source}** — ${info.count} failure(s) across ${info.pairs.size} pair(s): ${examples.join(', ')}${more}`);
  }
  lines.push('', 'You will not receive another DM for these specific pair/source errors until they recover.');
  return lines.join('\n');
}

module.exports = { newRunDigest, addToDigest, buildAdminDigestMessage };
// rpcCheck.db.js (modular, silent; uses src/eth/httpProvider + shared helpers)

const { getDb } = require('../db');
const { getHttpProvider } = require('../eth/httpProvider');
const { rpcWithTimeout } = require('../eth/shared');

// --- Tunables ---
const TIMEOUT_MS = 4000;

// --- DB loader ---
function loadRpcUrlsFromDb(filters = []) {
  const db = getDb();
  try {
    const rows = db.prepare(`SELECT mn FROM mn_rpc ORDER BY rowid`).all();
    let urls = rows.map(r => String(r.mn).trim()).filter(Boolean);
    if (filters.length) {
      const parts = filters.map(s => s.toLowerCase());
      urls = urls.filter(u => parts.some(p => u.toLowerCase().includes(p)));
    }
    return urls;
  } finally {
    db.close();
  }
}

// --- Provider ---
async function getProvider(url) {
  const rpcUrl = url || process.env.RPCURL_50;
  if (!rpcUrl) throw new Error('HTTP RPC URL required (set RPCURL_50 or pass url)');
  return getHttpProvider(rpcUrl);
}

// --- Tests ---
async function testRPCReachability(url) {
  try {
    const provider = await getProvider(url);
    let netVersion;
    try {
      netVersion = await rpcWithTimeout(provider, 'net_version', [], TIMEOUT_MS); // "50"
    } catch {
      const hexChainId = await rpcWithTimeout(provider, 'eth_chainId', [], TIMEOUT_MS); // "0x32"
      netVersion = parseInt(hexChainId, 16).toString();
    }
    return { networkId: Number(netVersion) };
  } catch (error) {
    return { error: error.message || 'Unknown error' };
  }
}

async function testRPCBlockchainSync(url) {
  try {
    const provider = await getProvider(url);
    const res = await rpcWithTimeout(provider, 'eth_syncing', [], TIMEOUT_MS);
    if (res && typeof res === 'object') {
      const toNum = (v) => (typeof v === 'string' && v.startsWith('0x') ? parseInt(v, 16) : Number(v));
      return {
        syncing: {
          startingBlock: toNum(res.startingBlock),
          currentBlock: toNum(res.currentBlock),
          highestBlock: toNum(res.highestBlock),
        },
      };
    }
    return { syncing: false };
  } catch (error) {
    return { error: error.message || 'Unknown error' };
  }
}

async function testLatestBlockNumber(url) {
  try {
    const provider = await getProvider(url);
    try {
      const hex = await rpcWithTimeout(provider, 'eth_blockNumber', [], TIMEOUT_MS);
      return parseInt(hex, 16);
    } catch {
      return await provider.getBlockNumber();
    }
  } catch {
    return undefined;
  }
}

// --- Probers ---
async function probe(url) {
  const reach = await testRPCReachability(url);
  const block = await testLatestBlockNumber(url);
  const sync = await testRPCBlockchainSync(url);
  return { reach, block, sync };
}

async function probeAll(urls) {
  const results = [];
  for (const url of urls) {
    results.push({ url, ...(await probe(url)) });
  }
  return results;
}

module.exports = {
  loadRpcUrlsFromDb,
  getProvider,
  testRPCReachability,
  testRPCBlockchainSync,
  testLatestBlockNumber,
  probe,
  probeAll,
};

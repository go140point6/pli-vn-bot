// checkWss.js
const { withWsProvider, wssReachable } = require('../eth/wssProvider');
const { rpcWithTimeout } = require('../eth/shared');

async function testWebSocketReachability(wssURL) {
  try { await wssReachable(wssURL); return; }
  catch (e) { throw e; }
}

async function testWSSBlockchainSync(wssURL) {
  try {
    return await withWsProvider(wssURL, async (provider) => {
      const res = await rpcWithTimeout(provider, 'eth_syncing', [], 5000);
      if (res && typeof res === 'object') {
        const toNum = (v) => (typeof v === 'string' && v.startsWith('0x') ? parseInt(v, 16) : Number(v));
        return { syncing: { startingBlock: toNum(res.startingBlock), currentBlock: toNum(res.currentBlock), highestBlock: toNum(res.highestBlock) } };
      }
      return { syncing: false };
    });
  } catch (error) {
    return { error: error.message || 'Unknown error' };
  }
}

module.exports = { testWebSocketReachability, testWSSBlockchainSync };
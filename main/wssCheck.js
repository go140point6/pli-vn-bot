const WebSocket = require('ws');
const Xdc3 = require('xdc3')

// Test 1: Check if the WebSocket endpoint is reachable
async function testWebSocketReachability(wssURL) {
  const ws = new WebSocket(wssURL);

  return new Promise((resolve, reject) => {
      ws.on('open', () => {
          ws.close();
          resolve();
      });

      ws.on('error', error => {
          reject(error);
      });
  });
}

// Test 2: Check if the blockchain node is synchronized via WebSocket
async function testWSSBlockchainSync(wssURL) {
  const xdc3 = new Xdc3(new Xdc3.providers.WebsocketProvider(wssURL));
  try {
      const syncing = await xdc3.eth.isSyncing();
      return { syncing };
  } catch (error) {
      return { error: error.message || 'Unknown error' };
  }
}

module.exports = {
  testWebSocketReachability,
  testWSSBlockchainSync
}
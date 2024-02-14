const Xdc3 = require('xdc3');

// Test 1: Check if the RPC endpoint is reachable
async function testRPCReachability(rpcURL) {
  const xdc3 = new Xdc3(new Xdc3.providers.HttpProvider(rpcURL));
  try {
    //console.log(rpcURL)
    const networkId = await xdc3.eth.net.getId();
    //console.log(`RPC ${rpcURL} is reachable. Network ID: ${networkId}`)
    return { networkId }
  } catch (error) {
    //console.error(`Error reaching RPC ${rpcURL} endpoint: ${error}`)
    return { error: error.message || 'Unknown error' }
  }
}

// Test 2: Check if the blockchain node is synchronized
async function testRPCBlockchainSync(rpcURL) {
  const xdc3 = new Xdc3(new Xdc3.providers.HttpProvider(rpcURL))
  try {
    const syncing = await xdc3.eth.isSyncing();
    if (syncing === false) {
      //console.log('Blockchain node is fully synchronized.');
    } else {
      //console.log(`Node not synced! currentBlock: ${syncing.currentBlock} highestBlock: ${syncing.highestBlock}`);
    }
    return { syncing }
  } catch (error) {
    //console.error('Error reaching RPC endpoint:', error);
    return { error: error.message || 'Unknown error'}
  }
}

// Test 3: Check block number
async function testLatestBlockNumber() {
  const xdc3 = new Xdc3(new Xdc3.providers.HttpProvider(rpcURL))
  try {
    const blockNumber = await xdc3.eth.getBlockNumber();
    console.log('Latest block number:', blockNumber);
    return blockNumber
  } catch (error) {
    console.error('Error reaching RPC endpoint:', error);
  }
}

module.exports = {
  testRPCReachability,
  testRPCBlockchainSync,
  testLatestBlockNumber
};
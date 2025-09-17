// rpcCheck.js (ethers v6, CommonJS, in-file endpoint list, verbose logging)

let _ethersP; // lazy + cached dynamic import for ESM-only ethers v6
function getEthers() {
  return (_ethersP ??= import('ethers'));
}

// ====== Your endpoints (comment out to test one-by-one) ======
const RPCS = [
  //'https://rpc-mn02.go140point6.com', //working
  'https://erpc.xinfin.network/',
  //'https://earpc.xinfin.network/',
  //'https://rpc.primenumbers.xyz/', //working
  //'https://erpc.xdcrpc.com/',
  //'https://rpc.xdcrpc.com/',
  //'https://rpc.xdc.org',
  //'https://rpc.ankr.com/xdc',
  //'https://rpc.xdc.network',
  //'https://rpc1.xinfin.network',
];

// ====== Logging helpers ======
const ISO = () => new Date().toISOString();
function log(url, msg) {
  console.log(`[${ISO()}] [${url}] ${msg}`);
}

// ====== Provider factory (STATIC network to avoid auto-detect retries) ======
const _providers = new Map();

async function getProvider(url) {
  if (!url) throw new Error('URL required');

  if (_providers.has(url)) {
    log(url, 'using cached JsonRpcProvider');
    return _providers.get(url);
  }

  const { JsonRpcProvider, Network } = await getEthers();
  const network = Network.from(50); // XDC mainnet
  log(url, 'creating JsonRpcProvider (staticNetwork=50)');
  const provider = new JsonRpcProvider(url, network, { staticNetwork: network });
  _providers.set(url, provider);
  return provider;
}

async function sendWithTimeout(provider, method, params = [], ms = 5000) {
  log(provider?.connection?.url ?? 'provider', `→ ${method} (timeout ${ms}ms)`);
  return await Promise.race([
    provider.send(method, params),
    new Promise((_, rej) =>
      setTimeout(() => rej(new Error(`Timeout after ${ms}ms for ${method}`)), ms)
    ),
  ]);
}

// ====== Tests ======

/** Reachability: try net_version, fall back to eth_chainId */
async function testRPCReachability(url) {
  try {
    const provider = await getProvider(url);

    let netVersion;
    try {
      netVersion = await sendWithTimeout(provider, 'net_version', [], 4000); // "50"
    } catch (e) {
      log(url, `net_version failed: ${e.message}; trying eth_chainId`);
      const hexChainId = await sendWithTimeout(provider, 'eth_chainId', [], 4000); // "0x32"
      netVersion = parseInt(hexChainId, 16).toString();
    }

    const networkId = Number(netVersion);
    log(url, `✓ reachable; networkId=${networkId}`);
    return { networkId };
  } catch (error) {
    log(url, `✗ reachability error: ${error.message || error}`);
    return { error: error.message || 'Unknown error' };
  }
}

/** Sync status: returns false or object with starting/current/highest blocks */
async function testRPCBlockchainSync(url) {
  try {
    const provider = await getProvider(url);
    const res = await sendWithTimeout(provider, 'eth_syncing', [], 4000); // false or hex object
    if (res && typeof res === 'object') {
      const toNum = (v) =>
        typeof v === 'string' && v.startsWith('0x') ? parseInt(v, 16) : Number(v);
      const syncing = {
        startingBlock: toNum(res.startingBlock),
        currentBlock: toNum(res.currentBlock),
        highestBlock: toNum(res.highestBlock),
      };
      log(url, `syncing: ${JSON.stringify(syncing)}`);
      return { syncing };
    }
    log(url, 'syncing: false');
    return { syncing: false };
  } catch (error) {
    log(url, `✗ syncing error: ${error.message || error}`);
    return { error: error.message || 'Unknown error' };
  }
}

/** Latest block number (tries raw RPC first for speed, then provider helper) */
async function testLatestBlockNumber(url) {
  try {
    const provider = await getProvider(url);
    let blockNumber;
    try {
      const hex = await sendWithTimeout(provider, 'eth_blockNumber', [], 4000); // "0xNNN"
      blockNumber = parseInt(hex, 16);
    } catch (e) {
      log(url, `eth_blockNumber failed: ${e.message}; trying provider.getBlockNumber()`);
      blockNumber = await provider.getBlockNumber(); // number
    }
    log(url, `blockNumber=${blockNumber}`);
    return blockNumber;
  } catch (error) {
    log(url, `✗ block number error: ${error.message || error}`);
    return undefined;
  }
}

// ====== Probers ======

async function probe(url) {
  log(url, '=== PROBE START ===');
  const reach = await testRPCReachability(url);
  const block = await testLatestBlockNumber(url);
  const sync = await testRPCBlockchainSync(url);
  log(url, `=== PROBE END (reach=${JSON.stringify(reach)}, block=${block}, sync=${JSON.stringify(sync)}) ===`);
  return { reach, block, sync };
}

async function probeAll(urls = RPCS) {
  log('system', `Node global fetch present: ${typeof fetch !== 'undefined' ? 'yes' : 'NO (Node < 18?)'}`);
  for (const url of urls) {
    await probe(url);
    console.log(''); // spacer
  }
}

// ====== Exports ======
module.exports = {
  RPCS,
  testRPCReachability,
  testRPCBlockchainSync,
  testLatestBlockNumber,
  probe,
  probeAll,
};

// ====== CLI ======
if (require.main === module) {
  (async () => {
    // By default, probe all URLs. To test a single one, comment the line below
    // and call: await probe(RPCS[0])
    await probeAll(RPCS);
  })().catch((e) => {
    console.error('FATAL:', e);
    process.exit(1);
  });
}

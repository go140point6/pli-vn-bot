// rpcCheck.js (ethers v6, CommonJS, in-file endpoint list, verbose logging, CF-friendly headers)

let _ethersP;
function getEthers() { return (_ethersP ??= import('ethers')); }

// ====== Your endpoints (comment out to test one-by-one) ======
const RPCS = [
  'https://rpc-mn02.go140point6.com',
  'https://rpc.primenumbers.xyz/',
  'https://erpc.xinfin.network/',
  'https://earpc.xinfin.network/',
  'https://erpc.xdcrpc.com/',
  'https://rpc.xdcrpc.com/',
  'https://rpc.xdc.org',
  'https://rpc.ankr.com/xdc',
  'https://rpc.xdc.network',
  'https://rpc1.xinfin.network',
];

// ====== Tunables ======
const REQUEST_TIMEOUT_MS = 5000;
const UA = process.env.RPC_UA
  || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const ORIGIN = process.env.RPC_ORIGIN || 'https://app.xdc.org'; // harmless; helps some CF configs

// ====== Logging ======
const ISO = () => new Date().toISOString();
function log(url, msg) { console.log(`[${ISO()}] [${url}] ${msg}`); }

// ====== Provider factory (STATIC + custom FetchRequest w/ headers) ======
const _providers = new Map();
const _meta = new WeakMap(); // track headers & url for debug

async function getProvider(url) {
  if (!url) throw new Error('URL required');
  if (_providers.has(url)) { log(url, 'using cached JsonRpcProvider'); return _providers.get(url); }

  const { JsonRpcProvider, Network, FetchRequest } = await getEthers();

  // Build CF-friendly request
  const req = new FetchRequest(url);
  req.setHeader('user-agent', UA);
  req.setHeader('accept', 'application/json');
  req.setHeader('content-type', 'application/json');
  req.setHeader('origin', ORIGIN);              // optional but often helpful behind CF
  req.setHeader('accept-language', 'en-US,en'); // optional
  req.setHeader('cache-control', 'no-cache');

  // Make provider static to avoid auto-detect handshake
  const network = Network.from(50); // XDC
  log(url, `creating JsonRpcProvider (staticNetwork=50, UA=${JSON.stringify(UA)})`);
  const provider = new JsonRpcProvider(req, network, { staticNetwork: network });

  _providers.set(url, provider);
  _meta.set(provider, { url, headers: req.headers });
  return provider;
}

async function sendWithTimeout(provider, method, params = [], ms = REQUEST_TIMEOUT_MS) {
  const meta = _meta.get(provider);
  log(meta?.url ?? 'provider', `→ ${method} (timeout ${ms}ms)`);
  try {
    return await Promise.race([
      provider.send(method, params),
      new Promise((_, rej) => setTimeout(() => rej(new Error(`Timeout after ${ms}ms for ${method}`)), ms)),
    ]);
  } catch (e) {
    // expand SERVER_ERROR a bit
    if (e && e.code === 'SERVER_ERROR') {
      log(meta?.url ?? 'provider', `SERVER_ERROR details: ${JSON.stringify(e.info || {}, null, 2)}`);
    }
    throw e;
  }
}

// ====== Tests ======

async function testRPCReachability(url) {
  try {
    const provider = await getProvider(url);
    let netVersion;
    try {
      netVersion = await sendWithTimeout(provider, 'net_version', [], 4000);
    } catch (e) {
      log(url, `net_version failed: ${e.message}; trying eth_chainId`);
      const hexChainId = await sendWithTimeout(provider, 'eth_chainId', [], 4000);
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

async function testRPCBlockchainSync(url) {
  try {
    const provider = await getProvider(url);
    const res = await sendWithTimeout(provider, 'eth_syncing', [], 4000);
    if (res && typeof res === 'object') {
      const toNum = (v) => typeof v === 'string' && v.startsWith('0x') ? parseInt(v, 16) : Number(v);
      const syncing = { startingBlock: toNum(res.startingBlock), currentBlock: toNum(res.currentBlock), highestBlock: toNum(res.highestBlock) };
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

async function testLatestBlockNumber(url) {
  try {
    const provider = await getProvider(url);
    let blockNumber;
    try {
      const hex = await sendWithTimeout(provider, 'eth_blockNumber', [], 4000);
      blockNumber = parseInt(hex, 16);
    } catch (e) {
      log(url, `eth_blockNumber failed: ${e.message}; trying provider.getBlockNumber()`);
      blockNumber = await provider.getBlockNumber();
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
    console.log('');
  }
}

module.exports = {
  RPCS,
  testRPCReachability,
  testRPCBlockchainSync,
  testLatestBlockNumber,
  probe,
  probeAll,
};

if (require.main === module) {
  (async () => { await probeAll(RPCS); })().catch((e) => { console.error('FATAL:', e); process.exit(1); });
}

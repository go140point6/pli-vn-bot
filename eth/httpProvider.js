// src/eth/httpProvider.js (CommonJS)

const { getEthers, CHAIN_ID, buildHttpHeaders } = require('./shared');

const _providers = new Map(); // cache by URL

async function getHttpProvider(url) {
  if (!url) throw new Error('HTTP RPC URL required');

  if (_providers.has(url)) return _providers.get(url);

  const { JsonRpcProvider, Network, FetchRequest } = await getEthers();

  const req = new FetchRequest(url);
  const headers = buildHttpHeaders();
  for (const [k, v] of Object.entries(headers)) req.setHeader(k, v);

  const network = Network.from(CHAIN_ID); // static network (no auto-detect)
  const provider = new JsonRpcProvider(req, network, { staticNetwork: network });

  _providers.set(url, provider);
  return provider;
}

module.exports = { getHttpProvider };
// src/eth/wssProvider.js (CommonJS)

const WS = require('ws');
const { getEthers, CHAIN_ID, ORIGIN } = require('./shared');

const WSS_TIMEOUT_MS = Number(process.env.WSS_TIMEOUT_MS || 5000);
const WSS_UA =
  process.env.WSS_UA ||
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

function wssHeaders() {
  return {
    'user-agent': WSS_UA,
    'accept': '*/*',
    'origin': ORIGIN,
    'cache-control': 'no-cache',
    'accept-language': 'en-US,en',
  };
}

let _installed = false;
function installGlobalWebSocketWithHeaders() {
  if (_installed) return;

  function HeaderWS(url, protocols, options) {
    const opts = options || {};
    const hdrs = Object.assign({}, wssHeaders(), opts.headers || {});
    const merged = Object.assign(
      { headers: hdrs, origin: hdrs.origin, handshakeTimeout: WSS_TIMEOUT_MS },
      opts
    );

    const socket = new WS(url, protocols, merged);

    // prevent process crashes — always have listeners
    const noop = () => {};
    socket.on('error', noop);
    socket.once('unexpected-response', (_req, res) => {
      try { res?.resume?.(); } catch {}
      socket.emit('error', new Error(`Unexpected server response: ${res?.statusCode || 'unknown'}`));
    });

    return socket;
  }

  Object.assign(HeaderWS, WS);
  HeaderWS.prototype = WS.prototype;

  globalThis.WebSocket = HeaderWS;
  _installed = true;
}

async function withWsProvider(url, fn) {
  if (!url) throw new Error('WSS URL required');

  const { WebSocketProvider, Network } = await getEthers();
  installGlobalWebSocketWithHeaders();

  const network = Network.from(CHAIN_ID);
  const provider = new WebSocketProvider(url, network);

  try {
    await provider.getNetwork(); // ensure handshake completes
    return await fn(provider);
  } finally {
    try { provider.destroy?.(); } catch {}
  }
}

async function wssReachable(url) {
  // direct ws open/close with headers (handshake check)
  const ws = new WS(url, undefined, {
    headers: wssHeaders(),
    origin: ORIGIN,
    handshakeTimeout: WSS_TIMEOUT_MS,
  });

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (err) => {
      if (settled) return; settled = true;
      try { ws.close(); } catch {}
      err ? reject(err) : resolve();
    };
    ws.once('open', () => finish());
    ws.once('error', (err) => finish(new Error(err?.message || 'WebSocket error')));
    ws.once('unexpected-response', (_req, res) => finish(new Error(`Unexpected server response: ${res?.statusCode || 'unknown'}`)));
    setTimeout(() => finish(new Error(`Timeout after ${WSS_TIMEOUT_MS}ms opening WebSocket`)), WSS_TIMEOUT_MS + 250);
  });
}

module.exports = {
  withWsProvider,
  wssReachable,
  installGlobalWebSocketWithHeaders,
};
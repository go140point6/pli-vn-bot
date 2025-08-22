// src/eth/shared.js (CommonJS)

let _ethersP;
function getEthers() { return (_ethersP ??= import('ethers')); }

const CHAIN_ID = 50; // XDC mainnet

// CF-friendly headers; override via env if needed
const UA =
  process.env.RPC_UA ||
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const ORIGIN = process.env.RPC_ORIGIN || 'https://app.xdc.org';

function buildHttpHeaders() {
  return {
    'user-agent': UA,
    'accept': 'application/json',
    'content-type': 'application/json',
    'origin': ORIGIN,
    'accept-language': 'en-US,en',
    'cache-control': 'no-cache',
  };
}

async function rpcWithTimeout(provider, method, params = [], ms = 5000) {
  return await Promise.race([
    provider.send(method, params),
    new Promise((_, rej) => setTimeout(() => rej(new Error(`Timeout after ${ms}ms for ${method}`)), ms)),
  ]);
}

module.exports = {
  getEthers,
  CHAIN_ID,
  buildHttpHeaders,
  rpcWithTimeout,
  UA,
  ORIGIN,
};
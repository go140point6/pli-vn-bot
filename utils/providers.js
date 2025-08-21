// utils/provider.js
const { ethers } = require('ethers');

/**
 * Return an ethers provider for a given chainId using env vars like RPC_50, RPC_1, etc.
 */
function getProvider(chainId) {
  const key = `RPCURL_${chainId}`;
  const url = process.env[key];
  if (!url) {
    throw new Error(`Missing ${key} in .env (RPC endpoint for chain ${chainId})`);
  }
  return new ethers.providers.JsonRpcProvider(url);
}

module.exports = { getProvider };
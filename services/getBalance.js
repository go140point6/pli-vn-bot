// getBalance.js
const { getEthers } = require('../eth/shared');
const { getHttpProvider } = require('../eth/httpProvider');
const { to0x } = require('../utils/address');

async function getAddressBalance(address, rpcUrl) {
  const { getAddress, formatEther } = await getEthers();
  const provider = await getHttpProvider(rpcUrl || process.env.RPCURL_50);
  const checksummed = getAddress(to0x(address));
  const wei = await provider.getBalance(checksummed);
  return formatEther(wei);
}

module.exports = { getAddressBalance };
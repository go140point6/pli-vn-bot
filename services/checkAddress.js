const { getEthers } = require('../eth/shared');
const { to0x, withSamePrefix } = require('../utils/address');

async function checkAddress(address) {
  try {
    const { getAddress } = await getEthers();
    const checksummed0x = getAddress(to0x(address));
    return { success: true, result: withSamePrefix(address, checksummed0x) };
  } catch (error) {
    return { success: false, result: String(error.message || error) };
  }
}

module.exports = { checkAddress };
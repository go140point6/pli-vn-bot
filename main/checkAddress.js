const Xdc3 = require('xdc3');
const xdc3 = new Xdc3();

async function checkAddress(address) {
    try {
        const checksummedAddress = xdc3.utils.toChecksumAddress(address)
        return { success: true, result: checksummedAddress }
    } catch (error) {
        //const errorAddress = error.message
        return { success: false, result: error.message }
    }
}

module.exports = {
    checkAddress,
}
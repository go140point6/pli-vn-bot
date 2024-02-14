const Xdc3 = require("xdc3");

const xdc3 = new Xdc3(
    new Xdc3.providers.HttpProvider(process.env.RPCURL)
)

async function getAddressBalance(address) {
    try {
        // Get balance in wei
        const balanceWei = await xdc3.eth.getBalance(address)

        // Convert balance from wei to XDC (1 XDC = 10^18 wei)
        const balanceXDC = xdc3.utils.fromWei(balanceWei, 'ether')
        return balanceXDC
    } catch (error) {
        throw new Error(`Error getting balance for address ${address}: ${error.message}`)
    }
}

module.exports = {
    getAddressBalance,
}
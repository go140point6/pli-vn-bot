const axios = require('axios');

async function getPrices(symbol,fixed) {
    try {
        const res = await axios.get(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${symbol}`)
        if (res.data && res.data[0].current_price && res.data[0].price_change_24h) {
            const currentPrice = res.data[0].current_price.toFixed(fixed) || 0
            const priceChange = res.data[0].price_change_24h 
            console.log("Current price: " + currentPrice);
            console.log("24h price change: " + priceChange)
            //module.exports.currentPrice = currentPrice;
            return { currentPrice, priceChange }
            } else {
                console.log("Error loading coin data")
                return { currentPrice: 0, priceChange: 0 }
            }
        } catch (error) {
            console.log("An error with the Coin Gecko api call: ", error);
            return { currentPrice: 0, priceChange: 0 }
    }
}

module.exports = {
    getPrices,
}
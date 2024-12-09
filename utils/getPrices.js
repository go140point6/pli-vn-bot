const axios = require('axios');

const myTable = [
    { name: "Coingecko", symbol: "plugin", api: 'https://api.coingecko.com/api/v3/simple/price?ids=${symbol}&vs_currencies=usd', path: 'plugin.usd' },
    { name: "Cryptocompare", symbol: "PLI", api: 'https://min-api.cryptocompare.com/data/price?fsym=${symbol}&tsyms=USD', path: 'USD' },
    { name: "Bitrue", symbol: "PLIUSDT", api: 'https://openapi.bitrue.com/api/v1/ticker/price?symbol=${symbol}', path: 'price' },
    { name: "Coinpaprika", symbol: "pli-plugin", api: 'https://api.coinpaprika.com/v1/tickers/${symbol}?quotes=USD', path: 'quotes.USD.price' },
];

// Helper function to extract nested properties dynamically
function getNestedValue(obj, path) {
    return path.split('.').reduce((acc, key) => acc?.[key], obj);
}

// Function to calculate the median of an array
function calculateMedian(values) {
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);

    if (sorted.length % 2 === 0) {
        return (sorted[mid - 1] + sorted[mid]) / 2; // Average of the two middle numbers
    } else {
        return sorted[mid]; // Middle number
    }
}

// Function to fetch and log prices and calculate the average excluding outliers
async function getPrices(fixed) {
    const prices = [];

    for (const row of myTable) {
        try {
            // Replace placeholder in the API URL
            const apiUrl = row.api.replace('${symbol}', row.symbol);

            console.log(`Fetching price from ${row.name}...`);

            // Make the API call
            const res = await axios.get(apiUrl);

            // Extract the price using the provided path
            let price = getNestedValue(res.data, row.path);

            // TEST: Artificially modify the price for Bitrue
            // if (row.name === "Bitrue") {
            //     console.log("TEST MODE: Replacing Bitrue price with an artificial value.");
            //     price = 0.034; // Artificial test value
            // }

            // Log the result
            if (price) {
                const roundedPrice = parseFloat(price);
                console.log(`${row.name} (${row.symbol}) - Price: $${roundedPrice}`);
                prices.push(roundedPrice); // Store the price for calculations
            } else {
                console.log(`Failed to fetch price from ${row.name} (${row.symbol}).`);
            }
        } catch (error) {
            console.log(`Error fetching price from ${row.name} (${row.symbol}):`, error.message);
        }
    }

    // Calculate the median
    if (prices.length > 0) {
        const median = calculateMedian(prices);
        console.log(`Median: $${median.toFixed(4)}`);

        // Filter out outliers (values that differ from the median by more than 30%)
        const threshold = 0.05; // 5%
        const filteredPrices = prices.filter(price => 
            Math.abs(price - median) / median <= threshold
        );

        // Calculate the average of the filtered prices
        const average =
            filteredPrices.reduce((sum, value) => sum + value, 0) / filteredPrices.length;

        console.log(`Filtered Prices (5% of median): [${filteredPrices.join(", ")}]`);
        console.log(`Average Price (excluding outliers): $${average.toFixed(fixed)}`);
        return parseFloat(average.toFixed(fixed));
    } else {
        console.log("No valid prices fetched to calculate the average.");
    }
}

module.exports = {
    getPrices,
};
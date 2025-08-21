// ingest/fetchDatasourcePrices.js
// Per-run orchestrator + one DM digest per admin for *new* errors this run.

const { beginRun, endRun } = require('../services/ingestRun');
const { newRunDigest } = require('../services/digest');
const { sendAdminDigests } = require('../services/alerts');
const { fetchCMCPrices }          = require('../datasources/coinmarketcap');
const { fetchCoinGeckoPrices }    = require('../datasources/coingecko');
const { fetchLBankPrices }        = require('../datasources/lbank');
const { fetchCryptoComparePrices }= require('../datasources/cryptocompare');
const { fetchBitruePrices }       = require('../datasources/bitrue');
const { fetchBitmartPrices }      = require('../datasources/bitmart');
const { aggregateAndDetect } = require('./aggregatePrices');

async function fetchAllDatasourcePrices(client) {
  const runId = beginRun();
  const digest = newRunDigest();
  try {
    await fetchCMCPrices(client, runId, digest);
    await fetchCoinGeckoPrices(client, runId, digest);
    await fetchBitmartPrices(client, runId, digest);
    await fetchBitruePrices(client, runId, digest);
    await fetchCryptoComparePrices(client, runId, digest);
    await fetchLBankPrices(client, runId, digest);
    await sendAdminDigests(client, digest);
  } finally {
    endRun(runId);
    await aggregateAndDetect(client, runId);
    console.log('🌀 All datasources fetched and aggregated. Waiting for next cycle...');
  }
}

module.exports = { fetchAllDatasourcePrices };
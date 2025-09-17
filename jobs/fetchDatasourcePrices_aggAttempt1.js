// ingest/fetchDatasourcePrices.js
// Per-run orchestrator + one DM digest per admin for *new* errors this run.

const { withRun } = require('../services/ingestRun');
const { newRunDigest } = require('../services/digest');
const { sendAdminDigests } = require('../services/alerts');

const { fetchCMCPrices }           = require('../datasources/coinmarketcap');
const { fetchCoinGeckoPrices }     = require('../datasources/coingecko');
const { fetchLBankPrices }         = require('../datasources/lbank');
const { fetchCryptoComparePrices } = require('../datasources/cryptocompare');
const { fetchBitruePrices }        = require('../datasources/bitrue');
const { fetchBitmartPrices }       = require('../datasources/bitmart');

const { aggregateAndDetect } = require('./aggregatePrices');

async function fetchAllDatasourcePrices(client) {
  // In-memory digest for this run (NOT stored in DB)
  const digest = newRunDigest();
  let runId = null;

  try {
    await withRun(
      client,
      null,               // create a new ingest_runs row
      'datasource',       // label stored in ingest_runs.digest
      async (rid) => {
        runId = rid;

        // Each datasource job accepts (client, rid, digestMap)
        await fetchCMCPrices(client, rid, digest);
        await fetchCoinGeckoPrices(client, rid, digest);
        await fetchBitmartPrices(client, rid, digest);
        await fetchBitruePrices(client, rid, digest);
        await fetchCryptoComparePrices(client, rid, digest);
        await fetchLBankPrices(client, rid, digest);

        // Send one admin digest summarizing *new* errors opened during this run
        await sendAdminDigests(client, digest);
      },
      { label: 'datasource' }
    );
  } finally {
    if (runId) {
      await aggregateAndDetect(client, runId);
    }
    console.log('🌀 All datasources fetched and aggregated. Waiting for next cycle...');
  }
}

module.exports = { fetchAllDatasourcePrices };
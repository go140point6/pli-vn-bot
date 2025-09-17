// ingest/fetchDatasourcePrices.js
// Per-run orchestrator that ONLY writes datasource prices to the DB.
// — No stall detection
// — No aggregation
// — No admin/owner DM digests for datasource runs

const { withRun } = require('../services/ingestRun');

// Keep a digest object for compatibility with existing datasource fetchers,
// but we do NOT dispatch or summarize it anywhere.
const { newRunDigest } = require('../services/digest');

const { fetchCMCPrices }           = require('../datasources/coinmarketcap');
const { fetchCoinGeckoPrices }     = require('../datasources/coingecko');
const { fetchLBankPrices }         = require('../datasources/lbank');
const { fetchCryptoComparePrices } = require('../datasources/cryptocompare');
const { fetchBitruePrices }        = require('../datasources/bitrue');
const { fetchBitmartPrices }       = require('../datasources/bitmart');

async function fetchAllDatasourcePrices(client) {
  await withRun(
    client,
    null,               // create a new ingest_runs row
    'datasource',       // label stored in ingest_runs.digest
    async (rid) => {
      // NOTE: digest is NOT sent anywhere; it only satisfies fetcher signatures.
      const digest = newRunDigest();

      // Each datasource job accepts (client, rid, digestMap)
      await fetchCMCPrices(client, rid, digest);
      await fetchCoinGeckoPrices(client, rid, digest);
      await fetchBitmartPrices(client, rid, digest);
      await fetchBitruePrices(client, rid, digest);
      await fetchCryptoComparePrices(client, rid, digest);
      await fetchLBankPrices(client, rid, digest);
    },
    { label: 'datasource' }
  );

  console.log('🌀 All datasources fetched. Waiting for next cycle...');
}

module.exports = { fetchAllDatasourcePrices };

// datasources/cryptocompare.js
const axios = require('axios');
const { withRun } = require('../services/ingestRun');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { THROTTLE_MS, getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');
const { sleep } = require('../utils/sleep');

async function fetchCryptoComparePrices(client, runId, digest) {
  return withRun(client, runId, digest, async (rid, d) => {
    const datasource = 'cryptocompare';
    try {
      const apiMeta = mustGetApiMeta(datasource);
      const rows = selMappingsBySource.all(datasource);
      if (!rows.length) return console.warn('⚠️ No mappings for cryptocompare.');

      const headers = buildHeaders(datasource, apiMeta.headers);

      let ok = 0, bad = 0;
      for (const row of rows) {
        const base = String(row.base || '').toUpperCase();
        const quote = String(row.quote || '').toUpperCase();
        const url = apiMeta.base_url.replace('${base}', base).replace('${quote}', quote);

        try {
          const res = await axios.get(url, { headers });
          const path = apiMeta.response_path.replace('${quote}', quote);
          const priceVal = getNestedValue(res.data, path);
          const price = toNumOrNull(priceVal);

          if (price && price > 0) {
            insertSnapshot.run(rid, row.chain_id, row.contract_address, datasource, price);
            resolveDsErrorForAdmins({ datasource, row });
            ok++;
          } else {
            bad++;
            raiseDsErrorForAdmins({ datasource, row, message: `Invalid/missing price (path=${path})`, detail: { path, priceVal }, digest: d });
          }
        } catch (e) {
          bad++;
          raiseDsErrorForAdmins({ datasource, row, message: 'HTTP/parse error', detail: e.message, digest: d });
        }

        await sleep(THROTTLE_MS);
      }
      console.log(`✅ ${datasource}: ${ok} prices inserted, ${bad} errors.`);
    } catch (err) {
      console.error(`💥 ${datasource} failure:`, err.message);
    }
  });
}

module.exports = { fetchCryptoComparePrices };
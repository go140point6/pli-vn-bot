// datasources/coingecko.js
const axios = require('axios');
const { withRun } = require('../services/ingestRun');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');

async function fetchCoinGeckoPrices(client, runId, digest) {
  return withRun(client, runId, digest, async (rid, d) => {
    const datasource = 'coingecko';
    try {
      const apiMeta = mustGetApiMeta(datasource);
      const rows = selMappingsBySource.all(datasource);
      if (!rows.length) return console.warn(`⚠️ No mappings for ${datasource}.`);

      const ids = [...new Set(rows.map(r => String(r.datasource_pair_id || '').trim()))].join(',');
      const url = apiMeta.base_url.replace('${pair_id}', ids);
      const headers = buildHeaders(datasource, apiMeta.headers);

      console.log(`📡 ${datasource}: GET ${url}`);
      const res = await axios.get(url, { headers });

      let ok = 0, bad = 0;
      for (const row of rows) {
        const id = String(row.datasource_pair_id || '').trim();
        if (!id) {
          bad++;
          raiseDsErrorForAdmins({ datasource, row, message: 'Missing datasource_pair_id', detail: null, digest: d });
          continue;
        }
        const path = apiMeta.response_path.replace('${pair_id}', id);
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
      }
      console.log(`✅ ${datasource}: ${ok} prices inserted, ${bad} errors.`);
    } catch (err) {
      console.error(`💥 ${datasource} failure:`, err.message);
    }
  });
}

module.exports = { fetchCoinGeckoPrices };
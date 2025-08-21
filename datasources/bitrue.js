// datasources/bitrue.js
const axios = require('axios');
const { withRun } = require('../services/ingestRun');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { THROTTLE_MS, getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');
const { sleep } = require('../utils/sleep');
const { maybeForceStallPrice } = require('../utils/testing');

async function fetchBitruePrices(client, runId, digest) {
  return withRun(client, runId, digest, async (rid, d) => {
    const datasource = 'bitrue';
    try {
      const apiMeta = mustGetApiMeta(datasource);
      const rows = selMappingsBySource.all(datasource);
      if (!rows.length) return console.warn('⚠️ No mappings for bitrue.');

      const headers = buildHeaders(datasource, apiMeta.headers);

      let ok = 0, bad = 0;
      for (const row of rows) {
        const pairId = String(row.datasource_pair_id || '').trim();
        const url = apiMeta.base_url.replace('${pair_id}', pairId);

        try {
          const res = await axios.get(url, { headers });
          const priceVal = getNestedValue(res.data, apiMeta.response_path);
          const price = toNumOrNull(priceVal);

          if (price && price > 0) {
            // 🧪 Optional test override to simulate a stall (no-op if env not set)
            const p = maybeForceStallPrice({
              datasource,
              chain_id: row.chain_id,
              contract_address: row.contract_address,
              price
            });
            insertSnapshot.run(rid, row.chain_id, row.contract_address, datasource, p);
            resolveDsErrorForAdmins({ datasource, row });
            ok++;
          } else {
            bad++;
            raiseDsErrorForAdmins({ datasource, row, message: 'Invalid/missing price', detail: { pairId, priceVal }, digest: d });
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

module.exports = { fetchBitruePrices };
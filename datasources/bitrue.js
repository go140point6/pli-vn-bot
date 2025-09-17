// datasources/bitrue.js
const axios = require('axios');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { THROTTLE_MS, getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');
const { sleep } = require('../utils/sleep');
const { maybeForceStallPrice } = require('../dev/testing');

async function fetchBitruePrices(client, runId, digest) {
  const datasource = 'bitrue';
  try {
    const apiMeta = mustGetApiMeta(datasource);
    const rows = selMappingsBySource.all(datasource);

    if (!Array.isArray(rows) || rows.length === 0) {
      console.warn('⚠️ No mappings for bitrue.');
      return;
    }

    // Bitrue uses datasource_pair_id
    const safeRows = rows.filter(r => r && r.contract_address && r.datasource_pair_id);
    if (safeRows.length !== rows.length) {
      console.warn(`↪︎ ${datasource}: filtered out ${rows.length - safeRows.length} invalid mapping row(s).`);
    }
    if (safeRows.length === 0) {
      console.warn(`⚠️ ${datasource}: 0 valid rows after filtering.`);
      return;
    }

    const headers = buildHeaders(datasource, apiMeta.headers);

    let ok = 0, bad = 0;
    for (const row of safeRows) {
      const pairId = String(row.datasource_pair_id || '').trim();
      const url = apiMeta.base_url.replace('${pair_id}', pairId);

      try {
        const res = await axios.get(url, { headers });
        const priceVal = getNestedValue(res.data, apiMeta.response_path);
        const rawPrice = toNumOrNull(priceVal);

        if (rawPrice && rawPrice > 0) {
          // Optional test override to simulate a stall (no-op if env not set)
          const price = maybeForceStallPrice({
            datasource,
            chain_id: row.chain_id,
            contract_address: row.contract_address,
            price: rawPrice
          });

          insertSnapshot.run(runId, row.chain_id, row.contract_address, datasource, price);
          resolveDsErrorForAdmins({ datasource, row });
          ok++;
        } else {
          bad++;
          raiseDsErrorForAdmins({
            datasource,
            row,
            message: 'Invalid/missing price',
            detail: { pairId, priceVal },
            digest
          });
        }
      } catch (e) {
        bad++;
        raiseDsErrorForAdmins({
          datasource,
          row,
          message: 'HTTP/parse error',
          detail: e.message,
          digest
        });
      }

      await sleep(THROTTLE_MS);
    }
    console.log(`✅ ${datasource}: ${ok} prices inserted, ${bad} errors.`);
  } catch (err) {
    console.error(`💥 ${datasource} failure:`, err.message);
  }
}

module.exports = { fetchBitruePrices };

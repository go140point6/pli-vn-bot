// datasources/coingecko.js
const axios = require('axios');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');

async function fetchCoinGeckoPrices(client, runId, digest) {
  const datasource = 'coingecko';
  try {
    const apiMeta = mustGetApiMeta(datasource);
    const rows = selMappingsBySource.all(datasource);

    if (!Array.isArray(rows) || rows.length === 0) {
      console.warn(`⚠️ No mappings for ${datasource}.`);
      return;
    }

    // CoinGecko uses datasource_pair_id
    const safeRows = rows.filter(r => r && r.contract_address && r.datasource_pair_id);
    if (safeRows.length !== rows.length) {
      console.warn(`↪︎ ${datasource}: filtered out ${rows.length - safeRows.length} invalid mapping row(s).`);
    }
    if (safeRows.length === 0) {
      console.warn(`⚠️ ${datasource}: 0 valid rows after filtering.`);
      return;
    }

    const ids = [...new Set(safeRows.map(r => String(r.datasource_pair_id || '').trim()))].join(',');
    const url = apiMeta.base_url.replace('${pair_id}', ids);
    const headers = buildHeaders(datasource, apiMeta.headers);

    // console.log(`📡 ${datasource}: GET ${url}`);
    const res = await axios.get(url, { headers });

    let ok = 0, bad = 0;
    for (const row of safeRows) {
      const id = String(row.datasource_pair_id || '').trim();
      const path = apiMeta.response_path.replace('${pair_id}', id);

      const priceVal = getNestedValue(res.data, path);
      const price = toNumOrNull(priceVal);

      if (price && price > 0) {
        insertSnapshot.run(runId, row.chain_id, row.contract_address, datasource, price);
        resolveDsErrorForAdmins({ datasource, row });
        ok++;
      } else {
        bad++;
        raiseDsErrorForAdmins({
          datasource,
          row,
          message: `Invalid/missing price (path=${path})`,
          detail: { path, priceVal },
          digest
        });
      }
    }
    console.log(`✅ ${datasource}: ${ok} prices inserted, ${bad} errors.`);
  } catch (err) {
    console.error(`💥 ${datasource} failure:`, err.message);
  }
}

module.exports = { fetchCoinGeckoPrices };

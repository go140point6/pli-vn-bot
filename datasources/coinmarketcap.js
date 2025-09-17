// datasources/coinmarketcap.js
const axios = require('axios');
const { insertSnapshot, selMappingsBySource } = require('../db/statements');
const { raiseDsErrorForAdmins, resolveDsErrorForAdmins } = require('../services/alerts');
const { getNestedValue, toNumOrNull, mustGetApiMeta, buildHeaders } = require('./common');

async function fetchCMCPrices(client, runId, digest) {
  const datasource = 'coinmarketcap';
  try {
    const apiMeta = mustGetApiMeta(datasource);
    const rows = selMappingsBySource.all(datasource);

    if (!Array.isArray(rows) || rows.length === 0) {
      console.warn(`⚠️ No mappings for ${datasource}.`);
      return;
    }

    // CMC uses base/quote fields, not datasource_pair_id
    const safeRows = rows.filter(r => r && r.contract_address && r.base && r.quote);
    if (safeRows.length !== rows.length) {
      console.warn(`↪︎ ${datasource}: filtered out ${rows.length - safeRows.length} invalid mapping row(s).`);
    }
    if (safeRows.length === 0) {
      console.warn(`⚠️ ${datasource}: 0 valid rows after filtering.`);
      return;
    }

    const baseSet  = [...new Set(safeRows.map(r => String(r.base  || '').toUpperCase()))].join(',');
    const quoteSet = [...new Set(safeRows.map(r => String(r.quote || '').toUpperCase()))].join(',');
    const url = apiMeta.base_url.replace('${base}', baseSet).replace('${quote}', quoteSet);
    const headers = buildHeaders(datasource, apiMeta.headers);

    // console.log(`📡 ${datasource}: GET ${url}`);
    const res = await axios.get(url, { headers });

    let ok = 0, bad = 0;
    for (const row of safeRows) {
      const base = String(row.base || '').toUpperCase();
      const quote = String(row.quote || '').toUpperCase();
      const path = apiMeta.response_path.replace('${base}', base).replace('${quote}', quote);

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

module.exports = { fetchCMCPrices };

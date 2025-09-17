// preflight.js
// Validate CSVs used by insertFullDB.js WITHOUT writing to the DB.
// - Verifies chain_id, Discord IDs, 0x/xdc addresses (EIP-55), JSON columns
// - Checks cross-file references and uniqueness (per your schema)
// - Prints a console summary and writes ../data/preflight-report.json
//
// Run: node preflight.js
//
// Tip: if you haven't already: npm i js-sha3 csv-parser

const path = require('path');
const fs = require('fs');

// Utils (adjust paths if your layout differs)
const { loadCSV } = require('../utils/csv');
const { isDiscordId } = require('../utils/discord');
const { jsonOrNull } = require('../utils/json');
const { parseChainId } = require('../utils/chain');
const { normalizeAddressAny } = require('../utils/address');

// Data folder
const DATA = (p) => path.join(__dirname, '../data', p);

// Report structure
const report = {
  files: {}, // fileKey -> { rows, errors:[], warnings:[] }
  totals: { rows: 0, errors: 0, warnings: 0 },
};

// Helper to start a file section
function startFile(key) {
  report.files[key] = { rows: 0, errors: [], warnings: [] };
  return report.files[key];
}
function issue(list, file, line, code, msg, extra = undefined) {
  list.push({ line, code, msg, extra });
}
function incTotals(fileRec) {
  report.totals.rows += fileRec.rows;
  report.totals.errors += fileRec.errors.length;
  report.totals.warnings += fileRec.warnings.length;
}

// In-memory sets/maps for cross-file checks
const seen = {
  datasources: new Set(),                      // name
  validators: new Set(),                       // `${chain_id}:${lowerAddr}`
  contracts: new Set(),                        // `${chain_id}:${lowerAddr}`
  dcmPK: new Set(),                            // `${datasource_name}:${chain_id}:${contract_lower}`
  dcmDatasourcePair: new Set(),                // `${datasource_name}:${datasource_pair_id}`
};

// ---- Validators ----
async function checkDatasources() {
  const f = startFile('datasources.csv');
  const dupCheck = new Set();
  let line = 1; // header

  await loadCSV(DATA('datasources.csv'), (row) => {
    line++;
    const name = (row.datasource_name || '').toLowerCase().trim();
    if (!name) {
      issue(f.errors, 'datasources.csv', line, 'EMPTY_NAME', 'datasource_name is required');
      return;
    }
    if (dupCheck.has(name)) {
      issue(f.errors, 'datasources.csv', line, 'DUP_NAME', `Duplicate datasource_name '${name}'`);
    } else {
      dupCheck.add(name);
      seen.datasources.add(name);
    }
    f.rows++;
  });

  return f;
}

async function checkUsers() {
  const f = startFile('users.csv');
  let line = 1;

  await loadCSV(DATA('users.csv'), (row) => {
    line++;
    const discord_id = row.discord_id?.trim();
    if (!isDiscordId(discord_id)) {
      issue(f.errors, 'users.csv', line, 'BAD_DISCORD_ID', `Invalid discord_id: '${row.discord_id}'`);
    }

    const accepts_dm = row.accepts_dm !== undefined ? (parseInt(row.accepts_dm, 10) || 0) : 0;
    if (![0, 1].includes(accepts_dm)) {
      issue(f.errors, 'users.csv', line, 'BAD_ACCEPTS_DM', `accepts_dm must be 0 or 1, got '${row.accepts_dm}'`);
    }

    const wt = row.warning_threshold ? parseInt(row.warning_threshold, 10) : null;
    const ct = row.critical_threshold ? parseInt(row.critical_threshold, 10) : null;
    if (row.warning_threshold && Number.isNaN(wt)) {
      issue(f.errors, 'users.csv', line, 'BAD_WARN_T', `warning_threshold must be integer or empty, got '${row.warning_threshold}'`);
    }
    if (row.critical_threshold && Number.isNaN(ct)) {
      issue(f.errors, 'users.csv', line, 'BAD_CRIT_T', `critical_threshold must be integer or empty, got '${row.critical_threshold}'`);
    }
    if (wt != null && ct != null && !(ct < wt)) {
      issue(f.errors, 'users.csv', line, 'THRESHOLD_ORDER', `critical_threshold (${ct}) must be < warning_threshold (${wt})`);
    }

    f.rows++;
  });

  return f;
}

async function checkValidators() {
  const f = startFile('validators.csv');
  const pkCheck = new Set();
  let line = 1;

  await loadCSV(DATA('validators.csv'), (row) => {
    line++;
    const chain_id = parseChainId(row.chain_id);
    if (chain_id === null) {
      issue(f.errors, 'validators.csv', line, 'BAD_CHAIN_ID', `Missing/invalid chain_id: '${row.chain_id}'`);
      return;
    }

    let lower, eip55, format;
    try {
      ({ lower, eip55, format } = normalizeAddressAny(String(row.address || '')));
    } catch (e) {
      issue(f.errors, 'validators.csv', line, 'BAD_ADDRESS', `Invalid address '${row.address}': ${e.message}`);
      return;
    }
    const discord_id = row.discord_id?.trim();
    if (!isDiscordId(discord_id)) {
      issue(f.errors, 'validators.csv', line, 'BAD_DISCORD_ID', `Invalid discord_id: '${row.discord_id}'`);
    }

    const pk = `${chain_id}:${lower}`;
    if (pkCheck.has(pk)) {
      issue(f.errors, 'validators.csv', line, 'DUP_VALIDATOR', `Duplicate (chain_id,address) = ${pk}`);
    } else {
      pkCheck.add(pk);
      seen.validators.add(pk);
    }

    // UX warning if not already canonical lowercase 0x
    if (String(row.address).trim() !== lower) {
      issue(f.warnings, 'validators.csv', line, 'NON_CANONICAL', `Not canonical lowercase 0x; will be normalized to '${lower}' (input format: ${format}, EIP-55: ${eip55})`);
    }

    f.rows++;
  });

  return f;
}

async function checkContracts() {
  const f = startFile('contracts.csv');
  const pkCheck = new Set();
  let line = 1;

  await loadCSV(DATA('contracts.csv'), (row) => {
    line++;
    const chain_id = parseChainId(row.chain_id);
    if (chain_id === null) {
      issue(f.errors, 'contracts.csv', line, 'BAD_CHAIN_ID', `Missing/invalid chain_id: '${row.chain_id}'`);
      return;
    }

    let lower, eip55, format;
    try {
      ({ lower, eip55, format } = normalizeAddressAny(String(row.address || ''))); // throws on invalid/mismatched EIP-55
    } catch (e) {
      issue(f.errors, 'contracts.csv', line, 'BAD_ADDRESS', `Invalid address '${row.address}': ${e.message}`);
      return;
    }

    const base = (row.base || '').trim();
    const quote = (row.quote || '').trim();
    if (!base || !quote) {
      issue(f.errors, 'contracts.csv', line, 'MISSING_BASE_QUOTE', 'Both base and quote are required');
    }

    const active = row.active?.toString().trim();
    if (active !== undefined && active !== '' && !['0', '1'].includes(active)) {
      issue(f.errors, 'contracts.csv', line, 'BAD_ACTIVE', `active must be 0 or 1, got '${row.active}'`);
    }

    const pk = `${chain_id}:${lower}`;
    if (pkCheck.has(pk)) {
      issue(f.errors, 'contracts.csv', line, 'DUP_CONTRACT', `Duplicate (chain_id,address) = ${pk}`);
    } else {
      pkCheck.add(pk);
      seen.contracts.add(pk);
    }

    if (String(row.address).trim() !== lower) {
      issue(f.warnings, 'contracts.csv', line, 'NON_CANONICAL', `Not canonical lowercase 0x; will be normalized to '${lower}' (input format: ${format}, EIP-55: ${eip55})`);
    }

    f.rows++;
  });

  return f;
}

async function checkValidatorContracts() {
  const f = startFile('validator_contracts.csv');
  const pkCheck = new Set();
  let line = 1;

  await loadCSV(DATA('validator_contracts.csv'), (row) => {
    line++;
    const chain_id = parseChainId(row.chain_id);
    if (chain_id === null) {
      issue(f.errors, 'validator_contracts.csv', line, 'BAD_CHAIN_ID', `Missing/invalid chain_id: '${row.chain_id}'`);
      return;
    }

    let v, c;
    try { v = normalizeAddressAny(String(row.validator_address || '')); }
    catch (e) {
      issue(f.errors, 'validator_contracts.csv', line, 'BAD_VALIDATOR_ADDR', `Invalid validator_address '${row.validator_address}': ${e.message}`);
      return;
    }
    try { c = normalizeAddressAny(String(row.contract_address || '')); }
    catch (e) {
      issue(f.errors, 'validator_contracts.csv', line, 'BAD_CONTRACT_ADDR', `Invalid contract_address '${row.contract_address}': ${e.message}`);
      return;
    }

    // FK presence in CSV data
    const vKey = `${chain_id}:${v.lower}`;
    const cKey = `${chain_id}:${c.lower}`;
    if (!seen.validators.has(vKey)) {
      issue(f.errors, 'validator_contracts.csv', line, 'MISSING_VALIDATOR', `No matching validator (chain_id,address) = ${vKey} in validators.csv`);
    }
    if (!seen.contracts.has(cKey)) {
      issue(f.errors, 'validator_contracts.csv', line, 'MISSING_CONTRACT', `No matching contract (chain_id,address) = ${cKey} in contracts.csv`);
    }

    const pk = `${chain_id}:${v.lower}:${c.lower}`;
    if (pkCheck.has(pk)) {
      issue(f.errors, 'validator_contracts.csv', line, 'DUP_REL', `Duplicate mapping (chain_id,validator,contract) = ${pk}`);
    } else {
      pkCheck.add(pk);
    }

    f.rows++;
  });

  return f;
}

async function checkDatasourceContractMap() {
  const f = startFile('datasource_contract_map.csv');
  let line = 1;

  await loadCSV(DATA('datasource_contract_map.csv'), (row) => {
    line++;
    const datasource_name = (row.datasource_name || '').toLowerCase().trim();
    if (!datasource_name) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'EMPTY_DS', 'datasource_name is required');
      return;
    }
    if (!seen.datasources.has(datasource_name)) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'MISSING_DS', `datasource '${datasource_name}' not found in datasources.csv`);
    }

    const chain_id = parseChainId(row.chain_id);
    if (chain_id === null) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'BAD_CHAIN_ID', `Missing/invalid chain_id: '${row.chain_id}'`);
      return;
    }

    let lower;
    try { ({ lower } = normalizeAddressAny(String(row.contract_address || ''))); }
    catch (e) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'BAD_CONTRACT_ADDR', `Invalid contract_address '${row.contract_address}': ${e.message}`);
      return;
    }

    const base = (row.base || '').trim();
    const quote = (row.quote || '').trim();
    if (!base || !quote) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'MISSING_BASE_QUOTE', 'Both base and quote are required');
    }

    const contract_pair_id = (row.contract_pair_id || '').trim();
    if (!contract_pair_id) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'EMPTY_CONTRACT_PAIR_ID', 'contract_pair_id is required');
    }

    const datasource_pair_id = (row.datasource_pair_id || '').trim() || null;

    // FKs: contract must exist
    if (!seen.contracts.has(`${chain_id}:${lower}`)) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'MISSING_CONTRACT', `No matching contract (chain_id,address) = ${chain_id}:${lower} in contracts.csv`);
    }

    // PK uniqueness
    const pk = `${datasource_name}:${chain_id}:${lower}`;
    if (seen.dcmPK.has(pk)) {
      issue(f.errors, 'datasource_contract_map.csv', line, 'DUP_PK', `Duplicate (datasource_name,chain_id,contract_address) = ${pk}`);
    } else {
      seen.dcmPK.add(pk);
    }

    // UNIQUE(datasource_name, datasource_pair_id) when datasource_pair_id is not null
    if (datasource_pair_id) {
      const upk = `${datasource_name}:${datasource_pair_id}`;
      if (seen.dcmDatasourcePair.has(upk)) {
        issue(f.errors, 'datasource_contract_map.csv', line, 'DUP_DS_PAIR', `Duplicate (datasource_name,datasource_pair_id) = ${upk}`);
      } else {
        seen.dcmDatasourcePair.add(upk);
      }
    }

    f.rows++;
  });

  return f;
}

async function checkMnRpc() {
  const f = startFile('mn_rpc.csv');
  let line = 1;

  await loadCSV(DATA('mn_rpc.csv'), (row) => {
    line++;
    const mn = (row.mn || '').trim();
    const name = (row.name || '').trim();
    if (!mn || !name) {
      issue(f.errors, 'mn_rpc.csv', line, 'MISSING_FIELDS', 'mn and name are required');
    }
    const rawId = row.discord_id?.toString().trim();
    if (rawId && rawId.toLowerCase() !== 'null' && !isDiscordId(rawId)) {
      issue(f.errors, 'mn_rpc.csv', line, 'BAD_DISCORD_ID', `Invalid discord_id: '${row.discord_id}'`);
    }
    const pub = row.public?.toString().trim();
    if (pub !== undefined && pub !== '' && !['0', '1'].includes(pub)) {
      issue(f.errors, 'mn_rpc.csv', line, 'BAD_PUBLIC', `public must be 0 or 1, got '${row.public}'`);
    }

    f.rows++;
  });

  return f;
}

async function checkMnWss() {
  const f = startFile('mn_wss.csv');
  let line = 1;

  await loadCSV(DATA('mn_wss.csv'), (row) => {
    line++;
    const mn = (row.mn || '').trim();
    const name = (row.name || '').trim();
    if (!mn || !name) {
      issue(f.errors, 'mn_wss.csv', line, 'MISSING_FIELDS', 'mn and name are required');
    }
    const rawId = row.discord_id?.toString().trim();
    if (rawId && rawId.toLowerCase() !== 'null' && !isDiscordId(rawId)) {
      issue(f.errors, 'mn_wss.csv', line, 'BAD_DISCORD_ID', `Invalid discord_id: '${row.discord_id}'`);
    }
    const pub = row.public?.toString().trim();
    if (pub !== undefined && pub !== '' && !['0', '1'].includes(pub)) {
      issue(f.errors, 'mn_wss.csv', line, 'BAD_PUBLIC', `public must be 0 or 1, got '${row.public}'`);
    }

    f.rows++;
  });

  return f;
}

async function checkDatasourceApis() {
  const f = startFile('datasource_apis.csv');
  let line = 1;

  await loadCSV(DATA('datasource_apis.csv'), (row) => {
    line++;
    const datasource_name = (row.datasource_name || '').toLowerCase().trim();
    const base_url = (row.base_url || '').trim();
    const response_path = (row.response_path || '').trim();

    if (!datasource_name) {
      issue(f.errors, 'datasource_apis.csv', line, 'EMPTY_DS', 'datasource_name is required');
    } else if (!seen.datasources.has(datasource_name)) {
      issue(f.errors, 'datasource_apis.csv', line, 'MISSING_DS', `datasource '${datasource_name}' not found in datasources.csv`);
    }

    if (!base_url) {
      issue(f.errors, 'datasource_apis.csv', line, 'EMPTY_BASE_URL', 'base_url is required');
    }
    if (!response_path) {
      issue(f.errors, 'datasource_apis.csv', line, 'EMPTY_RESPONSE_PATH', 'response_path is required');
    }

    if (row.headers !== undefined && String(row.headers).trim() !== '') {
      const j = jsonOrNull(row.headers);
      if (!j) {
        issue(f.errors, 'datasource_apis.csv', line, 'BAD_HEADERS_JSON', 'headers must be valid JSON or empty');
      }
    }

    f.rows++;
  });

  return f;
}

// ---- Runner ----
(async () => {
  try {
    console.log('\n🔎 Preflight validation starting...\n');

    // Order matters to resolve cross-file FKs:
    await checkDatasources();
    await checkUsers();
    await checkValidators();
    await checkContracts();
    await checkValidatorContracts();
    await checkDatasourceContractMap();
    await checkMnRpc();
    await checkMnWss();
    await checkDatasourceApis();

    // Aggregate & print summary
    Object.values(report.files).forEach(incTotals);

    const pad = (n, w = 5) => String(n).padStart(w, ' ');
    console.log('===== Preflight Summary =====');
    console.log(`Rows:     ${pad(report.totals.rows)}  Errors: ${pad(report.totals.errors)}  Warnings: ${pad(report.totals.warnings)}\n`);

    for (const [file, rec] of Object.entries(report.files)) {
      console.log(`${file}: rows=${rec.rows}, errors=${rec.errors.length}, warnings=${rec.warnings.length}`);
      const show = (arr, label) => {
        if (!arr.length) return;
        const max = Math.min(10, arr.length);
        console.log(`  ${label} (showing ${max}/${arr.length}):`);
        for (let i = 0; i < max; i++) {
          const it = arr[i];
          console.log(`    [${it.code}] line ${it.line}: ${it.msg}`);
        }
      };
      show(rec.errors, 'errors');
      show(rec.warnings, 'warnings');
      console.log('');
    }

    // Write JSON report
    const outPath = DATA('preflight-report.json');
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2), 'utf-8');
    console.log(`📄 Full report written to: ${outPath}\n`);

    // Non-zero exit on errors
    if (report.totals.errors > 0) {
      console.error('❌ Preflight failed: fix the errors above.');
      process.exitCode = 1;
    } else {
      console.log('✅ Preflight passed with no errors.');
    }
  } catch (err) {
    console.error('💥 Preflight crashed:', err);
    process.exitCode = 2;
  }
})();
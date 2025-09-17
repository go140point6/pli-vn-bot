// insertFullDBv2.js
// Imports CSVs into the multi-chain schema created by createDB.js.
//
// Features:
// - Normalizes 0x/xdc to canonical lowercase 0x for keys
// - Computes EIP-55 for display; preserves user input format (0x/xdc) via addr_format
// - Validates Discord IDs, chain IDs, addresses, and JSON headers
// - Per-table transactions (better-sqlite3)
// - Plain INSERTs with try/catch logging (no INSERT OR IGNORE)
// - imports validator_owners from validator_owners.csv
// - datasource_apis and datasource_contract_map insert only if referenced rows exist
// - POLISH: validator_contracts is existence-aware
// - POLISH: DCM & datasource_apis print inserted/skipped summaries

const path = require('path');

// ---- Utils ----
const { loadCSV } = require('../utils/csv');
const { isDiscordId } = require('../utils/discord');
const { jsonOrNull } = require('../utils/json');
const { parseChainId } = require('../utils/chain');
const {
  normalizeAddressAny, // -> { lower, eip55, format }
} = require('../utils/address');

// ---- DB (uniform entrypoint) ----
const { getDb } = require('../db');
const db = getDb();                        // opens the shared DB (fileMustExist: true)
const tx = (fn) => db.transaction(fn);

// ---------- Importers ----------

async function importDatasources() {
  console.log('🧪 Importing datasources...');
  const insert = db.prepare(`INSERT INTO datasources (datasource_name) VALUES (?);`);
  const runTx = tx((names) => { for (const name of names) insert.run(name); });

  const names = new Set();
  await loadCSV(path.join(__dirname, '../data/datasources.csv'), (row) => {
    try {
      const name = (row.datasource_name || '').toLowerCase().trim();
      if (name) names.add(name);
    } catch (e) {
      console.error('❌ datasource row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM datasources`).get().c;
    runTx([...names]);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM datasources`).get().c;
    console.log(`✅ datasources done — inserted=${after - before}`);
  } catch (e) {
    console.error('❌ datasources insert failed:', e.message);
  }
}

async function importUsers() {
  console.log('👤 Importing users...');

  const insertDefaults = db.prepare(`
    INSERT INTO users (discord_id, discord_name, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?)
  `);
  const insertAcceptsOnly = db.prepare(`
    INSERT INTO users (discord_id, discord_name, accepts_dm, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?, ?)
  `);
  const insertAdminOnly = db.prepare(`
    INSERT INTO users (discord_id, discord_name, is_admin, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?, ?)
  `);
  const insertBoth = db.prepare(`
    INSERT INTO users (discord_id, discord_name, accepts_dm, is_admin, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/users.csv'), (row) => {
    try {
      const discord_id = row.discord_id?.trim();
      if (!isDiscordId(discord_id)) {
        console.warn(`⚠️ Skip user: invalid discord_id: ${row.discord_id}`);
        return;
      }

      const discord_name = row.discord_name ?? null;

      let accepts_dm;
      const rawAcc = row.accepts_dm?.toString().trim();
      if (rawAcc === '0' || rawAcc === '1') accepts_dm = parseInt(rawAcc, 10);

      let is_admin;
      const rawAdm = row.is_admin?.toString().trim();
      if (rawAdm === '0' || rawAdm === '1') is_admin = parseInt(rawAdm, 10);

      const warning_threshold =
        row.warning_threshold !== undefined && row.warning_threshold !== ''
          ? parseInt(row.warning_threshold, 10)
          : null;

      const critical_threshold =
        row.critical_threshold !== undefined && row.critical_threshold !== ''
          ? parseInt(row.critical_threshold, 10)
          : null;

      rows.push({
        discord_id,
        discord_name,
        accepts_dm,
        is_admin,
        warning_threshold,
        critical_threshold,
      });
    } catch (e) {
      console.error('❌ user row error:', row, e.message);
    }
  });

  const runTx = db.transaction((batch) => {
    for (const r of batch) {
      const wt = Number.isFinite(r.warning_threshold) ? r.warning_threshold : null;
      const ct = Number.isFinite(r.critical_threshold) ? r.critical_threshold : null;

      if (r.accepts_dm !== undefined && r.is_admin !== undefined) {
        insertBoth.run(r.discord_id, r.discord_name, r.accepts_dm, r.is_admin, wt, ct);
      } else if (r.accepts_dm !== undefined) {
        insertAcceptsOnly.run(r.discord_id, r.discord_name, r.accepts_dm, wt, ct);
      } else if (r.is_admin !== undefined) {
        insertAdminOnly.run(r.discord_id, r.discord_name, r.is_admin, wt, ct);
      } else {
        insertDefaults.run(r.discord_id, r.discord_name, wt, ct);
      }
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM users`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM users`).get().c;
    console.log(`✅ users done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ users insert failed:', e.message);
  }
}

async function importValidators() {
  console.log('🔗 Importing validators...');

  const insertValidator = db.prepare(`
    INSERT INTO validators (chain_id, address, address_eip55, addr_format)
    VALUES (?, ?, ?, ?);
  `);

  const runTx = tx((rows) => {
    for (const r of rows) {
      insertValidator.run(r.chain_id, r.address, r.address_eip55, r.addr_format);
    }
  });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/validators.csv'), (row) => {
    try {
      const chain_id = parseChainId(row.chain_id);
      if (chain_id === null) {
        console.warn(`⚠️ Skip validator: missing/invalid chain_id: ${JSON.stringify(row)}`);
        return;
      }

      const { lower, eip55, format } = normalizeAddressAny(String(row.address || ''));

      rows.push({
        chain_id,
        address: lower,
        address_eip55: eip55,
        addr_format: (format === 'xdc') ? 'xdc' : '0x',
      });
    } catch (e) {
      console.error('❌ validator row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM validators`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM validators`).get().c;
    console.log(`✅ validators done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ validators insert failed:', e.message);
  }
}

async function importValidatorOwners() {
  console.log('👥 Importing validator_owners...');
  const insertOwner = db.prepare(`
    INSERT INTO validator_owners (chain_id, validator_address, discord_id)
    VALUES (?, ?, ?);
  `);

  const runTx = tx((rows) => {
    for (const r of rows) insertOwner.run(r.chain_id, r.validator_address, r.discord_id);
  });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/validator_owners.csv'), (row) => {
    try {
      const chain_id = parseChainId(row.chain_id);
      if (chain_id === null) {
        console.warn(`⚠️ Skip validator_owner: invalid chain_id: ${JSON.stringify(row)}`);
        return;
      }

      const { lower } = normalizeAddressAny(String(row.validator_address || ''));
      const rawId = row.discord_id?.toString().trim();
      if (!rawId || rawId.toLowerCase() === 'null') {
        console.warn(`⚠️ Skip validator_owner: missing discord_id: ${JSON.stringify(row)}`);
        return;
      }
      if (!isDiscordId(rawId)) {
        console.warn(`⚠️ Skip validator_owner: invalid discord_id: ${row.discord_id}`);
        return;
      }

      rows.push({ chain_id, validator_address: lower, discord_id: rawId });
    } catch (e) {
      console.error('❌ validator_owners row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM validator_owners`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM validator_owners`).get().c;
    console.log(`✅ validator_owners done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ validator_owners insert failed:', e.message);
  }
}

async function importContracts() {
  console.log('📄 Importing contracts...');
  const insert = db.prepare(`
    INSERT INTO contracts (chain_id, address, address_eip55, addr_format, pair, base, quote, active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
  `);
  const runTx = tx((rows) => { for (const r of rows) insert.run(
    r.chain_id, r.address, r.address_eip55, r.addr_format, r.pair, r.base, r.quote, r.active
  ); });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/contracts.csv'), (row) => {
    try {
      const chain_id = parseChainId(row.chain_id);
      if (chain_id === null) {
        console.warn(`⚠️ Skip contract: missing/invalid chain_id: ${JSON.stringify(row)}`);
        return;
      }
      const { lower, eip55, format } = normalizeAddressAny(String(row.address || ''));
      const base = (row.base || '').trim();
      const quote = (row.quote || '').trim();
      if (!base || !quote) {
        console.warn(`⚠️ Skip contract: missing base/quote: ${JSON.stringify(row)}`);
        return;
      }
      const pair = row.pair ? String(row.pair).trim() : `${base}/${quote}`;
      const active = row.active?.toString().trim() === '1' ? 1 : 0;

      rows.push({
        chain_id,
        address: lower,
        address_eip55: eip55,
        addr_format: (format === 'xdc') ? 'xdc' : '0x',
        pair,
        base,
        quote,
        active,
      });
    } catch (e) {
      console.error('❌ contract row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM contracts`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM contracts`).get().c;
    console.log(`✅ contracts done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ contracts insert failed:', e.message);
  }
}

// POLISH 1: validator_contracts existence-aware + summary
async function importValidatorContracts() {
  console.log('📎 Importing validator_contracts...');
  const insertIfExists = db.prepare(`
    INSERT INTO validator_contracts (chain_id, validator_address, contract_address)
    SELECT ?, ?, ?
    WHERE EXISTS (SELECT 1 FROM validators v WHERE v.chain_id = ? AND v.address = ?)
      AND EXISTS (SELECT 1 FROM contracts  c WHERE c.chain_id = ? AND c.address = ?)
  `);

  const runTx = tx((rows) => {
    for (const r of rows) {
      const info = insertIfExists.run(
        r.chain_id, r.validator_address, r.contract_address,
        r.chain_id, r.validator_address,
        r.chain_id, r.contract_address
      );
      if (info.changes === 0) r.__skipped = true; else r.__inserted = true;
    }
  });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/validator_contracts.csv'), (row) => {
    try {
      const chain_id = parseChainId(row.chain_id);
      if (chain_id === null) return console.warn(`⚠️ Skip v_c: invalid chain_id: ${JSON.stringify(row)}`);
      const v = normalizeAddressAny(String(row.validator_address || ''));
      const c = normalizeAddressAny(String(row.contract_address || ''));
      rows.push({ chain_id, validator_address: v.lower, contract_address: c.lower });
    } catch (e) {
      console.error('❌ validator_contracts row error:', row, e.message);
    }
  });

  try { runTx(rows); } catch (e) { console.error('❌ validator_contracts insert failed:', e.message); }

  const ins = rows.filter(r => r.__inserted).length;
  const skip = rows.filter(r => r.__skipped).length;
  console.log(`✅ validator_contracts done — inserted=${ins}, skipped=${skip}, read=${rows.length}`);
}

// POLISH 2: DCM existence-aware + per-row warnings + summary
async function importDatasourceContractMap() {
  console.log('🗺️  Importing datasource_contract_map...');
  const insertIfExists = db.prepare(`
    INSERT INTO datasource_contract_map
      (datasource_name, chain_id, contract_address, contract_pair_id, datasource_pair_id, base, quote)
    SELECT ?, ?, ?, ?, ?, ?, ?
    WHERE EXISTS (SELECT 1 FROM datasources d WHERE d.datasource_name = ?)
      AND EXISTS (SELECT 1 FROM contracts c WHERE c.chain_id = ? AND c.address = ?)
  `);

  const runTx = tx((rows) => {
    for (const r of rows) {
      const info = insertIfExists.run(
        r.datasource_name, r.chain_id, r.contract_address, r.contract_pair_id, r.datasource_pair_id, r.base, r.quote,
        r.datasource_name, r.chain_id, r.contract_address
      );
      if (info.changes === 0) {
        console.warn(`↪︎ skipped dcm: missing datasource="${r.datasource_name}" or contract (chain=${r.chain_id}, addr=${r.contract_address})`);
        r.__skipped = true;
      } else {
        r.__inserted = true;
      }
    }
  });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/datasource_contract_map.csv'), (row) => {
    try {
      const datasource_name = (row.datasource_name || '').toLowerCase().trim();
      if (!datasource_name) return console.warn('⚠️ Skip dcm: empty datasource_name');

      const chain_id = parseChainId(row.chain_id);
      if (chain_id === null) return console.warn(`⚠️ Skip dcm: missing/invalid chain_id: ${JSON.stringify(row)}`);

      const { lower } = normalizeAddressAny(String(row.contract_address || ''));
      const contract_pair_id = (row.contract_pair_id || '').trim();
      if (!contract_pair_id) return console.warn('⚠️ Skip dcm: empty contract_pair_id');

      const base = (row.base || '').trim();
      const quote = (row.quote || '').trim();

      rows.push({
        datasource_name,
        chain_id,
        contract_address: lower,
        contract_pair_id,
        datasource_pair_id: row.datasource_pair_id ? String(row.datasource_pair_id).trim() : null,
        base,
        quote,
      });
    } catch (e) {
      console.error('❌ dcm row error:', row, e.message);
    }
  });

  try { runTx(rows); } catch (e) { console.error('❌ datasource_contract_map insert failed:', e.message); }

  const ins = rows.filter(r => r.__inserted).length;
  const skip = rows.filter(r => r.__skipped).length;
  console.log(`✅ datasource_contract_map done — total=${rows.length}, inserted=${ins}, skipped=${skip}`);
}

async function importMnRpc() {
  console.log('🌐 Importing mn_rpc...');
  const insert = db.prepare(`
    INSERT INTO mn_rpc (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);
  const runTx = tx((rows) => { for (const r of rows) insert.run(r.mn, r.name, r.discord_id, r.public); });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/mn_rpc.csv'), (row) => {
    try {
      const mn = (row.mn || '').trim();
      const name = (row.name || '').trim();
      if (!mn || !name) return console.warn(`⚠️ Skip mn_rpc: missing mn/name: ${JSON.stringify(row)}`);
      let discord_id = null;
      const rawId = row.discord_id?.toString().trim();
      if (rawId && rawId.toLowerCase() !== 'null') {
        if (isDiscordId(rawId)) discord_id = rawId;
        else console.warn(`⚠️ mn_rpc: invalid discord_id -> NULL: ${row.discord_id}`);
      }
      const isPublic = row.public?.toString().trim() === '1' ? 1 : 0;
      rows.push({ mn, name, discord_id, public: isPublic });
    } catch (e) {
      console.error('❌ mn_rpc row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM mn_rpc`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM mn_rpc`).get().c;
    console.log(`✅ mn_rpc done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ mn_rpc insert failed:', e.message);
  }
}

async function importMnWss() {
  console.log('📡 Importing mn_wss...');
  const insert = db.prepare(`
    INSERT INTO mn_wss (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);
  const runTx = tx((rows) => { for (const r of rows) insert.run(r.mn, r.name, r.discord_id, r.public); });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/mn_wss.csv'), (row) => {
    try {
      const mn = (row.mn || '').trim();
      const name = (row.name || '').trim();
      if (!mn || !name) return console.warn(`⚠️ Skip mn_wss: missing mn/name: ${JSON.stringify(row)}`);
      let discord_id = null;
      const rawId = row.discord_id?.toString().trim();
      if (rawId && rawId.toLowerCase() !== 'null') {
        if (isDiscordId(rawId)) discord_id = rawId;
        else console.warn(`⚠️ mn_wss: invalid discord_id -> NULL: ${row.discord_id}`);
      }
      const isPublic = row.public?.toString().trim() === '1' ? 1 : 0;
      rows.push({ mn, name, discord_id, public: isPublic });
    } catch (e) {
      console.error('❌ mn_wss row error:', row, e.message);
    }
  });

  try {
    const before = db.prepare(`SELECT COUNT(*) AS c FROM mn_wss`).get().c;
    runTx(rows);
    const after = db.prepare(`SELECT COUNT(*) AS c FROM mn_wss`).get().c;
    console.log(`✅ mn_wss done — inserted=${after - before}, read=${rows.length}`);
  } catch (e) {
    console.error('❌ mn_wss insert failed:', e.message);
  }
}

// POLISH 2: datasource_apis EXISTS + summary
async function importDatasourceApis() {
  console.log('🛰️ Importing datasource_apis...');

  const insertIfExists = db.prepare(`
    INSERT INTO datasource_apis (datasource_name, base_url, response_path, headers)
    SELECT ?, ?, ?, ?
    WHERE EXISTS (SELECT 1 FROM datasources d WHERE d.datasource_name = ?)
  `);

  const runTx = tx((rows) => {
    for (const r of rows) {
      const info = insertIfExists.run(
        r.datasource_name, r.base_url, r.response_path, r.headers_json, r.datasource_name
      );
      if (info.changes === 0) {
        console.warn(`↪︎ skipped datasource_api: no such datasource="${r.datasource_name}"`);
        r.__skipped = true;
      } else {
        r.__inserted = true;
      }
    }
  });

  const rows = [];
  await loadCSV(path.join(__dirname, '../data/datasource_apis.csv'), (row) => {
    try {
      const datasource_name = (row.datasource_name || '').toLowerCase().trim();
      const base_url = (row.base_url || '').trim();
      const response_path = (row.response_path || '').trim();
      if (!datasource_name || !base_url || !response_path) {
        console.warn(`⚠️ Skip datasource_api: missing datasource_name/base_url/response_path: ${JSON.stringify(row)}`);
        return;
      }

      let headers_json = null;
      if (row.headers !== undefined && String(row.headers).trim() !== '') {
        headers_json = jsonOrNull(row.headers);
        if (!headers_json) {
          console.warn(`⚠️ datasource_api: invalid JSON in headers; storing NULL. Row: ${JSON.stringify(row)}`);
        }
      }
      rows.push({ datasource_name, base_url, response_path, headers_json });
    } catch (e) {
      console.error('❌ datasource_api row error:', row, e.message);
    }
  });

  try { runTx(rows); } catch (e) { console.error('❌ datasource_apis insert failed:', e.message); }

  const ins = rows.filter(r => r.__inserted).length;
  const skip = rows.filter(r => r.__skipped).length;
  console.log(`✅ datasource_apis done — total=${rows.length}, inserted=${ins}, skipped=${skip}`);
}

// ---------- Main ----------
async function main() {
  try {
    console.log('\n🚀 Starting data import...\n');

    // Order matters: parents before dependents
    await importDatasources();
    await importUsers();
    await importValidators();
    await importValidatorOwners();
    await importContracts();
    await importValidatorContracts();     // existence-aware
    await importDatasourceContractMap();  // EXISTS checks + summary
    await importMnRpc();
    await importMnWss();
    await importDatasourceApis();         // EXISTS checks + summary

    console.log('\n✅ All data imported successfully!');
  } catch (err) {
    console.error('❌ Import failed:', err.stack || err.message);
  } finally {
    db.close();
    console.log('🔒 Database connection closed.\n');
  }
}

main();

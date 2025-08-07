const Database = require('better-sqlite3');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true,
});

// Generic CSV loader
function loadCSV(filePath, handleRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', handleRow)
      .on('end', resolve)
      .on('error', reject);
  });
}

// Import functions

async function importDatasources() {
  const stmt = db.prepare(`INSERT OR IGNORE INTO datasources (datasource_name) VALUES (?);`);
  await loadCSV(path.join(__dirname, '../data/datasources.csv'), (row) => {
    const name = (row.datasource_name || '').toLowerCase().trim();
    if (name) stmt.run(name);
  });
  console.log('🧪 Imported datasources');
}

async function importUsers() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO users (discord_id, discord_name, accepts_dm, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/users.csv'), (row) => {
    const discord_id = row.discord_id?.trim();
    if (!/^\d{17,20}$/.test(discord_id)) {
      console.warn(`⚠️ Skipping invalid discord_id: ${row.discord_id}`);
      return;
    }

    const accepts_dm = row.accepts_dm !== undefined ? parseInt(row.accepts_dm) : 0;
    stmt.run(discord_id, row.discord_name, accepts_dm, row.warning_threshold, row.critical_threshold);
  });
  console.log('👤 Imported users');
}

async function importValidators() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO validators (address, discord_id, discord_name)
    VALUES (?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/validators.csv'), (row) => {
    const discord_id = row.discord_id?.trim();
    if (!/^\d{17,20}$/.test(discord_id)) {
      console.warn(`⚠️ Skipping validator with invalid discord_id: ${row.discord_id}`);
      return;
    }

    stmt.run(row.address, discord_id, row.discord_name);
  });
  console.log('🔗 Imported validators');
}

async function importContracts() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO contracts (address, pair, base, quote, active)
    VALUES (?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/contracts.csv'), (row) => {
    const active = row.active === '1' ? 1 : 0;
    stmt.run(row.address, row.pair, row.base, row.quote, active);
  });
  console.log('📄 Imported contracts');
}

async function importDatasourceContractMap() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO datasource_contract_map
    (datasource_name, contract_address, contract_pair_id, datasource_pair_id, base, quote)
    VALUES (?, ?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/datasource_contract_map.csv'), (row) => {
    stmt.run(
      row.datasource_name.toLowerCase().trim(),
      row.contract_address,
      row.contract_pair_id,
      row.datasource_pair_id || null,
      row.base,
      row.quote
    );
  });
  console.log('🗺️  Imported datasource_contract_map');
}

async function importMnRpc() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO mn_rpc (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);

  await loadCSV(path.join(__dirname, '../data/mn_rpc.csv'), (row) => {
    const isPublic = row.public === '1' ? 1 : 0;
    const discord_id = row.discord_id?.toLowerCase() === 'null' || row.discord_id === ''
      ? null
      : row.discord_id.trim();

    stmt.run(row.mn, row.name, discord_id, isPublic);
  });

  console.log('🌐 Imported mn_rpc');
}

async function importMnWss() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO mn_wss (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/mn_wss.csv'), (row) => {
    const isPublic = row.public === '1' ? 1 : 0;
    const discord_id = row.discord_id?.toLowerCase() === 'null' || row.discord_id === ''
      ? null
      : row.discord_id.trim();

    stmt.run(row.mn, row.name, discord_id, isPublic);
  });
  console.log('📡 Imported mn_wss');
}

async function importValidatorContracts() {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO validator_contracts (validator_address, contract_address)
    VALUES (?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/validator_contracts.csv'), (row) => {
    stmt.run(row.validator_address, row.contract_address);
  });
  console.log('📎 Imported validator_contracts');
}

async function main() {
  try {
    console.log('\n🚀 Starting data import...\n');

    db.pragma('foreign_keys = ON'); // ✅ Ensure FK constraints are enforced

    await importDatasources();
    await importUsers();
    await importValidators();
    await importContracts();
    await importDatasourceContractMap();
    await importMnRpc();
    await importMnWss();
    await importValidatorContracts();

    console.log('\n✅ All data imported successfully!');
  } catch (err) {
    console.error('❌ Import failed:', err.message);
  } finally {
    db.close();
    console.log('🔒 Database connection closed.\n');
  }
}

main();
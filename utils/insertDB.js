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
  const stmt = db.prepare(`INSERT INTO datasources (datasource_name) VALUES (?);`);
  await loadCSV(path.join(__dirname, '../data/datasources.csv'), (row) => {
    try {
      const name = (row.datasource_name || '').toLowerCase().trim();
      if (name) stmt.run(name);
    } catch (err) {
      console.error('❌ Error inserting datasource:', row, err.message);
    }
  });
  console.log('🧪 Imported datasources');
}

async function importUsers() {
  const stmt = db.prepare(`
    INSERT INTO users (discord_id, discord_name, accepts_dm, warning_threshold, critical_threshold)
    VALUES (?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/users.csv'), (row) => {
    try {
      const discord_id = row.discord_id?.trim();
      if (!/^\d{17,20}$/.test(discord_id)) {
        console.warn(`⚠️ Skipping invalid discord_id: ${row.discord_id}`);
        return;
      }
      const accepts_dm = row.accepts_dm !== undefined ? parseInt(row.accepts_dm) : 0;
      stmt.run(
        discord_id,
        row.discord_name,
        accepts_dm,
        parseInt(row.warning_threshold),
        parseInt(row.critical_threshold)
      );
    } catch (err) {
      console.error('❌ Error inserting user:', row, err.message);
    }
  });
  console.log('👤 Imported users');
}

async function importValidators() {
  const stmt = db.prepare(`
    INSERT INTO validators (address, discord_id, discord_name)
    VALUES (?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/validators.csv'), (row) => {
    try {
      const discord_id = row.discord_id?.trim();
      if (!/^\d{17,20}$/.test(discord_id)) {
        console.warn(`⚠️ Skipping validator with invalid discord_id: ${row.discord_id}`);
        return;
      }
      stmt.run(row.address, discord_id, row.discord_name);
    } catch (err) {
      console.error('❌ Error inserting validator:', row, err.message);
    }
  });
  console.log('🔗 Imported validators');
}

async function importContracts() {
  const stmt = db.prepare(`
    INSERT INTO contracts (address, pair, base, quote, active)
    VALUES (?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/contracts.csv'), (row) => {
    try {
      const active = row.active === '1' ? 1 : 0;
      stmt.run(row.address, row.pair, row.base, row.quote, active);
    } catch (err) {
      console.error('❌ Error inserting contract:', row, err.message);
    }
  });
  console.log('📄 Imported contracts');
}

async function importDatasourceContractMap() {
  const stmt = db.prepare(`
    INSERT INTO datasource_contract_map
    (datasource_name, contract_address, contract_pair_id, datasource_pair_id, base, quote)
    VALUES (?, ?, ?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/datasource_contract_map.csv'), (row) => {
    try {
      stmt.run(
        row.datasource_name.toLowerCase().trim(),
        row.contract_address,
        row.contract_pair_id,
        row.datasource_pair_id || null,
        row.base,
        row.quote
      );
    } catch (err) {
      console.error('❌ Error inserting datasource_contract_map:', row, err.message);
    }
  });
  console.log('🗺️  Imported datasource_contract_map');
}

async function importMnRpc() {
  const stmt = db.prepare(`
    INSERT INTO mn_rpc (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/mn_rpc.csv'), (row) => {
    try {
      const isPublic = row.public?.toString().trim() === '1' ? 1 : 0;
      const discord_id = row.discord_id?.toLowerCase() === 'null' || row.discord_id === ''
        ? null
        : row.discord_id.trim();
      stmt.run(row.mn, row.name, discord_id, isPublic);
    } catch (err) {
      console.error('❌ Error inserting mn_rpc:', row, err.message);
    }
  });
  console.log('🌐 Imported mn_rpc');
}

async function importMnWss() {
  const stmt = db.prepare(`
    INSERT INTO mn_wss (mn, name, discord_id, public)
    VALUES (?, ?, ?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/mn_wss.csv'), (row) => {
    try {
      const isPublic = row.public?.toString().trim() === '1' ? 1 : 0;
      const discord_id = row.discord_id?.toLowerCase() === 'null' || row.discord_id === ''
        ? null
        : row.discord_id.trim();
      stmt.run(row.mn, row.name, discord_id, isPublic);
    } catch (err) {
      console.error('❌ Error inserting mn_wss:', row, err.message);
    }
  });
  console.log('📡 Imported mn_wss');
}

async function importValidatorContracts() {
  const stmt = db.prepare(`
    INSERT INTO validator_contracts (validator_address, contract_address)
    VALUES (?, ?);
  `);
  await loadCSV(path.join(__dirname, '../data/validator_contracts.csv'), (row) => {
    try {
      stmt.run(row.validator_address, row.contract_address);
    } catch (err) {
      console.error('❌ Error inserting validator_contracts:', row, err.message);
    }
  });
  console.log('📎 Imported validator_contracts');
}

async function main() {
  try {
    console.log('\n🚀 Starting data import...\n');

    db.pragma('foreign_keys = ON');

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
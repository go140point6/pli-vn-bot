const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, '../data/validators.db');
const db = new Database(dbPath, { fileMustExist: true });

const tables = [
  'users',
  'validators',
  'contracts',
  'validator_contracts',
  'mn_rpc',
  'mn_wss',
  'datasources',
  'datasource_price_snapshots',
  'oracle_price_snapshots',
  'datasource_contract_map'
];

console.log('🔍 Reading table contents from database...\n');

for (const table of tables) {
  try {
    const rows = db.prepare(`SELECT * FROM ${table} LIMIT 100`).all();
    console.log(`📄 Table: ${table}`);
    if (rows.length === 0) {
      console.log('⚠️  No data found.');
    } else {
      console.table(rows);
    }
  } catch (err) {
    console.error(`❌ Error reading table ${table}:`, err.message);
  }
}

console.log('\n✅ Done printing all tables.');
console.log('🔒 Database connection closed.');
db.close();
const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), { fileMustExist: true });

try {
  db.prepare(`DROP TABLE IF EXISTS datasource_contract_map`).run();
  console.log('🗑️  Dropped table: datasource_contract_map');
} catch (err) {
  console.error('❌ Failed to drop table:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}
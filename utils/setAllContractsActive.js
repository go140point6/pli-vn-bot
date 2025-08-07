const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, '../data/validators.db');
const db = new Database(dbPath, { fileMustExist: true });

console.log('⚙️  Setting all contracts to active (1)...');

try {
  db.pragma('foreign_keys = ON');

  const stmt = db.prepare(`UPDATE contracts SET active = 1`);
  const result = stmt.run();

  console.log(`✅ Updated ${result.changes} contract(s) to active = 1`);
} catch (err) {
  console.error('❌ Failed to update contracts:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.\n');
}
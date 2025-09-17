const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

// 🛠️ Paste your CREATE TABLE statement here
const createTableSQL = `
  CREATE TABLE IF NOT EXISTS datasource_apis (
    datasource_name TEXT PRIMARY KEY,
    base_url TEXT NOT NULL,
    response_path TEXT NOT NULL,
    FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name)
  );
`;

// 🪄 Extract the table name automatically from your SQL
const match = createTableSQL.match(/CREATE TABLE IF NOT EXISTS\s+(\w+)/i);
const tableName = match ? match[1] : null;

if (!tableName) {
  console.error('❌ Could not detect table name in CREATE TABLE statement.');
  db.close();
  process.exit(1);
}

try {
  // 🔎 Check if the table already exists
  const exists = db.prepare(`
    SELECT name FROM sqlite_master WHERE type='table' AND name = ?;
  `).get(tableName);

  if (exists) {
    console.warn(`⚠️ Table '${tableName}' already exists. CREATE TABLE was skipped.`);
  } else {
    db.exec(createTableSQL);
    console.log(`🛠️  Created table: ${tableName}`);
  }

} catch (err) {
  console.error('❌ Failed to create table:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}
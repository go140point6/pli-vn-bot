const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

// 🛠️ Define the table
const tableName = 'datasource_apis';

try {
  // 🔎 Check if the column already exists
  //const columnName = 'headers';
  //const columnDefinition = 'TEXT DEFAULT NULL'; // Can also include DEFAULT or NOT NULL as needed

  //const columnExists = db
  //  .prepare(`PRAGMA table_info(${tableName})`)
  //  .all()
  //  .some(col => col.name === columnName);

  //if (columnExists) {
  //  console.warn(`⚠️ Column '${columnName}' already exists in table '${tableName}'. ALTER TABLE was skipped.`);
  //} else {
    // ➕ Add the column
    //db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDefinition}`);
    //console.log(`🛠️  Added column '${columnName}' to table '${tableName}'`);
  //}

    // Update base_url
    const newBaseUrl = 'https://api-cloud.bitmart.com/spot/quotation/v3/ticker?symbol=${pair_id}';
    const newResponsePath = 'data.last'

    // const stmt = db.prepare(`
    //   UPDATE datasource_apis
    //   SET base_url = ?
    //   WHERE datasource_name = 'bitmart'
    // `);

    const stmt = db.prepare(`
      UPDATE datasource_apis
      SET response_path = ?
      WHERE datasource_name = 'bitmart'
    `);
    
    //const result = stmt.run(newBaseUrl)
    const result = stmt.run(newResponsePath)

    if (result.changes > 0) {
        console.log(`✅ Updated base_url for Bitmart to: ${newBaseUrl}`);
    } else {
        console.warn('⚠️ No rows updated. Did you already update it, or is Bitmart missing from datasource_apis?');
    }
  //}

    // 🔍 Show current Bitmart row for verification
    const row = db.prepare(`
      SELECT datasource_name, base_url, response_path, COALESCE(headers, 'NULL') as headers
      FROM ${tableName}
      WHERE datasource_name = 'bitmart'
    `).get();

    if (row) {
      console.log('📄 Current Bitmart config:', row);
    } else {
      console.warn('⚠️ Bitmart row not found in datasource_apis.');
    }

} catch (err) {
  console.error(`❌ Failed to alter table '${tableName}':`, err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}

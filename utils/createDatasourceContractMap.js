const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), { fileMustExist: true });

try {
  db.exec(`
    CREATE TABLE IF NOT EXISTS datasource_contract_map (
      datasource_name TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      contract_pair_id TEXT NOT NULL,
      datasource_pair_id TEXT,
      base TEXT NOT NULL,
      quote TEXT NOT NULL,
      PRIMARY KEY (datasource_name, contract_address),
      UNIQUE (datasource_name, datasource_pair_id),
      FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name),
      FOREIGN KEY (contract_address) REFERENCES contracts(address)
    );
  `);
  console.log('🛠️  Created table: datasource_contract_map');
} catch (err) {
  console.error('❌ Failed to create table:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}
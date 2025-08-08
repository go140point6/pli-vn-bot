const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

const result = db.prepare(`
  UPDATE datasource_contract_map
  SET datasource_pair_id = NULL
  WHERE datasource_pair_id = 'NULL'
`).run();

console.log(`✅ Converted ${result.changes} 'NULL' strings to true NULLs.`);
db.close();
const Database = require('better-sqlite3');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), { fileMustExist: true });

function loadCSV(filePath, handleRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', handleRow)
      .on('end', resolve)
      .on('error', reject);
  });
}

async function importDatasourceContractMap() {
  const stmt = db.prepare(`
    INSERT INTO datasource_contract_map
    (datasource_name, contract_address, contract_pair_id, datasource_pair_id, base, quote)
    VALUES (?, ?, ?, ?, ?, ?);
  `);

  const file = path.join(__dirname, '../data/datasource_contract_map.csv');
  let total = 0;
  let failed = 0;

  await loadCSV(file, (row) => {
    total++;
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
      failed++;
      console.error(`❌ Row ${total} failed:`, row, err.message);
    }
  });

  console.log(`\n✅ Imported ${total - failed} of ${total} rows into datasource_contract_map`);
  db.close();
  console.log('🔒 Database connection closed.');
}

importDatasourceContractMap();
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Database = require('better-sqlite3');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

const validContracts = new Set(
  db.prepare('SELECT address FROM contracts').all().map(r => r.address.toLowerCase())
);

const validDatasources = new Set(
  db.prepare('SELECT datasource_name FROM datasources').all().map(r => r.datasource_name.toLowerCase())
);

const existingPrimaryKeys = new Set(
  db.prepare('SELECT datasource_name, contract_address FROM datasource_contract_map').all()
    .map(r => `${r.datasource_name.toLowerCase()}|${r.contract_address.toLowerCase()}`)
);

const existingPairIds = new Set(
  db.prepare('SELECT datasource_name, datasource_pair_id FROM datasource_contract_map')
    .all()
    .filter(r => r.datasource_pair_id !== null)
    .map(r => `${r.datasource_name.toLowerCase()}|${r.datasource_pair_id.toLowerCase()}`)
);

let total = 0;
let valid = 0;
let failed = 0;

fs.createReadStream(path.join(__dirname, '../data/datasource_contract_map.csv'))
  .pipe(csv())
  .on('data', (row) => {
    total++;

    const datasource = (row.datasource_name || '').toLowerCase().trim();
    const contract = (row.contract_address || '').toLowerCase().trim();
    const pairId = (row.datasource_pair_id || '').toLowerCase().trim();
    const primaryKey = `${datasource}|${contract}`;
    const pairKey = pairId ? `${datasource}|${pairId}` : null;

    const reasons = [];

    if (!validDatasources.has(datasource)) {
      reasons.push('❌ Invalid datasource_name (FK violation)');
    }

    if (!validContracts.has(contract)) {
      reasons.push('❌ Invalid contract_address (FK violation)');
    }

    if (existingPrimaryKeys.has(primaryKey)) {
      reasons.push('❌ Duplicate primary key (datasource + contract_address)');
    }

    if (pairKey && existingPairIds.has(pairKey)) {
      reasons.push('❌ Duplicate datasource_pair_id for this datasource');
    }

    if (reasons.length > 0) {
      failed++;
      console.warn(`Row ${total} failed:\n  - ${row.datasource_name}, ${row.contract_address}, ${row.datasource_pair_id}\n  - ${reasons.join('\n  - ')}`);
    } else {
      valid++;
    }
  })
  .on('end', () => {
    console.log('\n📊 Validation complete:');
    console.log(`✅ Valid rows:   ${valid}`);
    console.log(`❌ Skipped rows: ${failed}`);
    console.log(`📄 Total rows:   ${total}`);
    db.close();
  });

const path = require('path');
const Database = require('better-sqlite3');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

try {
  const result = db.prepare(`
    UPDATE datasource_contract_map
    SET datasource_pair_id = 'BTCUSDT'
    WHERE datasource_name = 'bitrue'
      AND datasource_pair_id = 'BTC USDT'
  `).run();

  if (result.changes > 0) {
    console.log(`✅ Updated ${result.changes} row(s) from 'BTC USDT' to 'BTCUSDT'`);
  } else {
    console.warn('⚠️ No rows found with "BTC USDT" for bitrue — nothing was updated.');
  }
} catch (err) {
  console.error('❌ Error updating Bitrue BTCUSDT:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}

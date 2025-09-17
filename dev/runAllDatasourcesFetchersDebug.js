const path = require('path');
const readline = require('readline');
const Database = require('better-sqlite3');
const {
  fetchCMCPrices,
  fetchCoinGeckoPrices,
  fetchBitmartPrices,
  fetchBitruePrices,
  fetchCryptoComparePrices,
  fetchLBankPrices
} = require('../main/fetchDatasourcePrices');

const db = new Database(path.join(__dirname, '../data/validators.db'), { fileMustExist: true });

async function promptToContinue(message = 'Press Enter to continue...') {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise(resolve => rl.question(`⏸️  ${message}`, () => {
    rl.close();
    resolve();
  }));
}

async function runAllDatasourceFetchersDebug() {
  // 🧹 Clear previous snapshot data
  try {
    const deleted = db.prepare('DELETE FROM datasource_price_snapshots').run();
    console.log(`🧼 Cleared ${deleted.changes} rows from datasource_price_snapshots\n`);
  } catch (err) {
    console.error('❌ Failed to clear datasource_price_snapshots:', err.message);
  }

  const datasources = [
    { name: 'coinmarketcap', fn: fetchCMCPrices },
    { name: 'coingecko', fn: fetchCoinGeckoPrices },
    { name: 'bitmart', fn: fetchBitmartPrices },
    { name: 'bitrue', fn: fetchBitruePrices },
    { name: 'cryptocompare', fn: fetchCryptoComparePrices },
    { name: 'lbank', fn: fetchLBankPrices }
  ];

  for (const { name, fn } of datasources) {
    console.log(`\n🚀 Starting fetch for ${name.toUpperCase()}...`);
    await fn(null);  // 'client' not needed
    console.log(`✅ Finished fetch for ${name.toUpperCase()}\n`);
    await promptToContinue();
  }

  // 📊 Output the results of the snapshot table
  try {
    const rows = db.prepare('SELECT * FROM datasource_price_snapshots ORDER BY datasource_name, contract_address').all();
    console.log('\n📈 Final contents of datasource_price_snapshots:\n');
    console.table(rows);
  } catch (err) {
    console.error('❌ Failed to query snapshot table:', err.message);
  }

  console.log('🌀 All datasources fetched. Run complete.\n');
}

runAllDatasourceFetchersDebug();
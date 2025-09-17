// dev/setContractsActive.js
// Sets contracts.active = 1 (optionally for a specific chain_id).
// Uses the unified DB entrypoint to avoid path drift.

const { openDb, dbFile } = require('../db');

// Optional: pass chain via CLI like "--chain=50" or "-c 50", or env CHAIN_ID=50
function parseChainId() {
  const arg = process.argv.find(a => a.startsWith('--chain=')) || null;
  const shortIdx = process.argv.findIndex(a => a === '-c');
  const fromShort = shortIdx !== -1 ? process.argv[shortIdx + 1] : null;
  const fromEnv = process.env.CHAIN_ID;

  const raw = arg ? arg.split('=')[1] : (fromShort ?? fromEnv);
  if (!raw) return null;
  const n = Number(raw);
  return Number.isInteger(n) ? n : null;
}

const chainId = parseChainId();

const db = openDb({ fileMustExist: true });
db.pragma('foreign_keys = ON');

console.log(`⚙️  Setting contracts to active=1 in DB: ${dbFile}${chainId !== null ? ` (chain_id=${chainId})` : ''}`);

try {
  let info;
  if (chainId !== null) {
    const stmt = db.prepare(`UPDATE contracts SET active = 1 WHERE chain_id = ?`);
    info = stmt.run(chainId);
  } else {
    const stmt = db.prepare(`UPDATE contracts SET active = 1`);
    info = stmt.run();
  }

  console.log(`✅ Updated ${info.changes} contract(s) to active = 1`);
} catch (err) {
  console.error('❌ Failed to update contracts:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.\n');
}
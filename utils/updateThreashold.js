const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, '../data/validators.db');
const db = new Database(dbPath, { fileMustExist: true });

const discordId = '12345678901234567';
const newWarning = 200;
const newCritical = 150;

try {
  const result = db.prepare(`
    UPDATE users
    SET warning_threshold = ?, critical_threshold = ?
    WHERE discord_id = ?
  `).run(newWarning, newCritical, discordId);

  if (result.changes === 1) {
    console.log(`✅ Updated thresholds for user ${discordId}`);
  } else {
    console.warn(`⚠️ No user found with discord_id ${discordId}`);
  }
} catch (err) {
  console.error('❌ Error updating thresholds:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}

// checkDB.js
// Quick inspection tool: prints up to N rows per table (prefers newest).
// Uses the unified DB entrypoint to avoid path drift.

const { openDb, dbFile } = require('../db'); // <-- unified import
const LIMIT = parseInt(process.env.CHECKDB_LIMIT || '100', 10);

const db = openDb({ fileMustExist: true }); // don't create if missing
db.pragma('foreign_keys = ON');

console.log('🔍 Reading table contents from database:', dbFile, '\n');

try {
  // 1) Discover user tables (skip SQLite internals)
  const tables = db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name
  `).all().map(r => r.name);

  if (tables.length === 0) {
    console.log('⚠️  No user tables found.');
  }

  // Column preference for "most recent" ordering
  const recencyCols = [
    'id',
    'window_end',
    'ended_at',
    'timestamp',
    'started_at',
    'created_at',
    'updated_at'
  ];

  for (const table of tables) {
    try {
      // 2) Inspect columns to decide ordering
      const columns = db.prepare(`PRAGMA table_info("${table}")`).all();
      const colNames = columns.map(c => c.name);

      // Pick the first preferred column that exists (in order)
      const orderCol = recencyCols.find(c => colNames.includes(c)) || null;

      // Count total rows for context
      const { cnt } = db.prepare(`SELECT COUNT(*) AS cnt FROM "${table}"`).get();

      // 3) Build query
      let query;
      if (orderCol) {
        // Grab newest LIMIT rows by orderCol, then display ascending for readability
        query = `
          SELECT * FROM (
            SELECT * FROM "${table}"
            ORDER BY "${orderCol}" DESC
            LIMIT ${LIMIT}
          ) sub
          ORDER BY "${orderCol}" ASC
        `;
      } else {
        query = `SELECT * FROM "${table}" LIMIT ${LIMIT}`;
      }

      const rows = db.prepare(query).all();

      // 4) Print
      const orderingNote = orderCol
        ? ` (newest ${LIMIT} by "${orderCol}")`
        : ` (first ${LIMIT} rows)`;

      console.log(`\n📄 Table: ${table}${orderingNote} — total rows: ${cnt}`);
      if (rows.length === 0) {
        console.log('⚠️  No data found.');
      } else {
        console.table(rows);
      }
    } catch (err) {
      console.error(`❌ Error reading table ${table}:`, err.message);
    }
  }

  console.log('\n✅ Done printing all tables.');
} catch (err) {
  console.error('❌ Failed to enumerate tables:', err.message);
} finally {
  db.close();
  console.log('🔒 Database connection closed.');
}
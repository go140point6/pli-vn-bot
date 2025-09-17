// db/index.js
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

function resolveDbFile() {
  return process.env.SQLITE_DB_PATH
    ? path.resolve(process.env.SQLITE_DB_PATH)
    : path.resolve(__dirname, '../data/validators.db');
}

function ensureDir(p) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
}

let _db = null;

/**
 * Runtime: get a singleton DB handle with fileMustExist: true
 * (prevents silently creating a blank DB due to bad paths)
 */
function getDb() {
  if (_db) return _db;
  const dbFile = resolveDbFile();
  ensureDir(dbFile);
  _db = new Database(dbFile, { fileMustExist: true });
  _db.pragma('foreign_keys = ON');
  return _db;
}

/**
 * Dev scripts (schema/migrations): open an explicit handle.
 * Pass { fileMustExist: false } to allow creating the DB.
 */
function openDb(opts = {}) {
  const { fileMustExist = true } = opts;
  const dbFile = resolveDbFile();
  ensureDir(dbFile);
  const db = new Database(dbFile, { fileMustExist });
  db.pragma('foreign_keys = ON');
  return db;
}

// Convenience: a property that returns the singleton (same as getDb()).
Object.defineProperty(module.exports, 'db', {
  enumerable: true,
  get: () => getDb(),
});

module.exports.getDb = getDb;
module.exports.openDb = openDb;
module.exports.dbFile = resolveDbFile();
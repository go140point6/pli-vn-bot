// src/utils/paths.js
const fs = require('fs');
const path = require('path');

function findProjectRoot(startDir) {
  let dir = startDir;
  const { root } = path.parse(dir);
  while (true) {
    if (fs.existsSync(path.join(dir, 'package.json'))) return dir;
    const parent = path.dirname(dir);
    if (parent === dir || parent === root) break;
    dir = parent;
  }
  // Fallback to cwd if no package.json was found (unlikely)
  return process.cwd();
}

const ROOT_DIR = findProjectRoot(__dirname);

// Allow override via env if you ever want to relocate the DB
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT_DIR, 'data');
const DB_FILE = process.env.VALIDATORS_DB || path.join(DATA_DIR, 'validators.db');

module.exports = {
  ROOT_DIR,
  DATA_DIR,
  DB_FILE,
};
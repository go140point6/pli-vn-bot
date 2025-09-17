const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../data/validators.db'), {
  fileMustExist: true
});

function checkRpc(isPublic, userId) {
  try {
    if (isPublic) {
      return db.prepare('SELECT mn FROM mn_rpc WHERE public = 1').all();
    } else {
      return db.prepare('SELECT mn FROM mn_rpc WHERE public = 0 AND discord_id = ?').all(userId);
    }
  } catch (err) {
    console.error('❌ DB error in checkRpc:', err.message);
    return [];
  }
}

function checkWss(isPublic, userId) {
  try {
    if (isPublic) {
      return db.prepare('SELECT mn FROM mn_wss WHERE public = 1').all();
    } else {
      return db.prepare('SELECT mn FROM mn_wss WHERE public = 0 AND discord_id = ?').all(userId);
    }
  } catch (err) {
    console.error('❌ DB error in checkWss:', err.message);
    return [];
  }
}

module.exports = {
  checkRpc,
  checkWss
};
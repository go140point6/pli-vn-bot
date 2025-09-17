// utils/dmPolicy.js
const { getDb } = require('../db');
const db = getDb();

// Local, simple check — avoids touching your shared statements module
const selIsAdmin = db.prepare('SELECT 1 FROM users WHERE discord_id = ? AND is_admin = 1');

const { setAcceptsDM } = require('../db/statements');

/**
 * Returns true if the user is an admin.
 */
function isAdmin(discord_id) {
  return !!selIsAdmin.get(discord_id);
}

/**
 * Disable DMs ONLY if the user is NOT an admin.
 * Returns:
 *  - true  if we disabled DMs (non-admin)
 *  - false if we did not (admin or DB error)
 */
function disableDMIfNonAdmin(discord_id) {
  if (isAdmin(discord_id)) return false;
  try {
    setAcceptsDM.run(discord_id); // sets accepts_dm -> 0
    return true;
  } catch (e) {
    console.error('❌ Failed to update accepts_dm:', e.message);
    return false;
  }
}

module.exports = { isAdmin, disableDMIfNonAdmin };

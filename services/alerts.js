// services/alerts.js
require('dotenv').config();
const { getDb } = require('../db');
const db = getDb();

const {
  selAdmins, setAcceptsDM,
  selOpenAlert, insAlert, resolveAlertById,
} = require('../db/statements');

const { addToDigest, buildAdminDigestMessage } = require('./digest');
const { fmtPair } = require('../datasources/common');

// Local helper to list owner labels for a validator
const selOwnerLabels = db.prepare(`
  SELECT COALESCE(u.discord_name, vo.discord_id) AS label
  FROM validator_owners vo
  LEFT JOIN users u ON u.discord_id = vo.discord_id
  WHERE vo.chain_id = ? AND vo.validator_address = ?
`);

// --- Unowned label helpers (env-based) ---
function getUnownedLabelDefault() {
  return (process.env.UNOWNED_LABEL_DEFAULT || 'unowned').trim();
}
function parseUnownedLabelMap() {
  const raw = process.env.UNOWNED_LABEL_MAP;
  if (!raw) return new Map();
  const m = new Map();
  for (const part of String(raw).split(';')) {
    const [k, v] = part.split('=');
    if (!k || !v) continue;
    const key = k.trim().toLowerCase();
    const val = v.trim();
    if (key && val) m.set(key, val);
  }
  return m;
}
const UNOWNED_LABEL_MAP = parseUnownedLabelMap();
function unownedLabel(chain_id, address) {
  const key = `${chain_id}:${String(address).toLowerCase()}`;
  return UNOWNED_LABEL_MAP.get(key) || getUnownedLabelDefault();
}
function listOwnerLabels(chain_id, validator_address) {
  const rows = selOwnerLabels.all(chain_id, validator_address);
  if (!rows || rows.length === 0) return null;
  return rows.map(r => r.label).join(', ');
}

// -------------------------
// Datasource error helpers
// -------------------------

// Open one alert per admin if not already open; add to run digest if newly-opened
function raiseDsErrorForAdmins({ datasource, row, message, detail, digest }) {
  const ds = (datasource ?? '').toString();
  const admins = selAdmins.all();
  console.error(`❌ [${ds || 'unknown'}] ${fmtPair(row || {})} | ${message}`);
  if (admins.length === 0) return;

  const alertType = `DS_FETCH_ERROR:${(ds.toLowerCase ? ds.toLowerCase() : 'unknown')}`;
  const extra = JSON.stringify({
    datasource: ds || null,
    pair_id: row?.datasource_pair_id ?? null,
    base: row?.base ?? null,
    quote: row?.quote ?? null,
    detail: detail ?? null
  });
  const msg = `Datasource ${ds || 'unknown'} fetch/parse error for ${fmtPair(row || {})} on contract ${row?.contract_address || '(unknown)'}.`;

  for (const { discord_id, discord_name } of admins) {
    const open = selOpenAlert.get(discord_id, row?.chain_id ?? null, row?.contract_address ?? null, alertType);
    if (!open) {
      insAlert.run(discord_id, row?.chain_id ?? null, row?.contract_address ?? null, alertType, 'warning', msg, extra);
      if (digest) {
        addToDigest(digest, {
          adminId: discord_id,
          adminName: discord_name,
          alertType,
          pair: fmtPair(row || {}),
          contract: row?.contract_address ?? null,
          chainId: row?.chain_id ?? null,
        });
      }
    }
  }
}

// Resolve all open admin alerts for this pair/source
function resolveDsErrorForAdmins({ datasource, row }) {
  const ds = (datasource ?? '').toString();
  const admins = selAdmins.all();
  if (admins.length === 0) return;
  const alertType = `DS_FETCH_ERROR:${(ds.toLowerCase ? ds.toLowerCase() : 'unknown')}`;
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, row?.chain_id ?? null, row?.contract_address ?? null, alertType);
    if (open) resolveAlertById.run(open.id);
  }
}

// Send one summary DM per admin for this run’s newly-opened errors
async function sendAdminDigests(client, digest) {
  if (!client || !digest || digest.size === 0) return;

  const admins = selAdmins.all().reduce((acc, a) => {
    acc[a.discord_id] = a;
    return acc;
  }, {});

  for (const [adminId, { adminName, items }] of digest) {
    if (items.size === 0) continue;
    const row = admins[adminId];
    const acceptsDm = row ? row.accepts_dm : 0;

    const dmText = buildAdminDigestMessage(items);
    const who = adminName ? `${adminName} (${adminId})` : adminId;

    if (acceptsDm === 1) {
      try {
        const userObj = await client.users.fetch(adminId);
        await userObj.send(dmText);
        console.log(`📣 Sent datasource error digest to admin ${who}`);
      } catch (dmError) {
        console.warn(`❌ Could not DM admin ${who}: ${dmError.message}`);
        try { setAcceptsDM.run(adminId); console.log(`🔧 accepts_dm -> 0 for ${adminId}`); }
        catch (dbErr) { console.error(`❌ Failed to update accepts_dm for ${adminId}:`, dbErr.message); }
      }
    } else {
      console.log(`📣 Digest not DM’d (DMs disabled) for admin ${who}`);
    }
  }
}

// -------------------------------------------
// Admin FYI for validator balance alerts (used by checkBalances.js)
// -------------------------------------------

async function notifyAdminsOnAlertOpened() { return; }   // batched in checkBalances
async function notifyAdminsOnAlertResolved() { return; } // batched in checkBalances

module.exports = {
  raiseDsErrorForAdmins,
  resolveDsErrorForAdmins,
  sendAdminDigests,
  notifyAdminsOnAlertOpened,
  notifyAdminsOnAlertResolved,
};

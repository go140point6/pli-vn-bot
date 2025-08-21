// services/alerts.js
const {
  selAdmins, setAcceptsDM,
  selOpenAlert, insAlert, resolveAlertById,
} = require('../db/statements');
const { addToDigest, buildAdminDigestMessage } = require('./digest');
const { fmtPair } = require('../datasources/common');

// Open one alert per admin if not already open; add to run digest if newly-opened
function raiseDsErrorForAdmins({ datasource, row, message, detail, digest }) {
  const admins = selAdmins.all();
  console.error(`❌ [${datasource}] ${fmtPair(row)} | ${message}`);
  if (admins.length === 0) return;

  const alertType = `DS_FETCH_ERROR:${datasource.toLowerCase()}`;
  const extra = JSON.stringify({
    datasource,
    pair_id: row.datasource_pair_id || null,
    base: row.base || null,
    quote: row.quote || null,
    detail: detail ?? null
  });
  const msg = `Datasource ${datasource} fetch/parse error for ${fmtPair(row)} on contract ${row.contract_address}.`;

  for (const { discord_id, discord_name } of admins) {
    const open = selOpenAlert.get(discord_id, row.chain_id, row.contract_address, alertType);
    if (!open) {
      insAlert.run(discord_id, row.chain_id, row.contract_address, alertType, 'warning', msg, extra);
      // Newly opened during this run → add to digest
      if (digest) {
        addToDigest(digest, {
          adminId: discord_id,
          adminName: discord_name,
          alertType,
          pair: fmtPair(row),
          contract: row.contract_address,
          chainId: row.chain_id,
        });
      }
    }
  }
}

// Resolve all open admin alerts for this pair/source
function resolveDsErrorForAdmins({ datasource, row }) {
  const admins = selAdmins.all();
  if (admins.length === 0) return;
  const alertType = `DS_FETCH_ERROR:${datasource.toLowerCase()}`;
  for (const { discord_id } of admins) {
    const open = selOpenAlert.get(discord_id, row.chain_id, row.contract_address, alertType);
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

module.exports = {
  raiseDsErrorForAdmins,
  resolveDsErrorForAdmins,
  sendAdminDigests,
};
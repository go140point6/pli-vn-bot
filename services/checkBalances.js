// checkBalances.js
const { getAddressBalance } = require('./getBalance'); // (address[, rpcUrl])
const { DB_FILE } = require('../utils/paths');
const Database = require('better-sqlite3');

const db = new Database(DB_FILE, { fileMustExist: true });
db.pragma('foreign_keys = ON');

// --- map chain_id -> RPCURL_<id> from env
function rpcUrlForChain(chain_id) {
  const key = `RPCURL_${chain_id}`;
  return process.env[key];
}

// ========== Prepared statements ==========
const selValidators = db.prepare(`
  SELECT chain_id, address, address_eip55, addr_format, discord_id
  FROM validators
`);

const selUser = db.prepare(`
  SELECT discord_name, warning_threshold, critical_threshold, accepts_dm
  FROM users
  WHERE discord_id = ?
`);

const selOpenAlert = db.prepare(`
  SELECT id
  FROM alerts
  WHERE discord_id = ? AND chain_id = ? AND validator_address = ? AND alert_type = ? AND resolved_at IS NULL
  LIMIT 1
`);

const insAlert = db.prepare(`
  INSERT INTO alerts (discord_id, chain_id, validator_address, alert_type, severity, message, extra)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const resolveAlertById = db.prepare(`
  UPDATE alerts SET resolved_at = CURRENT_TIMESTAMP WHERE id = ?
`);

const setAcceptsDM = db.prepare(`UPDATE users SET accepts_dm = 0 WHERE discord_id = ?`);

// ========== Helpers ==========
function displayAddress({ chain_id, address, address_eip55, addr_format }) {
  const body = (address_eip55 && address_eip55.startsWith('0x'))
    ? address_eip55.slice(2)
    : address.slice(2);
  const wantXdc = chain_id === 50 && addr_format === 'xdc';
  return wantXdc ? `xdc${body}` : `0x${body}`;
}

// Open/resolve alerts atomically for a single validator
const applyAlertStateTx = db.transaction(({ discord_id, chain_id, address, state, msgWarn, msgCrit, extra }) => {
  const openWarn = selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_WARNING');
  const openCrit = selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_CRITICAL');

  let opened = null;
  const resolvedTypes = [];

  if (state === 'critical') {
    if (!openCrit) {
      insAlert.run(discord_id, chain_id, address, 'BALANCE_CRITICAL', 'critical', msgCrit, extra);
      opened = 'critical';
    }
    if (openWarn) {
      resolveAlertById.run(openWarn.id);
      resolvedTypes.push('BALANCE_WARNING');
    }
  } else if (state === 'warning') {
    if (openCrit) {
      resolveAlertById.run(openCrit.id);
      resolvedTypes.push('BALANCE_CRITICAL');
    }
    if (!openWarn) {
      insAlert.run(discord_id, chain_id, address, 'BALANCE_WARNING', 'warning', msgWarn, extra);
      opened = 'warning';
    }
  } else { // ok
    if (openCrit) {
      resolveAlertById.run(openCrit.id);
      resolvedTypes.push('BALANCE_CRITICAL');
    }
    if (openWarn) {
      resolveAlertById.run(openWarn.id);
      resolvedTypes.push('BALANCE_WARNING');
    }
  }

  return { opened, resolvedTypes };
});

async function checkBalances(client) {
  const validators = selValidators.all();

  for (const v of validators) {
    const { chain_id, address, address_eip55, addr_format, discord_id } = v;
    if (!address) continue;

    try {
      // NEW: pick RPC by chain (e.g., RPCURL_50). If missing, skip safely.
      const rpcUrl = rpcUrlForChain(chain_id);
      if (!rpcUrl) {
        console.warn(`⚠️ No RPCURL_${chain_id} set; skipping ${address} (user ${discord_id})`);
        continue;
      }

      // NEW: pass rpcUrl (NOT chain_id) into getAddressBalance
      const bal = await getAddressBalance(address, rpcUrl);

      const numericBalance = parseFloat(bal);
      if (!Number.isFinite(numericBalance)) {
        console.warn(`⚠️ Non-numeric balance for ${address} (user ${discord_id}):`, bal);
        continue;
      }

      const user = selUser.get(discord_id);
      if (!user) {
        console.warn(`⚠️ No user record for ${discord_id}; skipping alerts.`);
        continue;
      }
      const { discord_name, warning_threshold, critical_threshold, accepts_dm } = user;
      const userLabel = discord_name ? discord_name : discord_id;

      if (warning_threshold == null || critical_threshold == null) {
        console.warn(`⚠️ Thresholds not set for user ${userLabel}; skipping alerts.`);
        continue;
      }

      let state = 'ok';
      if (numericBalance < critical_threshold) state = 'critical';
      else if (numericBalance < warning_threshold) state = 'warning';

      const addrForDisplay = displayAddress({ chain_id, address, address_eip55, addr_format });

      const msgCrit =
        `🚨 **CRITICAL ALERT** 🚨\nYour validator \`${addrForDisplay}\` has a ` +
        `dangerously low balance of **${numericBalance} XDC** (critical < ${critical_threshold}).\n` +
        `Immediate top-up is recommended.`;

      const msgWarn =
        `⚠️ **Low Balance Warning**\nYour validator \`${addrForDisplay}\` has a ` +
        `low balance of **${numericBalance} XDC** (warning < ${warning_threshold}).\n` +
        `Please top up to avoid disruptions.`;

      const msgClear =
        `✅ **All Clear**\nYour validator \`${addrForDisplay}\` balance recovered to **${numericBalance} XDC** ` +
        `(≥ ${warning_threshold}). Previously open balance alerts have been closed.`;

      const extra = JSON.stringify({
        balance: numericBalance,
        warning_threshold,
        critical_threshold,
        checked_at: new Date().toISOString(),
      });

      const { opened, resolvedTypes } = applyAlertStateTx({
        discord_id,
        chain_id,
        address,
        state,
        msgWarn,
        msgCrit,
        extra,
      });

      if (opened && accepts_dm === 1) {
        try {
          const userObj = await client.users.fetch(discord_id);
          await userObj.send(opened === 'critical' ? msgCrit : msgWarn);
          console.log(`🔔 Sent ${opened} alert to ${userLabel} for ${addrForDisplay}`);
        } catch (dmError) {
          console.warn(`❌ Could not DM user ${userLabel}: ${dmError.message}`);
          try { setAcceptsDM.run(discord_id); console.log(`🔧 accepts_dm -> 0 for ${discord_id}`); }
          catch (dbErr) { console.error(`❌ Failed to update accepts_dm:`, dbErr.message); }
        }
      } else if (opened) {
        console.log(`📣 Opened ${opened} alert (DMs disabled) for ${userLabel} @ ${addrForDisplay}`);
      }

      if (state === 'ok' && resolvedTypes.length > 0) {
        if (accepts_dm === 1) {
          try {
            const userObj = await client.users.fetch(discord_id);
            await userObj.send(msgClear);
            console.log(`✅ Sent all-clear to ${userLabel} for ${addrForDisplay}`);
          } catch (dmError) {
            console.warn(`❌ Could not DM user ${userLabel} (all-clear): ${dmError.message}`);
            try { setAcceptsDM.run(discord_id); console.log(`🔧 accepts_dm -> 0 for ${discord_id}`); }
            catch (dbErr) { console.error(`❌ Failed to update accepts_dm:`, dbErr.message); }
          }
        } else {
          console.log(`🟢 All-clear (DMs disabled) for ${userLabel} @ ${addrForDisplay}`);
        }
      }

      if (accepts_dm === 0 && (state === 'critical' || state === 'warning')) {
        console.warn(`❌ Could not DM user ${userLabel}: Cannot send messages to this user`);
      }

      const openedLabel = opened ? opened : '-';
      const resolvedLabel = resolvedTypes.length ? resolvedTypes.join('+') : '-';
      console.log(
        `💰 ${addrForDisplay} | user=${userLabel} | balance=${numericBalance} XDC | state=${state} | opened=${openedLabel} | resolved=${resolvedLabel}`
      );

    } catch (error) {
      console.error(`💥 Error checking balance for ${v.address} (user ${v.discord_id}):`, error);
    }
  }
}

module.exports = { checkBalances };
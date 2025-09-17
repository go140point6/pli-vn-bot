// checkBalances.js
require('dotenv').config();

const { getAddressBalance } = require('./getBalance'); // (address[, rpcUrl])
const { DB_FILE } = require('../utils/paths');
const Database = require('better-sqlite3');
const { escapeDiscord } = require('../utils/discordMarkdown');

const db = new Database(DB_FILE, { fileMustExist: true });
db.pragma('foreign_keys = ON');

// --- map chain_id -> RPCURL_<id> from env
function rpcUrlForChain(chain_id) {
  const key = `RPCURL_${chain_id}`;
  return process.env[key];
}

// ========== Prepared statements ==========
const selValidators = db.prepare(`
  SELECT chain_id, address, address_eip55, addr_format
  FROM validators
`);

const selOwnersForValidator = db.prepare(`
  SELECT vo.discord_id
  FROM validator_owners vo
  WHERE vo.chain_id = ? AND vo.validator_address = ?
`);

const selOwnerLabels = db.prepare(`
  SELECT COALESCE(u.discord_name, vo.discord_id) AS label
  FROM validator_owners vo
  LEFT JOIN users u ON u.discord_id = vo.discord_id
  WHERE vo.chain_id = ? AND vo.validator_address = ?
`);

const selUser = db.prepare(`
  SELECT discord_name, warning_threshold, critical_threshold, accepts_dm
  FROM users
  WHERE discord_id = ?
`);

const selAdmins = db.prepare(`
  SELECT discord_id, discord_name, accepts_dm
  FROM users
  WHERE is_admin = 1
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

function getOwnerLabelList(chain_id, validator_address) {
  const rows = selOwnerLabels.all(chain_id, validator_address);
  if (!rows || rows.length === 0) return null;
  return rows.map(r => r.label).join(', ');
}

// ---------- Unowned label mapping via .env ----------
const UNOWNED_LABEL_DEFAULT = (process.env.UNOWNED_LABEL_DEFAULT || 'unowned').trim();

// Parse UNOWNED_LABEL_MAP like:
// "50:0x8cbe01...=ops-sydney;50:0xf87a63...=ops-eu"
function parseUnownedLabelMap(s) {
  const map = new Map();
  if (!s) return map;
  const entries = String(s).split(';').map(t => t.trim()).filter(Boolean);
  for (const e of entries) {
    const [lhs, label] = e.split('=');
    if (!lhs || !label) continue;
    const [chainStr, addr] = lhs.split(':');
    const chain = Number(chainStr);
    if (!Number.isFinite(chain)) continue;
    const key = `${chain}:${String(addr || '').trim().toLowerCase()}`;
    map.set(key, label.trim());
  }
  return map;
}
const UNOWNED_LABEL_MAP = parseUnownedLabelMap(process.env.UNOWNED_LABEL_MAP);

function labelForUnowned(chain_id, validator_address) {
  const key = `${chain_id}:${String(validator_address || '').toLowerCase()}`;
  return UNOWNED_LABEL_MAP.get(key) || UNOWNED_LABEL_DEFAULT;
}

// ---------- Single env floor pair (applies to ALL) ----------
function getEnvNumber(name, fallback) {
  const v = process.env[name];
  if (v == null || v === '') return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

const MIN_WARNING  = getEnvNumber('MIN_WARNING_XDC', 3);
const MIN_CRITICAL = getEnvNumber('MIN_CRITICAL_XDC', 1);

if (MIN_CRITICAL >= MIN_WARNING) {
  console.warn(`⚠️ MIN_CRITICAL_XDC (${MIN_CRITICAL}) >= MIN_WARNING_XDC (${MIN_WARNING}) — double check your .env`);
}

// ---------- State machine ----------
// - state === 'critical': open/keep CRITICAL; DO NOT open new WARNING; keep existing WARNING open.
// - state === 'warning' : ensure WARNING open; resolve CRITICAL if open.
// - state === 'ok'      : resolve both.
const applyAlertStateTx = db.transaction(({ discord_id, chain_id, address, state, msgWarn, msgCrit, msgClear, extra }) => {
  const openWarn = selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_WARNING');
  const openCrit = selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_CRITICAL');

  let opened = null;
  const resolvedTypes = [];

  if (state === 'critical') {
    if (!openCrit) {
      insAlert.run(discord_id, chain_id, address, 'BALANCE_CRITICAL', 'critical', msgCrit, extra);
      opened = 'critical';
    }
    // keep warning as-is
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

// ---------- Admin summary batching ----------
function newAdminSummaryAcc() {
  // adminId -> {
  //   adminName,
  //   openedWarn: [], openedCrit: [],
  //   escalated: [],   // Warning -> Critical
  //   deescalated: [], // Critical -> Warning
  //   resolvedAll: []  // Any-to-OK (all alerts closed)
  // }
  return new Map();
}

function ensureAdmin(summary, admin) {
  if (!summary.has(admin.discord_id)) {
    summary.set(admin.discord_id, {
      adminName: admin.discord_name,
      openedWarn: [], openedCrit: [],
      escalated: [], deescalated: [], resolvedAll: []
    });
  }
}

function pushAdminOpened(summary, admin, item) {
  ensureAdmin(summary, admin);
  if (item.severity === 'critical') summary.get(admin.discord_id).openedCrit.push(item);
  else summary.get(admin.discord_id).openedWarn.push(item);
}

function pushAdminTransition(summary, admin, kind, item) {
  ensureAdmin(summary, admin);
  if (kind === 'escalated') summary.get(admin.discord_id).escalated.push(item);
  else if (kind === 'deescalated') summary.get(admin.discord_id).deescalated.push(item);
  else if (kind === 'resolvedAll') summary.get(admin.discord_id).resolvedAll.push(item);
}

function fmtBalance8(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 'n/a';
  return x.toFixed(8);
}

function renderAdminSummaryDM({ adminName, openedWarn, openedCrit, escalated, deescalated, resolvedAll }) {
  const lines = [];
  // add a leading blank line so this DM never butts against the previous message
  lines.push('');
  lines.push(`📬 Balance Alerts — Summary`);
  if (adminName) lines.push('');

  const section = (title, arr) => {
    if (!arr || arr.length === 0) return;
    lines.push(`${title}`);
    for (const e of arr) {
      // e: { chain_id, addrForDisplay, ownerLabel, balance }
      lines.push(
        `• Chain ${e.chain_id} — Validator ${e.addrForDisplay} (${escapeDiscord(e.ownerLabel)}) — Balance **${fmtBalance8(e.balance)} XDC**`
      );
    }
    lines.push(''); // spacer between sections
  };

  // Opened sections (unchanged)
  section('Opened - Warning  🟡', openedWarn);
  section('Opened - Critical  🔴', openedCrit);

  // Transitions (no "Transitions" header in DM)
  section('Escalated - Warning  🟡  → Critical  🔴', escalated);
  section('De-escalated - Critical  🔴 → Warning  🟡', deescalated);
  section('Resolved  🟢', resolvedAll);

  if (
    (!openedWarn || openedWarn.length === 0) &&
    (!openedCrit || openedCrit.length === 0) &&
    (!escalated || escalated.length === 0) &&
    (!deescalated || deescalated.length === 0) &&
    (!resolvedAll || resolvedAll.length === 0)
  ) {
    lines.push('No changes.');
    lines.push(''); // spacer after "No changes."
  }

  // ensure a trailing blank line for readability
  lines.push('\u200B');

  return lines.join('\n');
}

async function sendAdminSummaries(client, summaryMap) {
  if (!client || !(summaryMap instanceof Map) || summaryMap.size === 0) return;

  // cache admins to check accepts_dm quickly
  const admins = selAdmins.all().reduce((acc, a) => { acc[a.discord_id] = a; return acc; }, {});

  for (const [adminId, data] of summaryMap.entries()) {
    const row = admins[adminId];
    const accepts = row ? row.accepts_dm : 0;
    const who = data.adminName ? `${data.adminName} (${adminId})` : adminId;

    if (accepts !== 1) {
      console.log(`📣 Admin summary not DM’d (DMs disabled) for ${who}`);
      continue;
    }

    const txt = renderAdminSummaryDM(data);
    try {
      const userObj = await client.users.fetch(adminId);
      await userObj.send(txt);
      console.log(`📣 Sent admin balance summary to ${who}`);
    } catch (dmError) {
      console.warn(`❌ Could not DM admin ${who}: ${dmError.message}`);
      try { setAcceptsDM.run(adminId); console.log(`🔧 accepts_dm -> 0 for ${adminId}`); }
      catch (dbErr) { console.error(`❌ Failed to update accepts_dm for ${adminId}:`, dbErr.message); }
    }
  }
}

// ========== Main ==========
async function checkBalances(client) {
  const admins = selAdmins.all(); // used for summaries
  const adminSummary = newAdminSummaryAcc();

  const validators = selValidators.all();

  for (const v of validators) {
    const { chain_id, address, address_eip55, addr_format } = v;
    if (!address) continue;

    try {
      const rpcUrl = rpcUrlForChain(chain_id);
      if (!rpcUrl) {
        console.warn(`⚠️ No RPCURL_${chain_id} set; skipping ${address}`);
        continue;
      }

      const bal = await getAddressBalance(address, rpcUrl);
      const numericBalance = parseFloat(bal);
      if (!Number.isFinite(numericBalance)) {
        console.warn(`⚠️ Non-numeric balance for ${address}:`, bal);
        continue;
      }

      const addrForDisplay = displayAddress({ chain_id, address, address_eip55, addr_format });

      // Owners can be 0..N
      const ownerIds = selOwnersForValidator.all(chain_id, address).map(r => r.discord_id);
      const ownerLabelList = getOwnerLabelList(chain_id, address);

      if (ownerIds.length === 0) {
        // Unowned: use unified env floors directly
        const warning_threshold  = MIN_WARNING;
        const critical_threshold = MIN_CRITICAL;
        const ownerLabel = labelForUnowned(chain_id, address);

        let state = 'ok';
        if (numericBalance < critical_threshold) state = 'critical';
        else if (numericBalance < warning_threshold) state = 'warning';

        const msgCrit =
          `🚨 **CRITICAL ALERT** 🚨\nUnowned validator \`${addrForDisplay}\` has a ` +
          `dangerously low balance of **${numericBalance} XDC** (critical < ${critical_threshold}).\n` +
          `Immediate top-up is recommended.\n\u200B`;

        const msgWarn =
          `⚠️ **Low Balance Warning**\nUnowned validator \`${addrForDisplay}\` has a ` +
          `low balance of **${numericBalance} XDC** (warning < ${warning_threshold}).\n` +
          `Please top up to avoid disruptions.\n\u200B`;

        const msgClear =
          `✅ **All Clear**\nUnowned validator \`${addrForDisplay}\` balance recovered to **${numericBalance} XDC** ` +
          `(≥ ${warning_threshold}). Previously open balance alerts have been closed.\n\u200B`;

        const extra = JSON.stringify({
          balance: numericBalance,
          warning_threshold,
          critical_threshold,
          checked_at: new Date().toISOString(),
        });

        // Admin alerts + summary entries (one alert row per admin)
        // Track aggregate opened/resolved for logging
        let openedAny = null;           // 'critical' | 'warning' | null (prefer critical)
        const resolvedSet = new Set();  // {'BALANCE_WARNING','BALANCE_CRITICAL'}

        for (const admin of admins) {
          // --- prior state (per-admin, since alerts for unowned are per admin)
          const hadWarnBefore = !!selOpenAlert.get(admin.discord_id, chain_id, address, 'BALANCE_WARNING');
          const hadCritBefore = !!selOpenAlert.get(admin.discord_id, chain_id, address, 'BALANCE_CRITICAL');

          const { opened, resolvedTypes } = applyAlertStateTx({
            discord_id: admin.discord_id,
            chain_id,
            address,
            state,
            msgWarn,
            msgCrit,
            msgClear,
            extra,
          });

          // classify transitions
          const escalated = opened === 'critical' && hadWarnBefore; // W → C
          const deescalated = resolvedTypes.includes('BALANCE_CRITICAL') && state === 'warning'; // C → W

          // ✅ less strict: any-to-OK counts as "Resolved — All Alerts"
          const resolvedAll = state === 'ok' && resolvedTypes.length > 0;

          // Opened buckets (suppress when transition applies)
          if (!escalated) {
            if (opened === 'critical') {
              openedAny = 'critical';
              pushAdminOpened(adminSummary, admin, {
                severity: 'critical',
                chain_id,
                addrForDisplay,
                ownerLabel,
                balance: numericBalance
              });
            } else if (opened === 'warning' && !deescalated) {
              if (openedAny !== 'critical') openedAny = 'warning';
              pushAdminOpened(adminSummary, admin, {
                severity: 'warning',
                chain_id,
                addrForDisplay,
                ownerLabel,
                balance: numericBalance
              });
            }
          }

          // Transitions
          if (escalated) {
            pushAdminTransition(adminSummary, admin, 'escalated', {
              chain_id,
              addrForDisplay,
              ownerLabel,
              balance: numericBalance
            });
          }
          if (deescalated) {
            pushAdminTransition(adminSummary, admin, 'deescalated', {
              chain_id,
              addrForDisplay,
              ownerLabel,
              balance: numericBalance
            });
          }
          if (resolvedAll) {
            pushAdminTransition(adminSummary, admin, 'resolvedAll', {
              chain_id,
              addrForDisplay,
              ownerLabel,
              balance: numericBalance
            });
          }

          if (resolvedTypes.length) {
            for (const t of resolvedTypes) resolvedSet.add(t);
          }
        }

        // Log like owned: include actual opened/resolved aggregate for admins
        const openedLbl = openedAny ? openedAny : '-';
        const resolvedLbl = resolvedSet.size ? [...resolvedSet].join('+') : '-';
        console.log(
          `💰 ${addrForDisplay} | ${ownerLabel} | balance=${numericBalance} XDC | state=${state} | opened=${openedLbl} | resolved=${resolvedLbl}`
        );
        continue;
      }

      // Owned: per-owner thresholds floored by MIN_*; owner DMs; admin summaries also
      for (const discord_id of ownerIds) {
        const user = selUser.get(discord_id);
        if (!user) {
          console.warn(`⚠️ No user record for ${discord_id}; skipping alerts for ${addrForDisplay}.`);
          continue;
        }

        const {
          discord_name,
          warning_threshold: userWarnRaw,
          critical_threshold: userCritRaw,
          accepts_dm
        } = user;

        const warning_threshold =
          Number.isFinite(userWarnRaw) ? Math.max(userWarnRaw, MIN_WARNING) : MIN_WARNING;
        const critical_threshold =
          Number.isFinite(userCritRaw) ? Math.max(userCritRaw, MIN_CRITICAL) : MIN_CRITICAL;

        let state = 'ok';
        if (numericBalance < critical_threshold) state = 'critical';
        else if (numericBalance < warning_threshold) state = 'warning';

        const userLabel = discord_name ? discord_name : discord_id;

        const msgCrit =
          `🚨 **CRITICAL ALERT** 🚨\nYour validator \`${addrForDisplay}\` has a ` +
          `dangerously low balance of **${numericBalance} XDC** (critical < ${critical_threshold}).\n` +
          `Immediate top-up is recommended.\n\u200B`;

        const msgWarn =
          `⚠️ **Low Balance Warning**\nYour validator \`${addrForDisplay}\` has a ` +
          `low balance of **${numericBalance} XDC** (warning < ${warning_threshold}).\n` +
          `Please top up to avoid disruptions.\n\u200B`;

        const msgClear =
          `✅ **All Clear**\nYour validator \`${addrForDisplay}\` balance recovered to **${numericBalance} XDC** ` +
          `(≥ ${warning_threshold}). Previously open balance alerts have been closed.\n\u200B`;

        const extra = JSON.stringify({
          balance: numericBalance,
          warning_threshold,
          critical_threshold,
          checked_at: new Date().toISOString(),
        });

        // --- prior state (per-owner, since owned alerts belong to the owner)
        const hadWarnBefore = !!selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_WARNING');
        const hadCritBefore = !!selOpenAlert.get(discord_id, chain_id, address, 'BALANCE_CRITICAL');

        const { opened, resolvedTypes } = applyAlertStateTx({
          discord_id,
          chain_id,
          address,
          state,
          msgWarn,
          msgCrit,
          msgClear,
          extra,
        });

        // Owner DMs
        if (accepts_dm === 1) {
          try {
            if (opened === 'critical') {
              const userObj = await client.users.fetch(discord_id);
              await userObj.send(msgCrit);
              console.log(`🔔 Sent critical alert to ${userLabel} for ${addrForDisplay}`);
            } else if (opened === 'warning' && state === 'warning') {
              const userObj = await client.users.fetch(discord_id);
              await userObj.send(msgWarn);
              console.log(`🔔 Sent warning alert to ${userLabel} for ${addrForDisplay}`);
            } else if (state === 'ok' && resolvedTypes.length > 0) {
              const userObj = await client.users.fetch(discord_id);
              await userObj.send(msgClear);
              console.log(`✅ Sent all-clear to ${userLabel} for ${addrForDisplay}`);
            }
          } catch (dmError) {
            console.warn(`❌ Could not DM user ${userLabel}: ${dmError.message}`);
            try { setAcceptsDM.run(discord_id); console.log(`🔧 accepts_dm -> 0 for ${discord_id}`); }
            catch (dbErr) { console.error(`❌ Failed to update accepts_dm:`, dbErr.message); }
          }
        } else {
          if (opened === 'critical' || opened === 'warning') {
            console.warn(`❌ Could not DM user ${userLabel}: Cannot send messages to this user`);
          } else if ((state === 'critical' || state === 'warning')) {
            // Not newly opened (alert already existed), but user is still in bad state and can't be DM'd
            console.error(`❌ Could not DM user ${userLabel}: Cannot send messages to this user (ongoing ${state})`);
          }
        }

        // Admin summary (owned validators are only summarized for admins)
        for (const admin of admins) {
          // classify transitions for the owner's state change
          const escalated = opened === 'critical' && hadWarnBefore; // W → C
          const deescalated = resolvedTypes.includes('BALANCE_CRITICAL') && state === 'warning'; // C → W

          // ✅ less strict: any-to-OK counts as "Resolved — All Alerts"
          const resolvedAll = state === 'ok' && resolvedTypes.length > 0;

          // Opened buckets (suppress when transition applies)
          if (!escalated) {
            if (opened === 'critical') {
              pushAdminOpened(adminSummary, admin, {
                severity: 'critical',
                chain_id,
                addrForDisplay,
                ownerLabel: userLabel,
                balance: numericBalance
              });
            } else if (opened === 'warning' && !deescalated) {
              pushAdminOpened(adminSummary, admin, {
                severity: 'warning',
                chain_id,
                addrForDisplay,
                ownerLabel: userLabel,
                balance: numericBalance
              });
            }
          }

          // Transitions
          if (escalated) {
            pushAdminTransition(adminSummary, admin, 'escalated', {
              chain_id,
              addrForDisplay,
              ownerLabel: userLabel,
              balance: numericBalance
            });
          }
          if (deescalated) {
            pushAdminTransition(adminSummary, admin, 'deescalated', {
              chain_id,
              addrForDisplay,
              ownerLabel: userLabel,
              balance: numericBalance
            });
          }
          if (resolvedAll) {
            pushAdminTransition(adminSummary, admin, 'resolvedAll', {
              chain_id,
              addrForDisplay,
              ownerLabel: userLabel,
              balance: numericBalance
            });
          }
        }

        const openedLabel = opened ? opened : '-';
        const resolvedLabel = (resolvedTypes.length ? resolvedTypes.join('+') : '-');
        console.log(
          `💰 ${addrForDisplay} | ${userLabel} | balance=${numericBalance} XDC | state=${state} | opened=${openedLabel} | resolved=${resolvedLabel}`
        );
      }
    } catch (error) {
      console.error(`💥 Error checking balance for ${v.address}:`, error);
    }
  }

  // One summary DM per admin (opened/transitions for this run)
  await sendAdminSummaries(client, adminSummary);
}

module.exports = { checkBalances };

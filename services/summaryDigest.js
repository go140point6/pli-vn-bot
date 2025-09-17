// services/summaryDigest.js
require('dotenv').config();
const { getDb } = require('../db');
const db = getDb();

const { SUMMARY_WINDOW_MINUTES, getWindowBoundsForRun } = require('./rollup');

const selAdmins = db.prepare(`SELECT discord_id, discord_name, accepts_dm FROM users WHERE is_admin = 1`);
const setAcceptsDM = db.prepare(`UPDATE users SET accepts_dm = 0 WHERE discord_id = ?`);

const selLastRunId = db.prepare(`SELECT id FROM ingest_runs ORDER BY id DESC LIMIT 1`);
const selWindowRow = db.prepare(`SELECT * FROM summary_windows WHERE window_start = ?`);
const insWindow = db.prepare(`
  INSERT INTO summary_windows (window_start, window_end, created_by_run_id) VALUES (?, ?, ?)
`);
const markOwnersDone = db.prepare(`UPDATE summary_windows SET owners_done = 1, processed_at = CURRENT_TIMESTAMP WHERE window_start = ?`);
const markAdminsDone = db.prepare(`UPDATE summary_windows SET admins_done = 1, processed_at = CURRENT_TIMESTAMP WHERE window_start = ?`);

const selOwnerOracle = db.prepare(`
  SELECT vo.discord_id AS owner_id, u.discord_name, r.*
  FROM oracle_health_rollup r
  JOIN validator_owners vo ON vo.chain_id = r.chain_id AND vo.validator_address = r.validator_address
  LEFT JOIN users u ON u.discord_id = vo.discord_id
  WHERE r.window_start = ?
`);

const selAdminOracle = db.prepare(`
  SELECT r.* FROM oracle_health_rollup r WHERE r.window_start = ?
`);
const selAdminDatasource = db.prepare(`
  SELECT r.* FROM datasource_health_rollup r WHERE r.window_start = ?
`);

const pairLabelStmt = db.prepare(`
  SELECT COALESCE(c.pair, c.base || '/' || c.quote) AS label
  FROM contracts c
  WHERE c.chain_id = ? AND c.address = ?
`);

function pairLabel(chain_id, contract_address) {
  const r = pairLabelStmt.get(chain_id, contract_address);
  return r?.label ?? contract_address;
}

function classifyColor({ ok_hits, stalled_hits, open_at_end, uptimeYellow, uptimeRed }) {
  const total = (ok_hits || 0) + (stalled_hits || 0);
  if (open_at_end) return 'red';
  if (total === 0) return 'green';
  const uptime = (ok_hits || 0) / total;
  if (uptime < uptimeRed) return 'red';
  if (uptime < uptimeYellow) return 'yellow';
  return (stalled_hits > 0) ? 'yellow' : 'green';
}

function icon(c) { return c === 'red' ? '🔴' : c === 'yellow' ? '🟡' : '🟢'; }

function fmtPct(n){ return Number.isFinite(n) ? `${(n*100).toFixed(1)}%` : 'n/a'; }

async function sendOwnerDMs(client, window_start) {
  const UPTIME_YELLOW = Number(process.env.SUMMARY_UPTIME_YELLOW ?? 0.95);
  const UPTIME_RED    = Number(process.env.SUMMARY_UPTIME_RED    ?? 0.80);
  const ONLY_IF_EVENTS= (process.env.SUMMARY_ONLY_IF_EVENTS ?? '1') === '1';

  const rows = selOwnerOracle.all(window_start);
  if (!rows.length) return true; // nothing to do, but also nothing “to report”

  // Group by owner
  const byOwner = new Map();
  for (const r of rows) {
    if (!byOwner.has(r.owner_id)) byOwner.set(r.owner_id, { name: r.discord_name, items: [] });
    byOwner.get(r.owner_id).items.push(r);
  }

  let sentAny = false;

  for (const [ownerId, bucket] of byOwner) {
    const lines = [];
    lines.push(`🕓 **Oracle Health — last ${SUMMARY_WINDOW_MINUTES/60}h**`);
    lines.push('');

    let interesting = false;

    for (const r of bucket.items) {
      const label = pairLabel(r.chain_id, r.contract_address);
      const total = (r.ok_hits||0) + (r.stalled_hits||0);
      const uptime = total ? (r.ok_hits||0) / total : 1;
      const color = classifyColor({ ok_hits: r.ok_hits, stalled_hits: r.stalled_hits, open_at_end: r.open_at_end, uptimeYellow: UPTIME_YELLOW, uptimeRed: UPTIME_RED });
      if (color !== 'green' || r.stalled_hits > 0) interesting = true;

      lines.push(`${icon(color)} ${label} — uptime ${fmtPct(uptime)} (ok ${r.ok_hits||0}/${total}${r.stalled_hits?`, stalls ${r.stalled_hits}`:''})${r.open_at_end ? ' — **still stalled**' : ''}`);
    }

    if (ONLY_IF_EVENTS && !interesting) continue;

    if (client) {
      try {
        const u = await client.users.fetch(ownerId);
        await u.send(lines.join('\n') + '\n\u200B');
        sentAny = true;
      } catch (e) {
        try { setAcceptsDM.run(ownerId); } catch {}
      }
    }
  }

  return sentAny;
}

async function sendAdminDMs(client, window_start) {
  const UPTIME_YELLOW = Number(process.env.SUMMARY_UPTIME_YELLOW ?? 0.95);
  const UPTIME_RED    = Number(process.env.SUMMARY_UPTIME_RED    ?? 0.80);
  const ONLY_IF_EVENTS= (process.env.SUMMARY_ONLY_IF_EVENTS ?? '1') === '1';

  const admins = selAdmins.all().filter(a => a.accepts_dm === 1);
  if (!admins.length) return false;

  const oracle = selAdminOracle.all(window_start);
  const ds     = selAdminDatasource.all(window_start);

  const hasOracleSignal = oracle.some(r => r.stalled_hits > 0 || r.open_at_end);
  const hasDsSignal     = ds.some(r => r.stalled_hits > 0 || r.outlier_hits > 0 || r.fetch_error_hits > 0 || r.open_at_end);
  if (ONLY_IF_EVENTS && !hasOracleSignal && !hasDsSignal) return false;

  const lines = [];
  lines.push(`🕓 **Network Health — last ${SUMMARY_WINDOW_MINUTES/60}h**`);
  lines.push('');

  if (oracle.length) {
    lines.push(`**Oracle**`);
    for (const r of oracle) {
      const label = pairLabel(r.chain_id, r.contract_address);
      const total = (r.ok_hits||0) + (r.stalled_hits||0);
      const uptime = total ? (r.ok_hits||0) / total : 1;
      const color = classifyColor({ ok_hits: r.ok_hits, stalled_hits: r.stalled_hits, open_at_end: r.open_at_end, uptimeYellow: UPTIME_YELLOW, uptimeRed: UPTIME_RED });
      lines.push(`• ${icon(color)} ${label} @ ${r.validator_address.slice(0,6)}…${r.validator_address.slice(-4)} — uptime ${fmtPct(uptime)} (stalls ${r.stalled_hits||0})${r.open_at_end ? ' — **still stalled**' : ''}`);
    }
    lines.push('');
  }

  if (ds.length) {
    lines.push(`**Datasources**`);
    for (const r of ds) {
      const label = pairLabel(r.chain_id, r.contract_address);
      const total = (r.ok_hits||0) + (r.stalled_hits||0);
      const uptime = total ? (r.ok_hits||0) / total : 1;
      const hasSignal = (r.stalled_hits||0) > 0 || (r.outlier_hits||0) > 0 || (r.fetch_error_hits||0) > 0 || r.open_at_end;
      const color = hasSignal
        ? classifyColor({ ok_hits: r.ok_hits, stalled_hits: r.stalled_hits, open_at_end: r.open_at_end, uptimeYellow: UPTIME_YELLOW, uptimeRed: UPTIME_RED })
        : 'green';
      lines.push(`• ${icon(color)} ${label} [${r.datasource_name}] — uptime ${fmtPct(uptime)}; stalls ${r.stalled_hits||0}, outliers ${r.outlier_hits||0}, fetch errors ${r.fetch_error_hits||0}${r.open_at_end ? ' — **still stalled**' : ''}`);
    }
    lines.push('');
  }

  let sentAny = false;
  for (const a of admins) {
    if (!client) break;
    try {
      const u = await client.users.fetch(a.discord_id);
      await u.send(lines.join('\n') + '\n\u200B');
      sentAny = true;
    } catch (e) {
      try { setAcceptsDM.run(a.discord_id); } catch {}
    }
  }
  return sentAny;
}

async function sendSummaryIfDue(client, runId) {
  if (!Number.isFinite(runId)) return;

  const { window_start_iso, window_end_iso } = getWindowBoundsForRun(runId);
  let row = selWindowRow.get(window_start_iso);
  if (!row) {
    insWindow.run(window_start_iso, window_end_iso, runId);
    row = selWindowRow.get(window_start_iso);
  }

  // Only send once per audience per window, and only after the window has completed
  const now = Date.now();
  if (now < Date.parse(window_end_iso)) return; // window still in progress

  if (!row.owners_done) {
    const ownersSent = await sendOwnerDMs(client, window_start_iso);
    markOwnersDone.run(window_start_iso);
  }
  if (!row.admins_done) {
    const adminsSent = await sendAdminDMs(client, window_start_iso);
    markAdminsDone.run(window_start_iso);
  }
}

module.exports = { sendSummaryIfDue };

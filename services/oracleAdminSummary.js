// services/oracleAdminSummary.js
// Compact, per-validator traffic-light admin summary for ORACLES.
//
// Usage: await sendOracleAdminSummaryIfDue(client)
// (wire this from your existing services/windowSummary.js)
//
// Uses tables:
//   - summary_windows(window_start, window_end, admins_done, processed_at, created_by_run_id)
//   - oracle_health_rollup(window_start, window_end, chain_id, contract_address, validator_address,
//                          ok_hits, stalled_hits, open_at_end, last_dev_pct, last_span_sec, last_median_now, last_price)
//   - validator_owners(chain_id, validator_address, discord_id)
//   - users(discord_id, discord_name)
//   - contracts(chain_id, address, pair, base, quote)
//   - validators(chain_id, address, address_eip55)
//
// Env thresholds (already in your .env / thresholds config):
//   SUMMARY_UPTIME_YELLOW (default 0.95)
//   SUMMARY_UPTIME_RED    (default 0.80)
//   SUMMARY_ONLY_IF_EVENTS (0/1)
//   SUMMARY_WINDOW_MINUTES
//
// Notes:
//   - We classify RED if uptime < red OR open_at_end = 1 (explicit red).
//   - YELLOW if uptime < yellow.
//   - GREEN otherwise.
//   - We show one line per (validator@contract).
//   - Sort order: RED first (worst uptime), then YELLOW, then GREEN.

const { getDb } = require('../db');
const db = getDb();

const { getAggregationConfig, logActiveAggregationConfig } = require('../config/thresholds');
const AGG = getAggregationConfig();
logActiveAggregationConfig(AGG);

const {
  SUMMARY_UPTIME_YELLOW = 0.95,
  SUMMARY_UPTIME_RED = 0.80,
  SUMMARY_ONLY_IF_EVENTS = 0
} = AGG;

// ---------- local statements (kept here so we don't modify global statements) ----------
const selPendingAdminWindow = db.prepare(`
  SELECT window_start, window_end
  FROM summary_windows
  WHERE admins_done = 0
  ORDER BY window_start ASC
  LIMIT 1
`);

const markAdminWindowDone = db.prepare(`
  UPDATE summary_windows
  SET admins_done = 1, processed_at = CURRENT_TIMESTAMP
  WHERE window_start = ? AND window_end = ?
`);

const selOracleRollupForWindow = db.prepare(`
  SELECT
    o.window_start, o.window_end,
    o.chain_id, o.contract_address, o.validator_address,
    o.ok_hits, o.stalled_hits, o.open_at_end,
    -- labels / names
    c.pair, c.base, c.quote,
    v.address_eip55,
    COALESCE(MIN(u.discord_name), '-') AS owner_name
  FROM oracle_health_rollup o
  LEFT JOIN contracts  c ON c.chain_id = o.chain_id AND c.address = o.contract_address
  LEFT JOIN validators v ON v.chain_id = o.chain_id AND v.address = o.validator_address
  LEFT JOIN validator_owners vo ON vo.chain_id = o.chain_id AND vo.validator_address = o.validator_address
  LEFT JOIN users u ON u.discord_id = vo.discord_id
  WHERE o.window_start = ? AND o.window_end = ?
  GROUP BY o.window_start, o.window_end, o.chain_id, o.contract_address, o.validator_address
  ORDER BY o.chain_id, o.contract_address, o.validator_address
`);

const selAdmins = db.prepare(`SELECT discord_id, discord_name FROM users WHERE is_admin = 1`);

function pct(n) {
  if (!Number.isFinite(n)) return 'n/a';
  return `${(n * 100).toFixed(2)}%`;
}

function classifyLight({ uptime, openAtEnd }) {
  // Hard red if open at end of window
  if (openAtEnd || (Number.isFinite(uptime) && uptime < Number(SUMMARY_UPTIME_RED))) return 'red';
  if (Number.isFinite(uptime) && uptime < Number(SUMMARY_UPTIME_YELLOW)) return 'yellow';
  return 'green';
}

function lightEmoji(light) {
  return light === 'red' ? '🔴' : light === 'yellow' ? '🟡' : '🟢';
}

function shortAddr(addr) {
  if (!addr) return '';
  return addr.startsWith('0x') ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : addr;
}

function labelPair({ pair, base, quote }) {
  if (pair && pair.trim()) return pair;
  if (base && quote) return `${base}/${quote}`;
  return '';
}

function formatLine(row) {
  const total = Number(row.ok_hits || 0) + Number(row.stalled_hits || 0);
  const uptime = total > 0 ? Number(row.ok_hits || 0) / total : 1; // no data → treat as 100%
  const light = classifyLight({ uptime, openAtEnd: !!row.open_at_end });
  const emoji = lightEmoji(light);
  const pair = labelPair(row);
  const owner = row.owner_name || '-';
  const vAddr = row.validator_address;
  // Prefer EIP-55 for readability if present
  const vDisp = row.address_eip55 || vAddr;

  // Example: 🟡 0xABCD…1234 @ PLI/USD (owner: samsam) 83.04%
  const parts = [
    `${emoji} ${shortAddr(vDisp)}`,
    pair ? `@ ${pair}` : '',
    owner ? `(owner: ${owner})` : '',
    pct(uptime)
  ].filter(Boolean);

  return {
    light,
    uptime,
    text: parts.join(' ')
  };
}

function sortLines(lines) {
  // RED (worst uptime) first, then YELLOW, then GREEN.
  const colorRank = { red: 0, yellow: 1, green: 2 };
  return [...lines].sort((a, b) => {
    const cr = colorRank[a.light] - colorRank[b.light];
    if (cr !== 0) return cr;
    // inside same color, ascending uptime
    return a.uptime - b.uptime;
  });
}

function buildHeader({ window_start, window_end, adminName }) {
  return [
    `📊 **Health Summary (Admin)**`,
    `Window: ${new Date(window_start).toISOString()} → ${new Date(window_end).toISOString()}`,
    `Admin: ${adminName}`,
    `Legend: 🟢 ≥ ${(Number(SUMMARY_UPTIME_YELLOW) * 100).toFixed(0)}%  |  🟡 < ${(Number(SUMMARY_UPTIME_YELLOW) * 100).toFixed(0)}%  |  🔴 < ${(Number(SUMMARY_UPTIME_RED) * 100).toFixed(0)}% or open at end`,
  ].join('\n');
}

async function sendDM(client, discord_id, content) {
  if (!client) return; // allow running in jobs without Discord client
  try {
    const u = await client.users.fetch(discord_id);
    await u.send(content);
  } catch (e) {
    console.warn(`❌ Could not DM admin ${discord_id}: ${e.message}`);
  }
}

async function sendOracleAdminSummaryForWindow(client, window_start, window_end) {
  const rows = selOracleRollupForWindow.all(window_start, window_end);
  const lines = rows.map(formatLine);
  const ordered = sortLines(lines);

  // If caller wants to suppress “all green” windows:
  if (Number(SUMMARY_ONLY_IF_EVENTS) === 1) {
    const hasNonGreen = ordered.some(l => l.light !== 'green');
    if (!hasNonGreen) {
      // still mark window as done so we don't keep checking
      try { markAdminWindowDone.run(window_start, window_end); } catch {}
      return;
    }
  }

  const admins = selAdmins.all();
  for (const a of admins) {
    const header = buildHeader({ window_start, window_end, adminName: a.discord_name || a.discord_id });
    const body = ordered.length
      ? ordered.map(l => l.text).join('\n')
      : 'All green ✅';
    await sendDM(client, a.discord_id, `${header}\n${body}`);
  }

  try { markAdminWindowDone.run(window_start, window_end); } catch {}
}

async function sendOracleAdminSummaryIfDue(client) {
  const w = selPendingAdminWindow.get();
  if (!w) return; // nothing pending
  await sendOracleAdminSummaryForWindow(client, w.window_start, w.window_end);
}

module.exports = { sendOracleAdminSummaryIfDue };

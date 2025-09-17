// services/windowSummary.js
require('dotenv').config();

const { getDb } = require('../db');
const { sendDM } = require('./dm');

const db = getDb();

/* ---------- global debug (single toggle) ---------- */
// Turn on with DEBUG_ALL=1 (or true/yes/on) in .env
const envFlag = k => ['1','true','yes','on'].includes(String(process.env[k]).toLowerCase());
const DEBUG = envFlag('DEBUG_ALL');
const log = (...args) => { if (DEBUG) console.log(...args); };

/* ---------- env ---------- */
const MINUTES = Number(process.env.SUMMARY_WINDOW_MINUTES || 240); // 4h default
const ONLY_IF_EVENTS = (process.env.SUMMARY_ONLY_IF_EVENTS ?? '1') === '1';
const YELLOW = Number(process.env.SUMMARY_UPTIME_YELLOW || 0.95);
const RED    = Number(process.env.SUMMARY_UPTIME_RED    || 0.80);
const UNOWNED_LABEL_DEFAULT = process.env.UNOWNED_LABEL_DEFAULT || 'unassigned';
const UNOWNED_LABEL_MAP_STR = process.env.UNOWNED_LABEL_MAP || '';
const SKIP_PARTIAL = (process.env.SUMMARY_SKIP_PARTIAL_WINDOWS ?? '0') === '1';
const MIN_EVALS_ORACLE = Number(process.env.SUMMARY_MIN_EVALS_PER_ROW_ORACLE || 2);
const MIN_EVALS_DS     = Number(process.env.SUMMARY_MIN_EVALS_PER_ROW_DS || 2);

// keep messages under Discord’s 2000 cap
const MAX_MSG = 1800;

/* ---------- helpers ---------- */
function short(addr) { return addr ? `${addr.slice(0,6)}…${addr.slice(-4)}` : ''; }
function pct(n, d=0) { if (!Number.isFinite(n)) return 'n/a'; return `${(n*100).toFixed(d)}%`; }
function pad(s, w)   { s = String(s); return s.length >= w ? s : (s + ' '.repeat(w - s.length)); }
function iso(ts)     { return new Date(ts).toISOString().replace('.000Z','Z'); }
function floorToWindow(date, minutes) {
  const ms = minutes * 60_000;
  return new Date(Math.floor(date.getTime() / ms) * ms);
}
function chunkBySize(lines, title) {
  const chunks = [];
  let cur = [];
  const wrap = arr => [title, '```', ...arr, '```'].join('\n');
  for (const ln of lines) {
    const test = wrap([...cur, ln]);
    if (test.length <= MAX_MSG) cur.push(ln);
    else {
      if (cur.length) chunks.push(wrap(cur));
      cur = [ln];
    }
  }
  if (cur.length) chunks.push(wrap(cur));
  return chunks;
}
function classify(uptime, openAtEnd) {
  if (openAtEnd) return '🔴';
  if (!Number.isFinite(uptime)) return '🟡';
  if (uptime < RED)   return '🔴';
  if (uptime < YELLOW) return '🟡';
  return '🟢';
}

// Parse "50:0xabc...=ops-sydney;50:0xdef...=ops-eu" → Map<"chain:addr", label>
function parseUnownedLabelMap(s) {
  const m = new Map();
  if (!s) return m;
  for (const part of s.split(';')) {
    const p = part.trim();
    if (!p) continue;
    const eq = p.indexOf('=');
    if (eq === -1) continue;
    const lhs = p.slice(0, eq).trim();
    const label = p.slice(eq + 1).trim();
    const colon = lhs.indexOf(':');
    if (colon === -1) continue;
    const chainStr = lhs.slice(0, colon).trim();
    const addr = lhs.slice(colon + 1).trim().toLowerCase();
    const chain = Number(chainStr);
    if (!Number.isFinite(chain) || !addr) continue;
    m.set(`${chain}:${addr}`, label);
  }
  return m;
}
const UNOWNED_LABEL_MAP = parseUnownedLabelMap(UNOWNED_LABEL_MAP_STR);

// Prefer explicit owner(s); otherwise map by chain:validator → label; otherwise default.
function resolveOwnerLabel({ chain_id, validator_address, owner_one, owner_cnt }) {
  const cnt = Number(owner_cnt || 0);
  if (cnt > 0 && owner_one) {
    return cnt > 1 ? `${owner_one} (+${cnt - 1})` : owner_one;
  }
  const key = `${Number(chain_id)}:${String(validator_address).toLowerCase()}`;
  return UNOWNED_LABEL_MAP.get(key) || UNOWNED_LABEL_DEFAULT || 'unassigned';
}

function evalsOracleRow(r) { return (Number(r.ok_hits)||0) + (Number(r.stalled_hits)||0); }
function evalsDSRow(r)     { return (Number(r.ok_hits)||0) + (Number(r.stalled_hits)||0) + (Number(r.outlier_hits)||0) + (Number(r.fetch_error_hits)||0); }
function isOracleWindowComplete(rows, minEvals = MIN_EVALS_ORACLE) {
  if (!rows.length) return false;
  let minSeen = Infinity;
  for (const r of rows) {
    const n = evalsOracleRow(r);
    if (n < minSeen) minSeen = n;
    if (minSeen < minEvals) return false;
  }
  return true;
}
function isDSWindowComplete(rows, minEvals = MIN_EVALS_DS) {
  if (!rows.length) return false;
  let minSeen = Infinity;
  for (const r of rows) {
    const n = evalsDSRow(r);
    if (n < minSeen) minSeen = n;
    if (minSeen < minEvals) return false;
  }
  return true;
}

/* ---------- statements (new schema only) ---------- */
const selRun = db.prepare(`SELECT started_at FROM ingest_runs WHERE id = ?`);

const upsertWindow = db.prepare(`
  INSERT OR IGNORE INTO summary_windows
  (window_start, window_end, owners_done, admins_oracle_done, admins_ds_done, created_by_run_id)
  VALUES (?, ?, 0, 0, 0, ?)
`);

const getWindow = db.prepare(`
  SELECT window_start, window_end, owners_done,
         admins_oracle_done, admins_ds_done,
         oracle_actual_start, oracle_actual_end,
         ds_actual_start, ds_actual_end
  FROM summary_windows
  WHERE window_start = ? AND window_end = ?
`);

const markOwnersDone = db.prepare(`
  UPDATE summary_windows
  SET owners_done = 1,
      processed_at = COALESCE(processed_at, CURRENT_TIMESTAMP)
  WHERE window_start = ? AND window_end = ?
`);

const markAdminsOracleDone = db.prepare(`
  UPDATE summary_windows
  SET admins_oracle_done = 1,
      oracle_actual_start = ?,
      oracle_actual_end   = ?,
      processed_at = COALESCE(processed_at, CURRENT_TIMESTAMP)
  WHERE window_start = ? AND window_end = ?
`);

const markAdminsDSDone = db.prepare(`
  UPDATE summary_windows
  SET admins_ds_done = 1,
      ds_actual_start = ?,
      ds_actual_end   = ?,
      processed_at = COALESCE(processed_at, CURRENT_TIMESTAMP)
  WHERE window_start = ? AND window_end = ?
`);

/* Owners: pull oracle rollups for a specific window_end (owner-specific join) */
const selOwnersRows = db.prepare(`
  SELECT
    r.chain_id, r.contract_address, r.validator_address,
    r.ok_hits, r.stalled_hits, r.open_at_end,
    c.pair,
    u.discord_id, u.discord_name, u.accepts_dm
  FROM oracle_health_rollup r
  JOIN validator_owners vo
    ON vo.chain_id = r.chain_id AND vo.validator_address = r.validator_address
  JOIN users u ON u.discord_id = vo.discord_id
  LEFT JOIN contracts c
    ON c.chain_id = r.chain_id AND c.address = r.contract_address
  WHERE r.window_end = ?
`);

/* Admin (oracle): compact list, include one owner name and count */
const selAdminsOracle = db.prepare(`
  SELECT
    r.chain_id, r.contract_address, r.validator_address,
    r.ok_hits, r.stalled_hits, r.open_at_end,
    c.pair,
    MIN(u.discord_name)          AS owner_one,
    COUNT(DISTINCT u.discord_id) AS owner_cnt
  FROM oracle_health_rollup r
  LEFT JOIN contracts c
    ON c.chain_id = r.chain_id AND c.address = r.contract_address
  LEFT JOIN validator_owners vo
    ON vo.chain_id = r.chain_id AND vo.validator_address = r.validator_address
  LEFT JOIN users u
    ON u.discord_id = vo.discord_id
  WHERE r.window_end = ?
  GROUP BY r.chain_id, r.contract_address, r.validator_address,
           r.ok_hits, r.stalled_hits, r.open_at_end, c.pair
`);

const selAdminsDatasource = db.prepare(`
  SELECT
    r.chain_id, r.contract_address, r.datasource_name,
    r.ok_hits, r.stalled_hits, r.outlier_hits, r.fetch_error_hits, r.open_at_end,
    c.pair
  FROM datasource_health_rollup r
  LEFT JOIN contracts c
    ON c.chain_id = r.chain_id AND c.address = r.contract_address
  WHERE r.window_end = ?
`);

const selAdmins = db.prepare(`SELECT discord_id, discord_name, accepts_dm FROM users WHERE is_admin = 1`);

/* Find latest populated windows <= target, and the matching starts */
const selLatestOracleWindowEndLTE = db.prepare(`
  SELECT MAX(window_end) AS we
  FROM oracle_health_rollup
  WHERE window_end <= ?
`);
const selOracleWindowStartForEnd = db.prepare(`
  SELECT window_start AS ws
  FROM oracle_health_rollup
  WHERE window_end = ?
  LIMIT 1
`);
const selLatestDSWindowEndLTE = db.prepare(`
  SELECT MAX(window_end) AS we
  FROM datasource_health_rollup
  WHERE window_end <= ?
`);
const selDSWindowStartForEnd = db.prepare(`
  SELECT window_start AS ws
  FROM datasource_health_rollup
  WHERE window_end = ?
  LIMIT 1
`);

/* ---------- renderers ---------- */
// Owners DM
function renderOwnerSections(rows, windowStart, windowEnd, ownerName) {
  const header = [
    `🧭 Oracle Health Summary`,
    `Window: ${iso(windowStart)} → ${iso(windowEnd)}`,
    ownerName ? `Owner: ${ownerName}` : null,
    `Legend: 🟢 ≥ ${pct(YELLOW,0)}  |  🟡 < ${pct(YELLOW,0)}  |  🔴 < ${pct(RED,0)} or open at end`,
    ''
  ].filter(Boolean).join('\n');

  if (!rows.length) return [header + '\nAll green ✅'];

  const table = [];
  table.push(`pair        validator           uptime  (ok/stalled)  open`);
  for (const r of rows) {
    const total = (Number(r.ok_hits)||0) + (Number(r.stalled_hits)||0);
    const up = total > 0 ? (Number(r.ok_hits)||0)/total : NaN;
    const light = classify(up, !!r.open_at_end);
    const line = [
      pad((r.pair || r.contract_address || '').slice(0,12), 12),
      pad(short(r.validator_address), 18),
      pad(`${light} ${pct(up || 0, 0)}`, 8),
      pad(`(${r.ok_hits||0}/${r.stalled_hits||0})`, 13),
      r.open_at_end ? 'yes' : 'no'
    ].join('  ');
    table.push(line);
  }
  return [header, ...chunkBySize(table, 'Oracle Validators')];
}

/* Admin render (ORACLE): code-fenced hotlist with header row + green rollup line */
function renderAdminOracleCompact(oracleRows, windowStart, windowEnd, adminName) {
  log('[oracle-admin-compact] enter', {
    rows: Array.isArray(oracleRows) ? oracleRows.length : 0,
    windowStart: iso(windowStart),
    windowEnd: iso(windowEnd),
    adminName
  });

  // dedupe (pair,validator)
  const uniq = [];
  const seen = new Set();
  for (const r of oracleRows || []) {
    const k = `${r.chain_id}|${r.contract_address}|${r.validator_address}`;
    if (!seen.has(k)) { seen.add(k); uniq.push(r); }
  }

  // classify
  const pairsSet = new Set(uniq.map(r => `${r.chain_id}|${r.contract_address}`));
  const validatorsSet = new Set(uniq.map(r => r.validator_address));
  const classified = uniq.map(r => {
    const ok = Number(r.ok_hits) || 0;
    const st = Number(r.stalled_hits) || 0;
    const total = ok + st;
    const uptime = total > 0 ? ok / total : NaN;
    const open = !!r.open_at_end;
    let color = 'green';
    if (open) color = 'red';
    else if (!Number.isFinite(uptime)) color = 'yellow';
    else if (uptime < RED) color = 'red';
    else if (uptime < YELLOW) color = 'yellow';
    return { r, ok, st, uptime, open, color };
  });

  const reds    = classified.filter(x => x.color === 'red');
  const yellows = classified.filter(x => x.color === 'yellow');
  const greens  = classified.filter(x => x.color === 'green');
  const redCount = reds.length, yellowCount = yellows.length, greenCount = greens.length;
  const greenPairsSet = new Set(greens.map(x => `${x.r.chain_id}|${x.r.contract_address}`));

  const header = [
    `📊 Health Summary (Admin)`,
    `Window: ${iso(windowStart)} → ${iso(windowEnd)}`,
    adminName ? `Admin: ${adminName}` : null,
    `Legend: 🟢 ≥ ${pct(YELLOW,0)}  |  🟡 < ${pct(YELLOW,0)}  |  🔴 < ${pct(RED,0)} or open at end`,
    '',
    `Summary: pairs ${pairsSet.size} | validators ${validatorsSet.size} | 🔴 ${redCount} | 🟡 ${yellowCount} | 🟢 ${greenCount}`
  ].filter(Boolean).join('\n');

  const greenRollup = `✅ All-green rollup: ${greenCount} validators across ${greenPairsSet.size} pairs.`;

  // hotlist table (non-green only)
  const lines = [];
  lines.push(`pair        validator           owner            uptime  (ok/stalled)  open`);

  const rowsToShow = [...reds, ...yellows].sort((a, b) => {
    const rank = (x) => (x.color === 'red' ? 0 : 1);
    const rr = rank(a) - rank(b);
    if (rr !== 0) return rr;
    const ua = Number.isFinite(a.uptime) ? a.uptime : 1.01;
    const ub = Number.isFinite(b.uptime) ? b.uptime : 1.01;
    return ua - ub;
  });

  log('[oracle-admin-compact] classified', {
    red: redCount, yellow: yellowCount, green: greenCount,
    willShow: rowsToShow.length
  });

  function resolveOwnerLabelLocal(row) {
    const cnt = Number(row.owner_cnt || 0);
    if (cnt > 0 && row.owner_one) return cnt > 1 ? `${row.owner_one} (+${cnt - 1})` : row.owner_one;
    const key = `${Number(row.chain_id)}:${String(row.validator_address).toLowerCase()}`;
    return UNOWNED_LABEL_MAP.get(key) || UNOWNED_LABEL_DEFAULT || 'unassigned';
  }
  const pairLabel = (row) => (row.pair && row.pair.trim()) ? row.pair : short(row.contract_address);

  if (rowsToShow.length === 0) {
    const chunks = chunkBySize(lines, 'Oracle Validators'); // header-only table, inside ```
    const footered = (chunks.length <= 1)
      ? chunks
      : chunks.map((c, i) => `${c}\n\n${i+1}/${chunks.length} ${i+1 < chunks.length ? '(cont.)' : '(end)'}`);
    const first = [header, '', '🔥 Hotlist (non-green only)', '', footered[0]].join('\n');
    log('[oracleMsgs-preview][empty-hotlist]', { firstLen: first.length, hasFence: first.includes('```'), chunkCount: footered.length });
    const rest = footered.slice(1);
    return [first, ...rest, greenRollup];
  }

  for (const { r, ok, st, uptime, open, color } of rowsToShow) {
    const ownerLabel = resolveOwnerLabelLocal(r).slice(0, 14);
    const light = (color === 'red') ? '🔴' : '🟡';
    lines.push([
      pad(pairLabel(r).slice(0,12), 12),
      pad(short(r.validator_address), 18),
      pad(ownerLabel, 16),
      pad(`${light} ${pct(Number.isFinite(uptime) ? uptime : 0, 0)}`, 8),
      pad(`(${ok}/${st})`, 13),
      open ? 'yes' : 'no'
    ].join('  '));
  }

  const chunks = chunkBySize(lines, 'Oracle Validators'); // each chunk wrapped in ```
  const footered = (chunks.length <= 1)
    ? chunks
    : chunks.map((c, i) => `${c}\n\n${i+1}/${chunks.length} ${i+1 < chunks.length ? '(cont.)' : '(end)'}`);

  const firstMsg = [header, '', '🔥 Hotlist (non-green only)', '', footered[0]].join('\n');

  log('[oracleMsgs-preview][non-green]', {
    firstLen: firstMsg.length,
    firstHasFence: firstMsg.includes('```'),
    chunkCount: footered.length,
    chunk0Len: footered[0]?.length ?? 0,
  });

  const rest = footered.slice(1);
  return [firstMsg, ...rest, greenRollup];
}

/* Admin render (DS): code-fenced hotlist with header row */
function renderAdminDatasourceCompact(dsRows, windowStart, windowEnd, adminName) {
  const header = [
    `📊 Datasource Health (Admin)`,
    `Window: ${iso(windowStart)} → ${iso(windowEnd)}`,
    adminName ? `Admin: ${adminName}` : null,
    `Legend: 🟢 ≥ ${pct(YELLOW,0)}  |  🟡 < ${pct(YELLOW,0)}  |  🔴 < ${pct(RED,0)} or open at end`,
    ''
  ].filter(Boolean).join('\n');

  if (!Array.isArray(dsRows) || dsRows.length === 0) {
    return [header + `Summary: pairs 0 | sources 0 | 🔴 0 | 🟡 0 | 🟢 0\n\n✅ All-green rollup: 0 sources across 0 pairs.`];
  }

  const seenPairs = new Set();
  const seenSources = new Set();
  let red = 0, yellow = 0, green = 0;

  const hotlist = [];
  for (const r of dsRows) {
    const ok = Number(r.ok_hits)||0;
    const st = Number(r.stalled_hits)||0;
    const ol = Number(r.outlier_hits)||0;
    const fe = Number(r.fetch_error_hits)||0; // ferr = fetch error hits
    const denom = ok + st;            // uptime only measures stall vs ok
    const up = denom > 0 ? ok/denom : NaN; // NaN => 🟡 by classify()
    const light = classify(up, !!r.open_at_end);

    seenPairs.add(`${r.chain_id}:${r.contract_address}`);
    if (r.datasource_name) seenSources.add(String(r.datasource_name).toLowerCase());
    if (light === '🔴') red++; else if (light === '🟡') yellow++; else green++;

    if (light !== '🟢') {
      const pairDisp = (r.pair || short(r.contract_address) || '').slice(0, 12);
      const srcDisp  = String(r.datasource_name||'').slice(0, 10);
      hotlist.push(
        [
          pad(pairDisp, 12),
          pad(srcDisp, 10),
          pad(`${light} ${pct(up||0,0)}`, 8),
          pad(`(${ok}/${st}/${ol}/${fe})`, 19),
          r.open_at_end ? 'yes' : 'no'
        ].join('  ')
      );
    }
  }

  const summary = `Summary: pairs ${seenPairs.size} | sources ${seenSources.size} | 🔴 ${red} | 🟡 ${yellow} | 🟢 ${green}`;
  if (hotlist.length === 0) {
    return [ [header, summary, '', `✅ All-green rollup: ${green} sources across ${seenPairs.size} pairs.`].join('\n') ];
  }

  const lines = [];
  lines.push(`pair        datasource  uptime  (ok/stall/out/ferr)  open`);
  lines.push(...hotlist);
  const chunks = chunkBySize(lines, 'Datasources');
  const withFooters = (chunks.length <= 1)
    ? chunks
    : chunks.map((c, i) => `${c}\n\n${i+1}/${chunks.length} ${i+1 < chunks.length ? '(cont.)' : '(end)'}`);

  return [
    [header, summary, '', '🔥 Hotlist (non-green only)'].join('\n'),
    ...withFooters
  ];
}

/* ---------- main ---------- */
async function sendWindowSummariesIfDue(client, runId) {
  log('[summary-entry]', { runId, at: new Date().toISOString() });
  if (!client || !Number.isFinite(runId) || MINUTES <= 0) return;

  const run = selRun.get(runId);
  if (!run) return;

  const startedAt = new Date(run.started_at);
  const currWinStart = floorToWindow(startedAt, MINUTES);
  // summarize the *previous* window (the one that just ended)
  const prevWinEnd = currWinStart;
  const prevWinStart = new Date(prevWinEnd.getTime() - MINUTES*60_000);
  const prevWinStartISO = prevWinStart.toISOString();
  const prevWinEndISO = prevWinEnd.toISOString();

  // ensure window ledger row exists for the intended window
  upsertWindow.run(prevWinStartISO, prevWinEndISO, runId);
  const ledger = getWindow.get(prevWinStartISO, prevWinEndISO);
  if (!ledger) return;

  log('[summary-window]', {
    prevWinStartISO, prevWinEndISO,
    owners_done: ledger.owners_done,
    admins_oracle_done: ledger.admins_oracle_done,
    admins_ds_done: ledger.admins_ds_done
  });

  /* ----- OWNER SUMMARIES (oracle only) ----- */
  if (Number(ledger.owners_done) === 0) {
    const oracleEndForOwners = selLatestOracleWindowEndLTE.get(prevWinEndISO)?.we || null;
    const ownersRowsEnd = oracleEndForOwners || prevWinEndISO;

    const rows = selOwnersRows.all(ownersRowsEnd);

    // group per owner
    const byOwner = new Map();
    for (const r of rows) {
      if (!byOwner.has(r.discord_id)) byOwner.set(r.discord_id, { user: r, items: [] });
      byOwner.get(r.discord_id).items.push(r);
    }

    for (const [discord_id, { user, items }] of byOwner.entries()) {
      const accepts = user.accepts_dm === 1;
      if (!accepts) continue;

      const hasEvent = items.some(it => (it.stalled_hits || 0) > 0 || it.open_at_end === 1);
      if (ONLY_IF_EVENTS && !hasEvent) continue;

      const ownersWinStartISO = selOracleWindowStartForEnd.get(ownersRowsEnd)?.ws || prevWinStartISO;
      const ownersWinStart = new Date(ownersWinStartISO);
      const ownersWinEnd = new Date(ownersRowsEnd);

      const msgs = renderOwnerSections(items, ownersWinStart, ownersWinEnd, user.discord_name);
      // 👉 owners: normal behavior (allow disabling on 50007), and use the correct recipient id
      for (const m of msgs) await sendDM(client, discord_id, m);
    }

    markOwnersDone.run(prevWinStartISO, prevWinEndISO);

    const ownersWinStartISO = selOracleWindowStartForEnd.get(ownersRowsEnd)?.ws || prevWinStartISO;
    if (ownersRowsEnd !== prevWinEndISO) {
      upsertWindow.run(ownersWinStartISO, ownersRowsEnd, runId);
      markOwnersDone.run(ownersWinStartISO, ownersRowsEnd);
    }
  }

  /* ----- ADMIN SUMMARIES (oracle + datasource) ----- */
  if (Number(ledger.admins_oracle_done) === 0 || Number(ledger.admins_ds_done) === 0) {
    const oracleEnd      = selLatestOracleWindowEndLTE.get(prevWinEndISO)?.we || prevWinEndISO;
    const oracleStartISO = selOracleWindowStartForEnd.get(oracleEnd)?.ws || prevWinStartISO;

    const dsEnd      = selLatestDSWindowEndLTE.get(prevWinEndISO)?.we || prevWinEndISO;
    const dsStartISO = selDSWindowStartForEnd.get(dsEnd)?.ws || prevWinStartISO;

    const adminRowsOracle = selAdminsOracle.all(oracleEnd);
    const adminRowsDS     = selAdminsDatasource.all(dsEnd);

    const hasOracleEvent =
      adminRowsOracle.some(r => (r.stalled_hits||0) > 0 || r.open_at_end === 1);
    const hasDSEvent =
      adminRowsDS.some(r =>
        (r.stalled_hits||0) > 0 ||
        (r.outlier_hits||0) > 0 ||
        (r.fetch_error_hits||0) > 0 ||
        r.open_at_end === 1
      );

    // Partial-window gates
    const oracleComplete = isOracleWindowComplete(adminRowsOracle);
    const dsComplete     = isDSWindowComplete(adminRowsDS);

    const willSendOracle = (Number(ledger.admins_oracle_done) === 0) && (!SKIP_PARTIAL || oracleComplete) && (!ONLY_IF_EVENTS || hasOracleEvent);
    const willSendDS     = (Number(ledger.admins_ds_done)     === 0) && (!SKIP_PARTIAL || dsComplete)     && (!ONLY_IF_EVENTS || hasDSEvent);

    log('[admin-summary-gate]', {
      SKIP_PARTIAL,
      ONLY_IF_EVENTS,
      intendedWindow: { prevWinStartISO, prevWinEndISO },
      oracle: {
        end: oracleEnd,
        start: oracleStartISO,
        rows: adminRowsOracle.length,
        complete: oracleComplete,
        hasEvent: hasOracleEvent,
        willSend: willSendOracle,
      },
      datasource: {
        end: dsEnd,
        start: dsStartISO,
        rows: adminRowsDS.length,
        complete: dsComplete,
        hasEvent: hasDSEvent,
        willSend: willSendDS,
      }
    });

    let sentOracle = false;
    let sentDS = false;

    const admins = selAdmins.all();
    for (const a of admins) {
      if (a.accepts_dm !== 1) continue;

      // Oracle compact DM
      if (willSendOracle) {
        const oracleMsgs = renderAdminOracleCompact(
          adminRowsOracle,
          new Date(oracleStartISO),
          new Date(oracleEnd),
          a.discord_name
        );

        log('[oracleMsgs-preview]',
          oracleMsgs.map((m, i) => ({
            i, len: m.length, hasFence: m.includes('```'),
            head: m.slice(0, 60).replace(/\n/g,'⏎'),
            tail: m.slice(-60).replace(/\n/g,'⏎'),
          }))
        );

        // 👉 admins: never disable on DM errors (use the admin's id)
        for (const m of oracleMsgs) await sendDM(client, a.discord_id, m, { neverDisable: true });
        sentOracle = true;
      }

      // Datasource compact DM
      if (willSendDS) {
        const dsMsgs = renderAdminDatasourceCompact(
          adminRowsDS,
          new Date(dsStartISO),
          new Date(dsEnd),
          a.discord_name
        );
        // 👉 admins: never disable on DM errors
        for (const m of dsMsgs) await sendDM(client, a.discord_id, m, { neverDisable: true });
        sentDS = true;
      }
    }

    // Mark intended window done for what we actually sent (and record actual ranges)
    if (sentOracle) {
      markAdminsOracleDone.run(oracleStartISO, oracleEnd, prevWinStartISO, prevWinEndISO);

      // also mark the *actual* oracle summarized window to avoid re-send later
      if (oracleEnd !== prevWinEndISO) {
        upsertWindow.run(oracleStartISO, oracleEnd, runId);
        markAdminsOracleDone.run(oracleStartISO, oracleEnd, oracleStartISO, oracleEnd);
      }
    }

    if (sentDS) {
      markAdminsDSDone.run(dsStartISO, dsEnd, prevWinStartISO, prevWinEndISO);

      // also mark the *actual* DS summarized window to avoid re-send later
      if (dsEnd !== prevWinEndISO) {
        upsertWindow.run(dsStartISO, dsEnd, runId);
        markAdminsDSDone.run(dsStartISO, dsEnd, dsStartISO, dsEnd);
      }
    }

    if (!sentOracle && !sentDS) {
      log('ℹ️ Admin summaries suppressed (partial window and/or no events); not marking as done.');
    }
  }
}

module.exports = { sendWindowSummariesIfDue };

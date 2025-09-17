// config/thresholds.js
require('dotenv').config();

/* ---------- helpers ---------- */
function num(name, def) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === '') return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}
function bool(name, def=false) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === '') return !!def;
  const s = String(raw).trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes' ) return true;
  if (s === '0' || s === 'false' || s === 'no'  ) return false;
  const n = Number(raw);
  return Number.isFinite(n) ? n !== 0 : !!def;
}
// If TEST_* is defined, use it; otherwise use base.
function pick(baseName, testName, def) {
  const hasTest = process.env[testName] != null && String(process.env[testName]).trim() !== '';
  return hasTest ? num(testName, def) : num(baseName, def);
}
function pickBool(baseName, testName, def=false) {
  const hasTest = process.env[testName] != null && String(process.env[testName]).trim() !== '';
  return hasTest ? bool(testName, def) : bool(baseName, def);
}

const DEBUG_ALL = (process.env.DEBUG_ALL ?? '0') === '1';

/* ---------- public ---------- */
function getAggregationConfig() {
  const cfg = {
    // ===== Core aggregation & DS stall sensitivity =====
    OUTLIER_PCT:            pick('OUTLIER_PCT',           'TEST_OUTLIER_PCT',           0.01),
    FRESHNESS_SEC:          pick('FRESHNESS_SEC',         'TEST_FRESHNESS_SEC',         10800),   // 3h
    STALL_FLAT_PCT:         pick('STALL_FLAT_PCT',        'TEST_STALL_FLAT_PCT',        0.0005),
    STALL_MARKET_MOVE_PCT:  pick('STALL_MARKET_MOVE_PCT', 'TEST_STALL_MARKET_MOVE_PCT', 0.005),
    STALL_MIN_SPAN_SEC:     pick('STALL_MIN_SPAN_SEC',    'TEST_STALL_MIN_SPAN_SEC',    43200),   // 12h
    QUORUM_MIN_USED:        pick('QUORUM_MIN_USED',       'TEST_QUORUM_MIN_USED',       2),

    // ===== Datasource consecutive thresholds (DS realtime/open/close) =====
    STALL_OPEN_CONSEC:      pick('STALL_OPEN_CONSEC',     'TEST_STALL_OPEN_CONSEC',     3),
    STALL_CLEAR_CONSEC:     pick('STALL_CLEAR_CONSEC',    'TEST_STALL_CLEAR_CONSEC',    3),

    // ===== Oracle window-hit thresholds (used by summaries) =====
    ORACLE_STALL_HITS_OPEN:  pick('ORACLE_STALL_HITS_OPEN',  'TEST_ORACLE_STALL_HITS_OPEN',  2),
    ORACLE_STALL_HITS_CLEAR: pick('ORACLE_STALL_HITS_CLEAR', 'TEST_ORACLE_STALL_HITS_CLEAR', 1),

    // ===== Oracle consecutive thresholds (optional realtime) =====
    ORACLE_OPEN_CONSEC:     pick('ORACLE_OPEN_CONSEC',    'TEST_ORACLE_OPEN_CONSEC',    2),
    ORACLE_CLEAR_CONSEC:    pick('ORACLE_CLEAR_CONSEC',   'TEST_ORACLE_CLEAR_CONSEC',   1),
    ORACLE_REALTIME_DM:     pickBool('ORACLE_REALTIME_DM','TEST_ORACLE_REALTIME_DM',    false),
  };

  // Guard: stall span must be <= freshness window, or DS stall detection can never trigger.
  if (cfg.STALL_MIN_SPAN_SEC > cfg.FRESHNESS_SEC) {
    console.warn(
      `⚠️ STALL_MIN_SPAN_SEC (${cfg.STALL_MIN_SPAN_SEC}s) > FRESHNESS_SEC (${cfg.FRESHNESS_SEC}s). ` +
      `Datasource stall detection will never trigger. Lower TEST_STALL_MIN_SPAN_SEC or raise TEST_FRESHNESS_SEC for testing.`
    );
  }

  // Guard: quorum should be at least 2 (median/mean with 1 source is pointless)
  if (cfg.QUORUM_MIN_USED < 2) {
    console.warn(
      `⚠️ QUORUM_MIN_USED = ${cfg.QUORUM_MIN_USED}. Values < 2 can yield fragile aggregates.`
    );
  }

  return cfg;
}

let _printed = false;
function logActiveAggregationConfig(cfg) {
  if (_printed) return;
  _printed = true;

  if (!DEBUG_ALL) return; // be quiet unless global debug is on

  const usingTest = (test) =>
    process.env[test] != null && String(process.env[test]).trim() !== '' ? ' (TEST*)' : '';

  console.log([
    '📐 aggregation config:',
    `  OUTLIER_PCT            = ${cfg.OUTLIER_PCT}${usingTest('TEST_OUTLIER_PCT')}`,
    `  FRESHNESS_SEC          = ${cfg.FRESHNESS_SEC}${usingTest('TEST_FRESHNESS_SEC')}`,
    `  STALL_FLAT_PCT         = ${cfg.STALL_FLAT_PCT}${usingTest('TEST_STALL_FLAT_PCT')}`,
    `  STALL_MARKET_MOVE_PCT  = ${cfg.STALL_MARKET_MOVE_PCT}${usingTest('TEST_STALL_MARKET_MOVE_PCT')}`,
    `  STALL_MIN_SPAN_SEC     = ${cfg.STALL_MIN_SPAN_SEC}${usingTest('TEST_STALL_MIN_SPAN_SEC')}`,
    `  QUORUM_MIN_USED        = ${cfg.QUORUM_MIN_USED}${usingTest('TEST_QUORUM_MIN_USED')}`,
    `  STALL_OPEN_CONSEC      = ${cfg.STALL_OPEN_CONSEC}${usingTest('TEST_STALL_OPEN_CONSEC')}`,
    `  STALL_CLEAR_CONSEC     = ${cfg.STALL_CLEAR_CONSEC}${usingTest('TEST_STALL_CLEAR_CONSEC')}`,
    `  ORACLE_STALL_HITS_OPEN = ${cfg.ORACLE_STALL_HITS_OPEN}${usingTest('TEST_ORACLE_STALL_HITS_OPEN')}`,
    `  ORACLE_STALL_HITS_CLR  = ${cfg.ORACLE_STALL_HITS_CLEAR}${usingTest('TEST_ORACLE_STALL_HITS_CLEAR')}`,
    `  ORACLE_OPEN_CONSEC     = ${cfg.ORACLE_OPEN_CONSEC}${usingTest('TEST_ORACLE_OPEN_CONSEC')}`,
    `  ORACLE_CLEAR_CONSEC    = ${cfg.ORACLE_CLEAR_CONSEC}${usingTest('TEST_ORACLE_CLEAR_CONSEC')}`,
    `  ORACLE_REALTIME_DM     = ${cfg.ORACLE_REALTIME_DM}${usingTest('TEST_ORACLE_REALTIME_DM')}`,
  ].join('\n'));
}

module.exports = { getAggregationConfig, logActiveAggregationConfig };

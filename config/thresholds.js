// config/thresholds.js
require('dotenv').config();

function num(name, def) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === '') return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}

// If TEST_* is defined, use it; otherwise use base.
function pick(baseName, testName, def) {
  const hasTest = process.env[testName] != null && String(process.env[testName]).trim() !== '';
  return hasTest ? num(testName, def) : num(baseName, def);
}

function getAggregationConfig() {
  const cfg = {
    OUTLIER_PCT:            num('OUTLIER_PCT', 0.01),
    FRESHNESS_SEC:          pick('FRESHNESS_SEC', 'TEST_FRESHNESS_SEC', 10800),
    STALL_FLAT_PCT:         pick('STALL_FLAT_PCT', 'TEST_STALL_FLAT_PCT', 0.0005),
    STALL_MARKET_MOVE_PCT:  pick('STALL_MARKET_MOVE_PCT', 'TEST_STALL_MARKET_MOVE_PCT', 0.005),
    STALL_MIN_SPAN_SEC:     pick('STALL_MIN_SPAN_SEC', 'TEST_STALL_MIN_SPAN_SEC', 43200),
    QUORUM_MIN_USED:        num('QUORUM_MIN_USED', 2),
  };

  // Helpful guard: stall span must be <= freshness window
  if (cfg.STALL_MIN_SPAN_SEC > cfg.FRESHNESS_SEC) {
    console.warn(
      `⚠️ STALL_MIN_SPAN_SEC (${cfg.STALL_MIN_SPAN_SEC}s) > FRESHNESS_SEC (${cfg.FRESHNESS_SEC}s).` +
      ` Stall detection will never trigger. Consider lowering TEST_STALL_MIN_SPAN_SEC or raising TEST_FRESHNESS_SEC for testing.`
    );
  }
  return cfg;
}

let _printed = false;
function logActiveAggregationConfig(cfg) {
  if (_printed) return;
  _printed = true;

  const usingTest = (base, test) =>
    process.env[test] != null && String(process.env[test]).trim() !== '' ? ' (TEST*)' : '';

  console.log(
    [
      `📐 aggregation config:`,
      `  OUTLIER_PCT            = ${cfg.OUTLIER_PCT}`,
      `  FRESHNESS_SEC          = ${cfg.FRESHNESS_SEC}${usingTest('FRESHNESS_SEC','TEST_FRESHNESS_SEC')}`,
      `  STALL_FLAT_PCT         = ${cfg.STALL_FLAT_PCT}${usingTest('STALL_FLAT_PCT','TEST_STALL_FLAT_PCT')}`,
      `  STALL_MARKET_MOVE_PCT  = ${cfg.STALL_MARKET_MOVE_PCT}${usingTest('STALL_MARKET_MOVE_PCT','TEST_STALL_MARKET_MOVE_PCT')}`,
      `  STALL_MIN_SPAN_SEC     = ${cfg.STALL_MIN_SPAN_SEC}${usingTest('STALL_MIN_SPAN_SEC','TEST_STALL_MIN_SPAN_SEC')}`,
      `  QUORUM_MIN_USED        = ${cfg.QUORUM_MIN_USED}`,
    ].join('\n')
  );
}

module.exports = { getAggregationConfig, logActiveAggregationConfig };
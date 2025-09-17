// services/rollup.js
// Rollup helpers for summary windows.
// - Uses split admin flags (admins_oracle_done, admins_ds_done) per new schema
// - Ensures open_at_end is always a concrete 0/1 (never NULL)

require('dotenv').config();
const { getDb } = require('../db');
const db = getDb();

/* ---------- global debug (single toggle) ---------- */
// Turn on with DEBUG_ALL=1 (or true/yes/on) in .env
const envFlag = k => ['1','true','yes','on'].includes(String(process.env[k]).toLowerCase());
const DEBUG = envFlag('DEBUG_ALL');
const log = (...args) => { if (DEBUG) console.log(...args); };

/* ---------- window helpers ---------- */
function getWindowSizeMinutes() {
  const n = Number(process.env.SUMMARY_WINDOW_MINUTES || 240);
  return Number.isFinite(n) && n > 0 ? n : 240;
}
function windowRange(dateLike) {
  const mins = getWindowSizeMinutes();
  const msPerWin = mins * 60 * 1000;
  const now = dateLike ? new Date(dateLike) : new Date();
  const startMs = Math.floor(now.getTime() / msPerWin) * msPerWin;
  const endMs = startMs + msPerWin;
  return {
    window_start: new Date(startMs).toISOString(),
    window_end:   new Date(endMs).toISOString(),
  };
}

/* ---------- summary_windows upsert (NEW SCHEMA) ---------- */
const insSummaryWindow = db.prepare(`
  INSERT OR IGNORE INTO summary_windows
    (window_start, window_end, owners_done, admins_oracle_done, admins_ds_done, created_by_run_id, processed_at)
  VALUES
    (?,            ?,          0,           0,                  0,              ?,                 NULL)
`);
const touchSummaryWindowEnd = db.prepare(`
  UPDATE summary_windows
     SET window_end = ?
   WHERE window_start = ?
     AND window_end   <> ?
`);

function ensureSummaryWindow(window_start, window_end, runId) {
  insSummaryWindow.run(window_start, window_end, runId ?? null);
  // If the row already existed (INSERT ignored), make sure window_end reflects current window sizing
  touchSummaryWindowEnd.run(window_end, window_start, window_end);
  if (DEBUG) log('[rollup.ensureSummaryWindow]', { window_start, window_end, runId });
}

function toNumOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

/* ---------- ORACLE ROLLUP ---------- */
const upsertOracle = db.prepare(`
  INSERT INTO oracle_health_rollup (
    window_start, window_end, chain_id, contract_address, validator_address,
    ok_hits, stalled_hits,
    last_dev_pct, last_span_sec, last_median_now, last_price,
    open_at_end, first_seen_run_id, last_seen_run_id
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(window_start, chain_id, contract_address, validator_address)
  DO UPDATE SET
    window_end        = excluded.window_end,
    ok_hits           = oracle_health_rollup.ok_hits      + excluded.ok_hits,
    stalled_hits      = oracle_health_rollup.stalled_hits + excluded.stalled_hits,
    last_dev_pct      = excluded.last_dev_pct,
    last_span_sec     = excluded.last_span_sec,
    last_median_now   = excluded.last_median_now,
    last_price        = excluded.last_price,
    open_at_end       = excluded.open_at_end,
    last_seen_run_id  = excluded.last_seen_run_id,
    first_seen_run_id = COALESCE(oracle_health_rollup.first_seen_run_id, excluded.first_seen_run_id)
`);

function bumpOracleRollup({
  chain_id, contract_address, validator_address,
  isStalled,
  dev_pct, span_sec, median_now, price,
  runId,
  at // optional Date for testing; defaults to now
}) {
  const { window_start, window_end } = windowRange(at);
  ensureSummaryWindow(window_start, window_end, runId);

  const ok       = isStalled ? 0 : 1;
  const stalled  = isStalled ? 1 : 0;
  const openFlag = isStalled ? 1 : 0;

  upsertOracle.run(
    window_start, window_end,
    chain_id,
    String(contract_address).toLowerCase(),
    String(validator_address).toLowerCase(),
    ok, stalled,
    toNumOrNull(dev_pct), toNumOrNull(span_sec), toNumOrNull(median_now), toNumOrNull(price),
    openFlag, runId ?? null, runId ?? null
  );
}

/* ---------- DATASOURCE ROLLUP ---------- */
const upsertDatasource = db.prepare(`
  INSERT INTO datasource_health_rollup (
    window_start, window_end, chain_id, contract_address, datasource_name,
    ok_hits, stalled_hits, outlier_hits, fetch_error_hits,
    last_dev_pct, last_span_sec, last_median_now, last_price,
    open_at_end, first_seen_run_id, last_seen_run_id
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(window_start, chain_id, contract_address, datasource_name)
  DO UPDATE SET
    window_end        = excluded.window_end,
    ok_hits           = datasource_health_rollup.ok_hits           + excluded.ok_hits,
    stalled_hits      = datasource_health_rollup.stalled_hits      + excluded.stalled_hits,
    outlier_hits      = datasource_health_rollup.outlier_hits      + excluded.outlier_hits,
    fetch_error_hits  = datasource_health_rollup.fetch_error_hits  + excluded.fetch_error_hits,
    last_dev_pct      = excluded.last_dev_pct,
    last_span_sec     = excluded.last_span_sec,
    last_median_now   = excluded.last_median_now,
    last_price        = excluded.last_price,
    open_at_end       = excluded.open_at_end,
    last_seen_run_id  = excluded.last_seen_run_id,
    first_seen_run_id = COALESCE(datasource_health_rollup.first_seen_run_id, excluded.first_seen_run_id)
`);

function bumpDatasourceRollup({
  chain_id, contract_address, datasource_name,
  isStalled,                  // boolean
  dev_pct, span_sec, median_now, price,
  runId,
  at // optional Date for testing; defaults to now
}) {
  const { window_start, window_end } = windowRange(at);
  ensureSummaryWindow(window_start, window_end, runId);

  const ok       = isStalled ? 0 : 1;
  const stalled  = isStalled ? 1 : 0;
  const openFlag = isStalled ? 1 : 0;

  upsertDatasource.run(
    window_start, window_end,
    chain_id,
    String(contract_address).toLowerCase(),
    String(datasource_name).toLowerCase(),
    ok, stalled, /* outlier */ 0, /* fetch_err */ 0,
    toNumOrNull(dev_pct), toNumOrNull(span_sec), toNumOrNull(median_now), toNumOrNull(price),
    openFlag, runId ?? null, runId ?? null
  );
}

// Outlier-only event: keep open_at_end=0 (no stall context)
function noteDatasourceOutlierHit({
  chain_id, contract_address, datasource_name,
  dev_pct, median_now, price,
  runId,
  at // optional Date; defaults to now
}) {
  const { window_start, window_end } = windowRange(at);
  ensureSummaryWindow(window_start, window_end, runId);

  upsertDatasource.run(
    window_start, window_end,
    chain_id,
    String(contract_address).toLowerCase(),
    String(datasource_name).toLowerCase(),
    /* ok */ 0, /* stalled */ 0, /* outlier */ 1, /* fetch_err */ 0,
    toNumOrNull(dev_pct), null, toNumOrNull(median_now), toNumOrNull(price),
    0, runId ?? null, runId ?? null
  );
}

// Fetch/parse error as a health datapoint
function noteDatasourceFetchError({
  chain_id, contract_address, datasource_name,
  runId,
  at // optional Date; defaults to now
}) {
  const { window_start, window_end } = windowRange(at);
  ensureSummaryWindow(window_start, window_end, runId);

  upsertDatasource.run(
    window_start, window_end,
    chain_id,
    String(contract_address).toLowerCase(),
    String(datasource_name).toLowerCase(),
    0, 0, 0, 1,   // fetch_error_hits +1
    null, null, null, null,
    0, runId ?? null, runId ?? null
  );
}

module.exports = {
  bumpOracleRollup,
  bumpDatasourceRollup,
  noteDatasourceOutlierHit,
  noteDatasourceFetchError,
  // testing hooks
  _windowRange: windowRange,
};

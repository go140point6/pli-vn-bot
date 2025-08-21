// db/statements.js
const { getDb } = require('./');   // resolves to db/index.js
const db = getDb();

/* ========== Ingest runs ========== */
const beginRunStmt = db.prepare(`INSERT INTO ingest_runs DEFAULT VALUES`);
const endRunStmt   = db.prepare(`UPDATE ingest_runs SET ended_at = CURRENT_TIMESTAMP WHERE id = ?`);

/* ========== Snapshots (datasource) ========== */
const insertSnapshot = db.prepare(`
  INSERT INTO datasource_price_snapshots (run_id, chain_id, contract_address, datasource_name, price)
  VALUES (?, ?, ?, ?, ?)
`);

/* ========== Snapshots (oracles) ========== */
const insertOracleSnapshot = db.prepare(`
  INSERT INTO oracle_price_snapshots (run_id, chain_id, contract_address, validator_address, price)
  VALUES (?, ?, ?, ?, ?)
`);

/* ========== Contracts (labels & enumerations) ========== */
// Pair + base/quote (useful when you want either the explicit pair or to synthesize it)
const selContractPair = db.prepare(`
  SELECT pair, base, quote
  FROM contracts
  WHERE chain_id = ? AND address = ?
`);

// Convenience single-string label (pair if present, else base/quote)
const selContractLabel = db.prepare(`
  SELECT
    CASE
      WHEN pair IS NOT NULL AND TRIM(pair) <> '' THEN TRIM(pair)
      ELSE base || '/' || quote
    END AS label
  FROM contracts
  WHERE chain_id = ? AND address = ?
`);

// All active contracts (for oracle polling, etc.)
const selActiveContracts = db.prepare(`
  SELECT chain_id, address AS contract_address, pair, base, quote
  FROM contracts
  WHERE active = 1
`);

/* ========== Datasource API metadata & mappings ========== */
const selApiMeta = db.prepare(`
  SELECT base_url, response_path, headers
  FROM datasource_apis
  WHERE datasource_name = ?
`);

const selMappingsBySource = db.prepare(`
  SELECT chain_id, contract_address, datasource_pair_id, base, quote
  FROM datasource_contract_map
  WHERE datasource_name = ?
`);

/* ========== Users / Admins ========== */
const selAdmins    = db.prepare(`SELECT discord_id, discord_name, accepts_dm FROM users WHERE is_admin = 1`);
const setAcceptsDM = db.prepare(`UPDATE users SET accepts_dm = 0 WHERE discord_id = ?`);

/* ========== Alerts (open/insert/resolve) ========== */
const selOpenAlert = db.prepare(`
  SELECT id
  FROM alerts
  WHERE discord_id = ?
    AND chain_id = ?
    AND contract_address = ?
    AND alert_type = ?
    AND resolved_at IS NULL
  LIMIT 1
`);

const insAlert = db.prepare(`
  INSERT INTO alerts (discord_id, chain_id, contract_address, alert_type, severity, message, extra)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const resolveAlertById = db.prepare(`
  UPDATE alerts SET resolved_at = CURRENT_TIMESTAMP WHERE id = ?
`);

/* ========== Aggregation helpers (fresh data, outliers, stalls) ========== */
/* cutoffEpoch is UNIX seconds */
const selActiveContractsWithFreshData = db.prepare(`
  SELECT DISTINCT s.chain_id, s.contract_address
  FROM datasource_price_snapshots s
  JOIN contracts c
    ON c.chain_id = s.chain_id AND c.address = s.contract_address
  WHERE c.active = 1
    AND s.timestamp >= datetime(?, 'unixepoch')
`);

/* All fresh snapshots for a contract (we’ll pick newest per source in JS) */
const selFreshSnapshotsForContract = db.prepare(`
  SELECT datasource_name, price, timestamp, run_id
  FROM datasource_price_snapshots
  WHERE chain_id = ?
    AND contract_address = ?
    AND timestamp >= datetime(?, 'unixepoch')
  ORDER BY datasource_name ASC, timestamp DESC
`);

/* Write aggregate row */
const insAggregate = db.prepare(`
  INSERT INTO price_aggregates
    (run_id, chain_id, contract_address, window_start, window_end, median, mean, source_count, used_sources, discarded_sources)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

/* Last 3 samples for stall check (one source, one contract) */
const selLast3ForSourceContract = db.prepare(`
  SELECT price, timestamp, run_id
  FROM datasource_price_snapshots
  WHERE chain_id = ?
    AND contract_address = ?
    AND datasource_name = ?
  ORDER BY timestamp DESC
  LIMIT 3
`);

/* Fetch prices grouped by two run_ids (to compute market medians) */
const selPricesForRuns = db.prepare(`
  SELECT run_id, price
  FROM datasource_price_snapshots
  WHERE chain_id = ?
    AND contract_address = ?
    AND run_id IN (?, ?)
`);

/* Which sources are mapped to a contract (for outlier/stall sweeps) */
const selSourcesForContract = db.prepare(`
  SELECT DISTINCT datasource_name
  FROM datasource_contract_map
  WHERE chain_id = ?
    AND contract_address = ?
`);

module.exports = {
  // runs
  beginRunStmt, endRunStmt,

  // snapshots (datasource & oracle)
  insertSnapshot,
  insertOracleSnapshot,

  // contracts
  selContractPair,
  selContractLabel,
  selActiveContracts,

  // datasource config/mapping
  selApiMeta, selMappingsBySource,

  // users/admins
  selAdmins, setAcceptsDM,

  // alerts
  selOpenAlert, insAlert, resolveAlertById,

  // aggregation helpers
  selActiveContractsWithFreshData,
  selFreshSnapshotsForContract,
  insAggregate,
  selLast3ForSourceContract,
  selPricesForRuns,
  selSourcesForContract,
};

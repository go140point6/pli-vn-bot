// dev/createDB.js
// Fresh SQLite schema for the Discord bot (better-sqlite3).
// - Canonical address key: lowercase 0x... (EIP-55 kept for display)
// - Multi-chain ready: composite PKs on (chain_id, address)
// - addr_format to optionally render XDC as 'xdc...' while storing '0x...'
// - WAL, sane PRAGMA, atomic build, JSON checks (if available), STRICT tables (if supported)
// - Explicit ingest runs (ingest_runs + run_id in *_price_snapshots)

const { openDb, dbFile } = require('../db');
const Database = require('better-sqlite3');
const db = openDb({ fileMustExist: false });

// ---- PRAGMAs ----
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('busy_timeout = 5000');
db.pragma('foreign_keys = ON');

// Detect optional features
const compileOptions = db.pragma('compile_options'); // array
const hasJSON1 = compileOptions.some(s => /JSON1/.test(s));
const hasSTRICT = compileOptions.some(s => /STRICT/.test(s));

// Helpers
const STRICT = hasSTRICT ? ' STRICT' : '';
const jsonCheck = (col) => hasJSON1 ? `CHECK (${col} IS NULL OR json_valid(${col}))` : '';
const maybeComma = (s) => (s && s.trim() ? `, ${s}` : '');

db.exec('BEGIN');
try {
  console.log('📦 Connected ✅');
  console.log('🛠️  Creating tables...\n');

  // ========= users =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      discord_id TEXT PRIMARY KEY,
      discord_name TEXT,
      accepts_dm INTEGER NOT NULL DEFAULT 0 CHECK (accepts_dm IN (0,1)),
      is_admin INTEGER NOT NULL DEFAULT 0 CHECK (is_admin IN (0,1)),
      warning_threshold INTEGER,
      critical_threshold INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      CHECK (critical_threshold < warning_threshold)
    )${STRICT};
  `);

  db.exec(`
    DROP TRIGGER IF EXISTS trg_users_updated_at;
    CREATE TRIGGER trg_users_updated_at
    AFTER UPDATE ON users
    FOR EACH ROW
    WHEN NEW.updated_at <= OLD.updated_at
    BEGIN
      UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE rowid = NEW.rowid;
    END;
  `);

  // ========= validators =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS validators (
      chain_id INTEGER NOT NULL,
      address  TEXT NOT NULL
        CHECK (address = LOWER(address))
        CHECK (LOWER(address) LIKE '0x%' AND LENGTH(address) = 42),
      address_eip55 TEXT,
      addr_format   TEXT NOT NULL DEFAULT '0x' CHECK (addr_format IN ('0x','xdc')),
      PRIMARY KEY (chain_id, address)
    )${STRICT};
  `);

  // ========= validator_owners (N:M) =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS validator_owners (
      chain_id INTEGER NOT NULL,
      validator_address TEXT NOT NULL CHECK (validator_address = LOWER(validator_address)),
      discord_id TEXT NOT NULL,
      PRIMARY KEY (chain_id, validator_address, discord_id),
      FOREIGN KEY (chain_id, validator_address) REFERENCES validators(chain_id, address) ON DELETE CASCADE,
      FOREIGN KEY (discord_id) REFERENCES users(discord_id) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= contracts =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS contracts (
      chain_id INTEGER NOT NULL,
      address  TEXT NOT NULL
        CHECK (address = LOWER(address))
        CHECK (LOWER(address) LIKE '0x%' AND LENGTH(address) = 42),
      address_eip55 TEXT,
      addr_format   TEXT NOT NULL DEFAULT '0x' CHECK (addr_format IN ('0x','xdc')),
      pair  TEXT,
      base  TEXT NOT NULL,
      quote TEXT NOT NULL,
      active INTEGER DEFAULT 0 CHECK (active IN (0,1)),
      PRIMARY KEY (chain_id, address)
    )${STRICT};
  `);

  // ========= validator_contracts =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS validator_contracts (
      chain_id INTEGER NOT NULL,
      validator_address TEXT NOT NULL CHECK (validator_address = LOWER(validator_address)),
      contract_address  TEXT NOT NULL CHECK (contract_address  = LOWER(contract_address)),
      PRIMARY KEY (chain_id, validator_address, contract_address),
      FOREIGN KEY (chain_id, validator_address) REFERENCES validators(chain_id, address) ON DELETE CASCADE,
      FOREIGN KEY (chain_id, contract_address)  REFERENCES contracts(chain_id, address)  ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= mn_rpc / mn_wss =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS mn_rpc (
      mn TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      discord_id TEXT,
      public INTEGER NOT NULL CHECK (public IN (0,1)),
      FOREIGN KEY (discord_id) REFERENCES users(discord_id) ON DELETE SET NULL
    )${STRICT};
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS mn_wss (
      mn TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      discord_id TEXT,
      public INTEGER NOT NULL CHECK (public IN (0,1)),
      FOREIGN KEY (discord_id) REFERENCES users(discord_id) ON DELETE SET NULL
    )${STRICT};
  `);

  // ========= datasources & datasource_apis =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS datasources (
      datasource_name TEXT PRIMARY KEY
    )${STRICT};
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS datasource_apis (
      datasource_name TEXT PRIMARY KEY,
      base_url TEXT NOT NULL,
      response_path TEXT NOT NULL,
      headers TEXT DEFAULT NULL
      ${maybeComma(jsonCheck('headers'))},
      FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= ingest_runs (now includes digest) =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS ingest_runs (
      id INTEGER PRIMARY KEY,
      started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      ended_at   DATETIME,
      digest     TEXT
    )${STRICT};
  `);

  // ========= datasource_price_snapshots =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS datasource_price_snapshots (
      id INTEGER PRIMARY KEY,
      run_id INTEGER NOT NULL,
      chain_id INTEGER NOT NULL,
      contract_address TEXT NOT NULL CHECK (contract_address = LOWER(contract_address)),
      datasource_name  TEXT NOT NULL,
      price REAL NOT NULL,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (run_id) REFERENCES ingest_runs(id) ON DELETE CASCADE,
      FOREIGN KEY (chain_id, contract_address) REFERENCES contracts(chain_id, address) ON DELETE CASCADE,
      FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= oracle_price_snapshots =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS oracle_price_snapshots (
      id INTEGER PRIMARY KEY,
      run_id INTEGER NOT NULL,
      chain_id INTEGER NOT NULL,
      contract_address  TEXT NOT NULL CHECK (contract_address  = LOWER(contract_address)),
      validator_address TEXT NOT NULL CHECK (validator_address = LOWER(validator_address)),
      price REAL NOT NULL,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (run_id) REFERENCES ingest_runs(id) ON DELETE CASCADE,
      FOREIGN KEY (chain_id, contract_address)  REFERENCES contracts(chain_id, address)  ON DELETE CASCADE,
      FOREIGN KEY (chain_id, validator_address) REFERENCES validators(chain_id, address) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= datasource_contract_map =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS datasource_contract_map (
      datasource_name    TEXT NOT NULL,
      chain_id           INTEGER NOT NULL,
      contract_address   TEXT NOT NULL CHECK (contract_address = LOWER(contract_address)),
      contract_pair_id   TEXT NOT NULL,
      datasource_pair_id TEXT,
      base  TEXT NOT NULL,
      quote TEXT NOT NULL,
      PRIMARY KEY (datasource_name, chain_id, contract_address),
      UNIQUE  (datasource_name, datasource_pair_id),
      FOREIGN KEY (datasource_name)            REFERENCES datasources(datasource_name) ON DELETE CASCADE,
      FOREIGN KEY (chain_id, contract_address) REFERENCES contracts(chain_id, address) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= alerts =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS alerts (
      id INTEGER PRIMARY KEY,
      discord_id TEXT NOT NULL,
      chain_id INTEGER,
      contract_address  TEXT,
      validator_address TEXT,
      alert_type TEXT NOT NULL,
      severity  TEXT NOT NULL,
      message   TEXT NOT NULL,
      started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      resolved_at DATETIME,
      extra TEXT
      ${maybeComma(jsonCheck('extra'))},
      CHECK (severity IN ('info','warning','critical')),
      CHECK (length(alert_type) BETWEEN 1 AND 128),
      CHECK (
        (contract_address IS NULL AND validator_address IS NULL)
        OR (chain_id IS NOT NULL)
      ),
      FOREIGN KEY (discord_id) REFERENCES users(discord_id) ON DELETE CASCADE,
      FOREIGN KEY (chain_id, contract_address)  REFERENCES contracts(chain_id, address)  ON DELETE CASCADE,
      FOREIGN KEY (chain_id, validator_address) REFERENCES validators(chain_id, address) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= price_aggregates =========
  db.exec(`
    CREATE TABLE IF NOT EXISTS price_aggregates (
      id INTEGER PRIMARY KEY,
      run_id INTEGER,
      chain_id INTEGER NOT NULL,
      contract_address TEXT NOT NULL CHECK (contract_address = LOWER(contract_address)),
      window_start DATETIME NOT NULL,
      window_end   DATETIME NOT NULL,
      median REAL NOT NULL,
      mean   REAL,
      source_count INTEGER NOT NULL,
      used_sources INTEGER NOT NULL,
      discarded_sources TEXT
      ${maybeComma(jsonCheck('discarded_sources'))},
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      CHECK (window_start <= window_end),
      FOREIGN KEY (run_id) REFERENCES ingest_runs(id) ON DELETE SET NULL,
      FOREIGN KEY (chain_id, contract_address) REFERENCES contracts(chain_id, address) ON DELETE CASCADE
    )${STRICT};
  `);

  // ========= Indexes =========

  // Admins
  db.exec(`CREATE INDEX IF NOT EXISTS idx_users_is_admin ON users(is_admin) WHERE is_admin = 1;`);

  // Contracts active
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_contracts_active
    ON contracts(chain_id, address)
    WHERE active = 1;
  `);

  // Ingest runs
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ingest_runs_time ON ingest_runs(started_at DESC);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ingest_runs_digest_time ON ingest_runs(digest, started_at DESC);`);

  // Datasource snapshots
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_datasource_snapshots_contract_source
    ON datasource_price_snapshots (chain_id, contract_address, datasource_name, timestamp DESC);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_datasource_snapshots_contract_time
    ON datasource_price_snapshots (chain_id, contract_address, timestamp DESC);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_datasource_snapshots_run
    ON datasource_price_snapshots (run_id, chain_id, contract_address);
  `);
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_datasource_snapshots_run_source
    ON datasource_price_snapshots (run_id, chain_id, contract_address, datasource_name);
  `);

  // Oracle snapshots
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_oracle_snapshots_contract_validator
    ON oracle_price_snapshots (chain_id, contract_address, validator_address, timestamp DESC);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_oracle_snapshots_contract_time
    ON oracle_price_snapshots (chain_id, contract_address, timestamp DESC);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_oracle_snapshots_run
    ON oracle_price_snapshots (run_id, chain_id, contract_address);
  `);
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_oracle_snapshots_run_source
    ON oracle_price_snapshots (run_id, chain_id, contract_address, validator_address);
  `);

  // Validator-Contract reverse lookup
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_validator_contracts_by_contract
    ON validator_contracts(chain_id, contract_address, validator_address);
  `);

  // validator_owners indexes
  db.exec(`CREATE INDEX IF NOT EXISTS idx_validator_owners_by_user ON validator_owners(discord_id);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_validator_owners_by_validator ON validator_owners(chain_id, validator_address);`);

  // Alerts
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_open_contract
    ON alerts(discord_id, chain_id, contract_address, alert_type)
    WHERE resolved_at IS NULL AND contract_address IS NOT NULL;
  `);
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_open_validator
    ON alerts(discord_id, chain_id, validator_address, alert_type)
    WHERE resolved_at IS NULL AND validator_address IS NOT NULL;
  `);
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_open_global
    ON alerts(discord_id, alert_type)
    WHERE resolved_at IS NULL AND contract_address IS NULL AND validator_address IS NULL;
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_started_at ON alerts(started_at DESC);`);

  // Price aggregates
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_price_aggregates_window
    ON price_aggregates(chain_id, contract_address, window_start, window_end);
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_price_aggregates_time ON price_aggregates(chain_id, window_end DESC);`);

  db.exec('COMMIT');
  console.log('\n✅ Schema created successfully!');
} catch (err) {
  db.exec('ROLLBACK');
  console.error('❌ Schema creation failed:', err);
  throw err;
} finally {
  db.close();
  console.log('🔒 Database connection closed.\n');
}

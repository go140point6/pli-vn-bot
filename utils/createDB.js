const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, '../data/validators.db');
const db = new Database(dbPath);

console.log('📦 Connected to SQLite database ✅');
console.log('🛠️  Creating tables...\n');

// Enforce foreign keys
db.pragma('foreign_keys = ON');

// === Table Definitions ===

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    discord_id TEXT PRIMARY KEY,
    discord_name TEXT,
    accepts_dm BOOLEAN DEFAULT 0 CHECK(accepts_dm IN (0, 1)),
    warning_threshold INTEGER,
    warned INTEGER DEFAULT 0 CHECK(warned IN (0, 1)),
    critical_threshold INTEGER,
    CHECK (critical_threshold < warning_threshold)
  );
`);
console.log('👤 users table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS validators (
    address TEXT PRIMARY KEY CHECK(LOWER(address) LIKE '0x%' AND LENGTH(address) = 42),
    discord_id TEXT NOT NULL,
    discord_name TEXT NOT NULL,
    FOREIGN KEY (discord_id) REFERENCES users(discord_id)
  );
`);
console.log('🔗 validators table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS contracts (
    address TEXT PRIMARY KEY CHECK(LOWER(address) LIKE '0x%' AND LENGTH(address) = 42),
    pair TEXT,
    base TEXT NOT NULL,
    quote TEXT NOT NULL,
    active BOOLEAN DEFAULT 0 CHECK(active IN (0, 1))
  );
`);
console.log('📄 contracts table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS validator_contracts (
    validator_address TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    PRIMARY KEY (validator_address, contract_address),
    FOREIGN KEY (validator_address) REFERENCES validators(address),
    FOREIGN KEY (contract_address) REFERENCES contracts(address)
  );
`);
console.log('📎 validator_contracts table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS mn_rpc (
    mn TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    discord_id INTEGER,
    public BOOLEAN NOT NULL CHECK(public IN (0, 1)),
    FOREIGN KEY (discord_id) REFERENCES users(discord_id)
  );
`);
console.log('🌐 mn_rpc table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS mn_wss (
    mn TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    discord_id INTEGER,
    public BOOLEAN NOT NULL CHECK(public IN (0, 1)),
    FOREIGN KEY (discord_id) REFERENCES users(discord_id)
  );
`);
console.log('📡 mn_wss table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS datasources (
    datasource_name TEXT PRIMARY KEY
  );
`);
console.log('🧪 datasources table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS datasource_price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_address TEXT NOT NULL,
    datasource_name TEXT NOT NULL,
    price REAL NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contract_address) REFERENCES contracts(address),
    FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name)
  );
`);
console.log('📈 datasource_price_snapshots table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS oracle_price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_address TEXT NOT NULL,
    validator_address TEXT NOT NULL,
    price REAL NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contract_address) REFERENCES contracts(address),
    FOREIGN KEY (validator_address) REFERENCES validators(address)
  );
`);
console.log('🔮 oracle_price_snapshots table created');

db.exec(`
  CREATE TABLE IF NOT EXISTS datasource_contract_map (
    datasource_name TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    contract_pair_id TEXT NOT NULL,
    datasource_pair_id TEXT,
    base TEXT NOT NULL,
    quote TEXT NOT NULL,
    PRIMARY KEY (datasource_name, contract_address),
    UNIQUE (datasource_name, datasource_pair_id),
    FOREIGN KEY (datasource_name) REFERENCES datasources(datasource_name),
    FOREIGN KEY (contract_address) REFERENCES contracts(address)
  );
`);
console.log('🗺️  datasource_contract_map table created');

// === Indexes ===

db.exec(`
  CREATE INDEX IF NOT EXISTS idx_datasource_snapshots_contract_source
  ON datasource_price_snapshots (contract_address, datasource_name, timestamp DESC);
`);
console.log('⚡ Index created on datasource_price_snapshots');

db.exec(`
  CREATE INDEX IF NOT EXISTS idx_oracle_snapshots_contract_validator
  ON oracle_price_snapshots (contract_address, validator_address, timestamp DESC);
`);
console.log('⚡ Index created on oracle_price_snapshots');

console.log('\n✅ All tables and indexes created successfully!');
db.close();
console.log('🔒 Database connection closed.\n');
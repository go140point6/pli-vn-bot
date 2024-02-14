const sqlite3 = require('sqlite3').verbose();

// Connect to the SQLite database (or create it if it doesn't exist)
const db = new sqlite3.Database('../data/validators.db');

// Create a table
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS validators (
        address TEXT PRIMARY KEY CHECK(LOWER(address) LIKE '0x%' AND LENGTH(address) = 42), 
        user TEXT,
        warning INTEGER CHECK(warning >= 50 AND warning <= 1000),
        critical INTEGER CHECK(critical >= 1 AND critical < 80) 
        )
    `)
})

// Close the database connection after completing operations
db.close()
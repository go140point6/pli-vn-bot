const sqlite3 = require('sqlite3').verbose();

// Connect to the SQLite database (or create it if it doesn't exist)
const db = new sqlite3.Database('../data/validators.db');

const validatorData = [
  ['0xf87A639bCE2064aBA1833a2ADeB1caD5800b46bD', '567425551229386758', 100, 70],
  ['0xaC240DAc406DDD87CD48bA02D67e8aEc8F129462', '905444143503921192', 100, 70],
  ['0x507bbb6f40fd3103aDe74fe5883d04fcd4D5bd56', '567425551229386758', 1, 0.9]
];

// Sample test data for rpc-mn
const rpcMnData = [
  ['https://erpc.xinfin.network', 'blocksscan', false],
  ['https://earpc.xinfin.network', 'blocksscan', false],
  ['https://rpc.primenumbers.xyz', 'prime-numbers', false]
];

// Sample test data for wss-mn
const wssMnData = [
  ['wss://eaws.xinfin.network', 'blocksscan', false],
  ['wss://ews.xinfin.network', 'blocksscan', false],
  ['wss://ws.xinfin.network', 'blocksscan', false]
];

// Create a table
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS validators (
        address TEXT PRIMARY KEY CHECK(LOWER(address) LIKE '0x%' AND LENGTH(address) = 42), 
        user TEXT,
        warning INTEGER,
        critical INTEGER 
        )
    `)

  db.run(`
    CREATE TABLE IF NOT EXISTS rpc_mn (
      id INTEGER PRIMARY KEY,
      mn TEXT UNIQUE,
      owner TEXT,
      private BOOLEAN
    )
  `)

  db.run(`
    CREATE TABLE IF NOT EXISTS wss_mn (
      id INTEGER PRIMARY KEY,
      mn TEXT UNIQUE,
      owner TEXT,
      private BOOLEAN
    )
  `)

  const validatorStmt = db.prepare(`
    INSERT INTO validators (address, user, warning, critical)
    VALUES (?, ?, ?, ?)
  `)
  validatorData.forEach(data => {
    validatorStmt.run(data, err => {
      if (err) {
        console.error("Error inserting validator data", err)
      }
    })
  })
  validatorStmt.finalize()

  // Check
  db.all("SELECT * FROM validators", (err, rows) => {
    if (err) {
      console.log("Error retrieving data from validators table:", err)
      return
    }

    console.log("Validators table:")
    console.table(rows)
  })

  const rpcMnStmt = db.prepare(`
  INSERT INTO rpc_mn (mn, owner, private)
  VALUES (?, ?, ?)
  `)
  rpcMnData.forEach(data => {
    rpcMnStmt.run(data, err => {
      if (err) {
        console.error("Error inserting rpc data", err)
      }
    })
  })
  rpcMnStmt.finalize()

  // Check
  db.all("SELECT * FROM rpc_mn", (err, rows) => {
  if (err) {
    console.log("Error retrieving data from rpc_mn table:", err)
    return
  }

  console.log("RPC_mn table:")
  console.table(rows)
  })

  const wssMnStmt = db.prepare(`
  INSERT INTO wss_mn (mn, owner, private)
  VALUES (?, ?, ?)
  `)
  wssMnData.forEach(data => {
    wssMnStmt.run(data, err => {
      if (err) {
        console.error("Error inserting wss data", err)
      }
    })
  })
  wssMnStmt.finalize()

  // Check
  db.all("SELECT * FROM wss_mn", (err, rows) => {
    if (err) {
      console.log("Error retrieving data from wss_mn table:", err)
      return
    }
  
  console.log("WSS_mn table:")
  console.table(rows)
  })

})

// Close the database connection after completing operations
db.close()
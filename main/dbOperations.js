const util = require('util');
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./data/validators.db')
const dbGetAsync = util.promisify(db.get)

async function addAddress(result, user) {
    //Query the DB for this address
    const [res, res2] = await verifyAddress(result)

    if (res) { // if true (address exists from verify function)
        let res2 = false
        return [res, res2]
    } else {
        // Insert address and user id
        const warning = 80
        const critical = 74
        const insert = db.prepare('INSERT OR IGNORE INTO validators (address, name, warning, critical) VALUES (?, ?, ?, ?)')
        insert.run(result.result, user, warning, critical)
        insert.finalize()

        // Double-check that the address exists now.
        let res2 = await verifyAddress(result)
        return [res, res2] 
    }
}

async function removeAddress(result, user) {
	//console.log(`Do DB add stuff: ${user} ${result.result}`)
    //Query the DB for this address
    const [res, res2] = await verifyAddress(result)
    //const testResult = await verifyAddress(result)
    //console.log("Test Result: ", testResult)

    if (res) { // if true
        // Remove address and user id
        const remove = db.prepare('DELETE FROM validators WHERE address = ?')
        remove.run(result.result)
        remove.finalize()

        // Double-check tha the address no longer exists.
        let res2 = await verifyAddress(result)
        return [res, res2] 
    } else {
        let res2 = true
        return [res, res2]
    }
}

async function verifyAddress(result) {
    return new Promise((res, rej) => {
    	//console.log("Do DB check stuff", result.result)
        let address = result.result

        // Simulating a failure by rejecting the promise
        // if (address === '0xf87A639bCE2064aBA1833a2ADeB1caD5800b46bD') {
        //     const simulatedError = new Error('Simulated failure');
        //     rej(simulatedError)
        //     return
        // }

        // Query the database
        const query = 'SELECT COUNT(*) AS count FROM validators WHERE address = ?'

        db.get(query, [address], (err, row) => {
            if (err) {
                console.log(err.message)
                rej(err)
            } else {
                // If count is greater than 0, the value exists
                const valueExists = row.count > 0
                res([valueExists])
            }
        })
    })
}

async function getAllRows() {
    return new Promise((res, rej) => {
        const query = 'SELECT address, name, warning, critical FROM validators'

        db.all(query, [], (err, rows) => {
            if (err) {
                console.log(err.message)
                rej(err)                
            } else {
                res(rows)
            }
        })
    })   
}

async function checkRpc(isPublic, userId) {
    return new Promise((res, rej) => {
        let query
        let params = []
        if (isPublic) {
            query = 'SELECT mn FROM rpc_mn WHERE private = 0'
        } else {
            query = 'SELECT mn FROM rpc_mn WHERE private = 1 AND owner = ?'
            params.push(userId)
        }

        db.all(query, params, (err, rows) => {
            if (err) {
                console.log(err.message)
                rej(err)
            } else {
                res(rows)
            }
        })
    })
}

async function checkWss(isPublic, userId) {
    return new Promise((res, rej) => {
        let query
        let params = []
        if (isPublic) {
            query = 'SELECT mn FROM wss_mn WHERE private = 0'
        } else {
            query = 'SELECT mn FROM wss_mn WHERE private = 1 AND owner = ?'
            params.push(userId)
        }

        db.all(query, params, (err, rows) => {
            if (err) {
                console.log(err.message)
                rej(err)
            } else {
                res(rows)
            }
        })
    })
}

// Check the db to see if rpc and/or websocket already present
async function checkMn(rpc, wss) {
    try {
        const query1 = 'SELECT COUNT(*) AS rpcCount FROM rpc_mn WHERE mn = ?';
        const query2 = 'SELECT COUNT(*) AS wssCount FROM wss_mn WHERE mn = ?';

        const [rpcResult, wssResult] = await Promise.all([
            dbGetAsync.call(db, query1, [rpc]),
            dbGetAsync.call(db, query2, [wss])
        ]);

        const rpcCount = `rpc.${rpcResult.rpcCount}`
        const wssCount = `wss.${wssResult.wssCount}`

        return { 
            rpc: rpcCount, 
            wss: wssCount 
        }
    } catch (err) {
        console.error(err.message);
        throw err;
    }
}

async function verifyMn(table, mn) {
    return new Promise((res, rej) => {
        const query = `SELECT COUNT(*) AS count FROM ${table} WHERE mn = ?`

        db.get(query, [mn], (err, row) => {
            if (err) {
                console.log(err.message)
                rej(err)
            } else {
                const valueExists = row.count > 0
                //console.log(valueExists)
                res(valueExists)
            }
        })
    })

}

async function addMn(RpcOrWss, mn, owner) {
    let table

    if (RpcOrWss === 'rpc') {
        table = 'rpc_mn'
    } else if (RpcOrWss === 'wss') {
        table = 'wss_mn'
    }

    try {

        const private = 1
        const insert = db.prepare(`INSERT OR IGNORE INTO ${table} (mn, owner, private) VALUES (?, ?, ?)`)
        insert.run(mn, owner, private)
        insert.finalize()

        let verifyMnResult = await verifyMn(table, mn)
        return verifyMnResult

    } catch (err) {
        console.error(err.message)
        throw err
    }
}

async function verifyOwner(rpc, wss, owner) {
    try {
        const query1 = 'SELECT COUNT(*) AS rpcCount FROM rpc_mn WHERE mn = ?';
        const query2 = 'SELECT COUNT(*) AS wssCount FROM wss_mn WHERE mn = ?';
        const query3 = 'SELECT COUNT(*) AS rpcOwnerCount FROM rpc_mn WHERE mn = ? AND owner = ?';
        const query4 = 'SELECT COUNT(*) AS wssOwnerCount FROM wss_mn WHERE mn = ? AND owner = ?';

        const [rpcCountResult, wssCountResult, rpcOwnerCountResult, wssOwnerCountResult] = await Promise.all([
            dbGetAsync.call(db, query1, [rpc]),
            dbGetAsync.call(db, query2, [wss]),
            dbGetAsync.call(db, query3, [rpc, owner]),
            dbGetAsync.call(db, query4, [wss, owner])
        ]);

        let rpcVerify, wssVerify

        if (rpcCountResult.rpcCount === 0) {
            rpcVerify = 'rpcOwner.2'; // RPC missing from the database
        } else { 
            rpcVerify = rpcOwnerCountResult.rpcOwnerCount === 1 ? 'rpcOwner.1' : 'rpcOwner.2'; // RPC present and owner matches (1) or doesn't match (2)
        }

        if (wssCountResult.wssCount === 0) {
            wssVerify = 'wssOwner.2'; // Websocket missing from the database
        } else { 
            wssVerify = wssOwnerCountResult.wssOwnerCount === 1 ? 'wssOwner.1' : 'wssOwner.2'; // Websocket present and owner matches (1) or doesn't match (2)
        }

        return { rpc: rpcVerify, wss: wssVerify };
    } catch (err) {
        console.error(err.message);
        throw err;
    }
}

async function removeMn(RpcOrWss, mn, owner) {
    let table

    if (RpcOrWss === 'rpc') {
        table = 'rpc_mn'
    } else if (RpcOrWss === 'wss') {
        table = 'wss_mn'
    }

    try {
        const remove = db.prepare(`DELETE FROM ${table} WHERE mn = ? AND owner = ?`)
        remove.run(mn, owner)
        remove.finalize()

        let verifyMnResult = await verifyMn(table, mn)
        return verifyMnResult

    } catch (err) {
        console.error(err.message)
        throw err
    }
}

module.exports = {
    addAddress,
    removeAddress,
    verifyAddress,
    getAllRows,
    checkRpc,
    checkWss,
    checkMn,
    addMn,
    removeMn,
    verifyOwner
}
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./data/validators.db')

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

module.exports = {
    addAddress,
    removeAddress,
    verifyAddress,
    getAllRows,
    checkRpc,
    checkWss
}
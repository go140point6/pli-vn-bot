// services/ingestRun.js
const { getDb } = require('../db');
const db = getDb();

const insRun = db.prepare(`INSERT INTO ingest_runs (digest) VALUES (?)`);
const endRun = db.prepare(`UPDATE ingest_runs SET ended_at = CURRENT_TIMESTAMP WHERE id = ?`);

/**
 * Create/continue an ingest run, execute a worker, then finish if created here.
 *
 * @param {*} client - not used by the helper, but forwarded for symmetry
 * @param {number|null} runId - existing run id or null to create one
 * @param {string|null} labelDigest - label stored in ingest_runs.digest (e.g., 'datasource', 'oracle')
 * @param {(rid:number)=>Promise<any>} worker - body to execute (receives run id only)
 * @param {{label?: string}} opts - console label, defaults to 'datasource'
 * @returns {Promise<{result:any, runId:number}>}
 */
async function withRun(client, runId, labelDigest, worker, opts = {}) {
  const label = opts.label || 'datasource';

  // Normalize to a DB-bindable label string (or null)
  const digestForDb =
    labelDigest == null ? label :
    (typeof labelDigest === 'string' ? labelDigest : String(labelDigest));

  let rid = runId;
  let createdHere = false;

  if (!rid) {
    const info = insRun.run(digestForDb);
    rid = info.lastInsertRowid;
    createdHere = true;
    console.log(`${label} run started (run_id=${rid})`);
  }

  try {
    const result = await worker(rid);
    return { result, runId: rid };
  } finally {
    if (createdHere) {
      endRun.run(rid);
      console.log(`${label} run finished (run_id=${rid})`);
    }
  }
}

module.exports = { withRun };
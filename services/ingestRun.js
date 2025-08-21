// services/ingestRun.js
const { beginRunStmt, endRunStmt } = require('../db/statements');

function beginRun() {
  const info = beginRunStmt.run();
  const runId = info.lastInsertRowid;
  console.log(`▶️  datasource run started (run_id=${runId})`);
  return runId;
}
function endRun(runId) {
  endRunStmt.run(runId);
  console.log(`⏹️  datasource run finished (run_id=${runId})`);
}

// Generic run wrapper, with optional "after run" hook
async function withRun(client, runId, digest, fn, onAfterRun) {
  const haveRun = Number.isFinite(runId);
  const rid = haveRun ? runId : beginRun();
  try {
    return await fn(rid, digest);
  } finally {
    if (typeof onAfterRun === 'function') {
      try { await onAfterRun(client, digest); } catch (e) { console.error('afterRun error:', e.message); }
    }
    if (!haveRun) endRun(rid);
  }
}

module.exports = { beginRun, endRun, withRun };
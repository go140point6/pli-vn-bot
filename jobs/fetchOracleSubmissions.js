// jobs/fetchOracleSubmissions.js
// Pulls oracle submissions and ONLY writes them to oracle_price_snapshots.
// — No stall detection
// — No aggregation
// — No admin/owner DMs as part of oracle runs

require('dotenv').config();
const { ethers } = require('ethers');

const { withRun } = require('../services/ingestRun');
const { getDb } = require('../db');

// FluxAggregator ABI (v0.6)
const FluxABI = require('@goplugin/contracts/abi/v0.6/FluxAggregator.json');

const ORACLE_THROTTLE_MS = parseInt(process.env.ORACLE_THROTTLE_MS || '150', 10);
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

/** -------- Address helpers (tolerant, non-throwing) -------- */
function safeLower0x(input) {
  if (!input && input !== 0) return null;
  let a = String(input).trim();
  if (!a) return null;

  // xdc → 0x
  if (a.slice(0, 3).toLowerCase() === 'xdc') a = '0x' + a.slice(3);

  // Must be 0x + 40 hex
  if (a.slice(0, 2).toLowerCase() !== '0x') return null;
  const body = a.slice(2);
  if (body.length !== 40) return null;
  if (!/^[0-9a-fA-F]{40}$/.test(body)) return null;

  return '0x' + body.toLowerCase();
}

/** -------- RPC / provider -------- */
const providerCache = new Map();
function rpcUrlForChain(chain_id) {
  const key = `RPCURL_${chain_id}`;
  return process.env[key];
}
function getProvider(chain_id) {
  if (providerCache.has(chain_id)) return providerCache.get(chain_id);
  const url = rpcUrlForChain(chain_id);
  if (!url) throw new Error(`Missing RPCURL_${chain_id} in environment`);
  const p = new ethers.JsonRpcProvider(url);
  providerCache.set(chain_id, p);
  return p;
}

/** -------- Price scaling helpers -------- */
const decimalsCache = new Map();
async function getDecimals(contract) {
  const addr = safeLower0x(contract.address);
  if (addr && decimalsCache.has(addr)) return decimalsCache.get(addr);
  const d = await contract.decimals();
  const n = Number(d);
  if (addr) decimalsCache.set(addr, n);
  return n;
}
function scaleSubmission(submissionBN, decimals) {
  const asStr = submissionBN.toString();
  const sign = asStr.startsWith('-') ? -1 : 1;
  const abs = sign === -1 ? asStr.slice(1) : asStr;
  return sign * (Number(abs) / Math.pow(10, decimals));
}

/** -------- DB -------- */
const db = getDb();

const selActiveContracts = db.prepare(`
  SELECT chain_id, address AS contract_address, pair, base, quote
  FROM contracts
  WHERE active = 1
`);

const insSnapshotIfExists = db.prepare(`
  INSERT INTO oracle_price_snapshots
    (run_id, chain_id, contract_address, validator_address, price)
  SELECT ?, ?, ?, ?, ?
  WHERE EXISTS (SELECT 1 FROM contracts  c WHERE c.chain_id = ? AND c.address = ?)
    AND EXISTS (SELECT 1 FROM validators v WHERE v.chain_id = ? AND v.address = ?)
`);

/** -------- Core per-aggregator work -------- */
async function processAggregator({ rid, chain_id, contract_address }) {
  const provider = getProvider(chain_id);
  const contract = new ethers.Contract(contract_address, FluxABI, provider);

  const [roundData, oracles, decimals] = await Promise.all([
    contract.latestRoundData(),
    contract.getOracles(),
    getDecimals(contract),
  ]);

  // Query round: try to coerce latestRound to number; fallback to 0 (current)
  let queriedRound = 0;
  try {
    const lr = roundData.roundId;
    const asNum = typeof lr === 'bigint' ? Number(lr) : Number(lr);
    queriedRound = Number.isFinite(asNum) ? asNum : 0;
  } catch { /* noop */ }

  for (const oracleRaw of oracles) {
    const validator_address = safeLower0x(oracleRaw);
    if (!validator_address) {
      console.warn(`↪︎ skip oracle: invalid address "${oracleRaw}" from ${contract_address} (chain ${chain_id})`);
      continue;
    }

    try {
      const state = await contract.oracleRoundState(validator_address, queriedRound);
      const submission = state._latestSubmission ?? state.latestSubmission ?? state[1] ?? 0n;
      const price = scaleSubmission(submission, decimals);

      const info = insSnapshotIfExists.run(
        rid,
        chain_id,
        contract_address,
        validator_address,
        price,
        // WHERE EXISTS params:
        chain_id, contract_address,
        chain_id, validator_address
      );

      if (info.changes === 0) {
        console.warn(`↪︎ skipped snapshot: missing contract or validator FK (chain=${chain_id}, agg=${contract_address}, val=${validator_address})`);
      }
    } catch (e) {
      console.warn(`⚠️ oracleRoundState failed: ${validator_address} @ ${contract_address} (chain ${chain_id}): ${e.message}`);
    }

    if (ORACLE_THROTTLE_MS > 0) await sleep(ORACLE_THROTTLE_MS);
  }
}

/** -------- Batch over all active aggregators -------- */
async function _fetch(rid) {
  const rows = selActiveContracts.all();
  if (!rows.length) {
    console.log('ℹ️ No active contracts to ingest oracle submissions from.');
    return;
  }

  let ok = 0, fail = 0;
  for (const r of rows) {
    const chain_id = r.chain_id;
    const contract_address = safeLower0x(r.contract_address);
    if (!contract_address) {
      console.warn(`↪︎ skip aggregator: invalid contract_address "${r.contract_address}" (chain ${chain_id})`);
      fail++;
      continue;
    }

    try {
      await processAggregator({ rid, chain_id, contract_address });
      ok++;
    } catch (e) {
      console.error(`❌ Aggregator ingest failed for ${contract_address} @ chain ${chain_id}: ${e.message}`);
      fail++;
    }
  }

  console.log(`✅ Oracle ingest complete: ${ok} aggregator(s) processed, ${fail} failed.`);
}

/** -------- Public entry -------- */
async function fetchOracleSubmissions(client, runId = null) {
  await withRun(
    client,
    runId,
    'oracle',   // label stored in ingest_runs.digest
    async (rid) => {
      await _fetch(rid);
      // NOTE: no stall detection or DM dispatch in this stripped mode.
    },
    { label: 'oracle' }
  );

  console.log('🌀 All oracles fetched. Waiting for next cycle...');
}

module.exports = { fetchOracleSubmissions };

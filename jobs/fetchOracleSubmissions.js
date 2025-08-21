// jobs/fetchOracleSubmissions.js
// Reads each active FluxAggregator, grabs latestRound + getOracles +
// oracleRoundState(oracle, latestRound), scales by decimals, and writes
// to oracle_price_snapshots (with run_id if provided).
//
// Usage:
//   const { fetchOracleSubmissions } = require('./jobs/fetchOracleSubmissions');
//   await fetchOracleSubmissions(client /*optional*/, runId /*optional*/);

require('dotenv').config();
const { ethers } = require('ethers');
const path = require('path');

const { withRun } = require('../services/ingestRun'); // same helper you use for datasources
const { getDb } = require('../db');
const {
  selActiveContracts,
  insertOracleSnapshot,
} = require('../db/statements');

const { getProvider } = require('../utils/provider');

const ORACLE_THROTTLE_MS = parseInt(process.env.ORACLE_THROTTLE_MS || '150', 10);
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// FluxAggregator ABI (use your package)
const FluxABI = require('@goplugin/contracts/abi/v0.6/FluxAggregator.json');

const db = getDb();

// Simple per-contract decimals cache
const decimalsCache = new Map();

async function getDecimals(contract) {
  const addr = contract.address.toLowerCase();
  if (decimalsCache.has(addr)) return decimalsCache.get(addr);
  const d = await contract.decimals();
  decimalsCache.set(addr, Number(d));
  return Number(d);
}

function scaleSubmission(submissionBN, decimals) {
  // int256 -> number (scaled). Note: safe for typical price ranges.
  const asStr = submissionBN.toString();
  const sign = asStr.startsWith('-') ? -1 : 1;
  const abs = sign === -1 ? asStr.slice(1) : asStr;
  const scaled = Number(abs) / Math.pow(10, decimals);
  return sign * scaled;
}

async function processAggregator({ rid, chain_id, contract_address }) {
  const provider = getProvider(chain_id);
  const contract = new ethers.Contract(contract_address, FluxABI, provider);

  // Read basic info
  const [roundData, oracles, decimals] = await Promise.all([
    contract.latestRoundData(),
    contract.getOracles(),
    getDecimals(contract),
  ]);

  const latestRound = roundData.roundId; // uint80
  // Loop oracles and fetch their latest submission for this round
  for (const oracleAddrRaw of oracles) {
    const oracleAddr = oracleAddrRaw.toLowerCase();
    try {
      // oracleRoundState(oracle, _queriedRoundId)
      const state = await contract.oracleRoundState(oracleAddr, latestRound);
      const submission = state._latestSubmission; // int256
      // Some aggregators return 0 when not submitted; we still record it as 0
      const price = scaleSubmission(submission, decimals);

      insertOracleSnapshot.run(
        rid,                 // run_id
        chain_id,
        contract_address.toLowerCase(),
        oracleAddr,
        price
      );
    } catch (e) {
      console.warn(`⚠️ oracleRoundState failed: ${oracleAddr} @ ${contract_address} (chain ${chain_id}): ${e.message}`);
    }

    if (ORACLE_THROTTLE_MS > 0) {
      await sleep(ORACLE_THROTTLE_MS);
    }
  }
}

async function _fetch(client, runId) {
  const rows = selActiveContracts.all(); // {chain_id, contract_address, pair, base, quote}
  if (!rows.length) {
    console.log('ℹ️ No active contracts to ingest oracle submissions from.');
    return;
  }

  let ok = 0, fail = 0;
  for (const r of rows) {
    try {
      await processAggregator({ rid: runId, chain_id: r.chain_id, contract_address: r.contract_address });
      ok++;
    } catch (e) {
      console.error(`❌ Aggregator ingest failed for ${r.contract_address} @ chain ${r.chain_id}: ${e.message}`);
      fail++;
    }
  }
  console.log(`✅ Oracle ingest complete: ${ok} aggregator(s) processed, ${fail} failed.`);
}

/**
 * Public entry — uses withRun so you can pass an existing runId (to co-track
 * with datasource run) or let it create its own.
 */
async function fetchOracleSubmissions(client, runId = null, digest = null) {
  if (typeof withRun === 'function') {
    return withRun(client, runId, digest, async (rid /*, d */) => {
      await _fetch(client, rid);
    });
  }
  // Fallback: no withRun helper found — just do it without run encapsulation.
  await _fetch(client, runId);
}

module.exports = { fetchOracleSubmissions };

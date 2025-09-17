require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const { REST, Routes, Collection } = require('discord.js');
const { setPresence } = require('../services/setPresence');
const { checkBalances } = require('../services/checkBalances');
const { fetchAllDatasourcePrices } = require('../jobs/fetchDatasourcePrices');
const { fetchOracleSubmissions } = require('../jobs/fetchOracleSubmissions');

const INTERVAL_MS = parseInt(process.env.FETCH_INTERVAL_SEC || '270', 10) * 1000;              // DS→Oracle→Presence cadence
const BALANCE_INTERVAL_MS = parseInt(process.env.BALANCE_INTERVAL_HOURS || '6', 10) * 60 * 60 * 1000; // default 6h

async function onReady(client) {
  console.log(`Ready! Logged in as ${client.user.tag}`);
  client.commands = new Collection();

  // ----- Load / register slash commands -----
  const commands = [];
  const commandsPath = path.join(__dirname, '..', 'commands');
  const commandFiles = fs.readdirSync(commandsPath).filter(file => file.endsWith('.js'));

  for (const file of commandFiles) {
    const filePath = path.join(commandsPath, file);
    const command = require(filePath);
    if ('data' in command && 'execute' in command) {
      client.commands.set(command.data.name, command);
      commands.push(command.data.toJSON());
    } else {
      console.log(`[WARNING] The command at ${filePath} is missing a required "data" or "execute" property.`);
    }
  }

  const rest = new REST({ version: '10' }).setToken(process.env.BOT_TOKEN);
  (async () => {
    try {
      const data = await rest.put(
        Routes.applicationGuildCommands(process.env.CLIENT_ID, process.env.GUILD_ID),
        { body: commands },
      );
      console.log(`Successfully loaded ${data.length} application (/) commands.`);
    } catch (error) {
      console.error(error);
    }
  })();

  // ===== Helpers =====

  // Sequential, non-overlapping DS → Oracle → Presence pipeline
  async function runPipelineOnce() {
    const t0 = Date.now();
    console.log('▶️  Pipeline start: fetchAllDatasourcePrices → fetchOracleSubmissions → setPresence');

    try { await fetchAllDatasourcePrices(client); }
    catch (e) { console.error('❌ fetchAllDatasourcePrices failed:', e); }

    try { await fetchOracleSubmissions(client); }
    catch (e) { console.error('❌ fetchOracleSubmissions failed:', e); }

    try { await setPresence(client); }
    catch (e) { console.error('❌ setPresence failed:', e); }

    const elapsed = Date.now() - t0;
    console.log(`⏹️  Pipeline end (elapsed ${elapsed} ms)`);
    return elapsed;
  }

  function startSequentialPipeline(intervalMs) {
    let running = false;

    async function tick() {
      if (running) return; // guard
      running = true;
      try {
        const elapsed = await runPipelineOnce();
        const nextDelay = Math.max(0, intervalMs - elapsed);
        if (nextDelay === 0) {
          console.warn(`⏱️ Pipeline duration (${elapsed} ms) ≥ interval (${intervalMs} ms). Scheduling next immediately.`);
        }
        setTimeout(() => { running = false; tick(); }, nextDelay);
      } catch (err) {
        console.error('Unexpected pipeline error:', err);
        running = false;
        setTimeout(tick, intervalMs);
      }
    }

    tick(); // kick off
  }

  // Balance checks (separate cadence, never overlap with themselves)
  async function runBalancesOnce() {
    const t0 = Date.now();
    console.log('💰 Balance check start');
    try { await checkBalances(client); }
    catch (e) { console.error('❌ checkBalances failed:', e); }
    const elapsed = Date.now() - t0;
    console.log(`✅ Balance check end (elapsed ${elapsed} ms)`);
    return elapsed;
  }

  function startBalancesScheduler(intervalMs) {
    let running = false;

    async function tick() {
      if (running) return;
      running = true;
      try {
        await runBalancesOnce();
      } finally {
        running = false;
        setTimeout(tick, intervalMs);
      }
    }

    // We already run one immediately at startup (see below),
    // so schedule the NEXT one for +intervalMs.
    setTimeout(tick, intervalMs);
  }

  // ===== Startup order you requested =====

  // 1) Run balances immediately (so you can watch it at startup)
  await runBalancesOnce();

  // 2) After that completes, start the DS→Oracle→Presence pipeline
  startSequentialPipeline(INTERVAL_MS);

  // 3) Start the recurring balance scheduler (separate cadence)
  startBalancesScheduler(BALANCE_INTERVAL_MS);
}

module.exports = { onReady };

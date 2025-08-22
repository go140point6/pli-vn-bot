require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const { REST, Routes, Collection, ActivityType } = require('discord.js');
const { setPresence } = require('../services/setPresence');
const { checkBalances } = require('../services/checkBalances');
const { fetchAllDatasourcePrices } = require('../jobs/fetchDatasourcePrices');
const INTERVAL_MS = parseInt(process.env.FETCH_INTERVAL_SEC || '270', 10) * 1000;

async function onReady(client) {
  console.log(`Ready! Logged in as ${client.user.tag}`);
  client.commands = new Collection();

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

  // ⏱️ Recurring tasks

  // For number of active nodes, color and arrows for bot
  //await setPresence(client);
  //setInterval(() => setPresence(client), 2 * 60 * 1000); // every 5 min

  // Check each validator gas levels
  await checkBalances(client);
  //setInterval(() => checkBalances(client), 12 * 60 * 60 * 1000); // every 12 hours

  await fetchAllDatasourcePrices(client);
  // Every 2 Minutes for testing
  //setInterval(() => fetchAllDatasourcePrices(client), 2 * 60 * 1000);
  // Every X Minutes based on .env
  //setInterval(() => fetchAllDatasourcePrices(client), INTERVAL_MS);

  // This is an example of how to run a function based on a time value
  // In this example, getting XRP price and updating it every 5 minutes
  //getXRPToken(); 
  //setInterval(getXRPToken, Math.max(1, 5 || 1) * 60 * 1000);

  // async function getXRP() {
  //     await axios.get(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=ripple`).then(res => {
  //                if (res.data && res.data[0].current_price) {
  //                 const currentXRP = res.data[0].current_price.toFixed(4) || 0 
  //                 console.log("XRP current price: " + currentXRP);
  //                 module.exports.currentXRP = currentXRP;
  //             } else {
  //                 console.log("Error loading coin data")
  //             }
  //         }).catch(err => {
  //             console.log("An error with the Coin Gecko api call: ", err.response.status, err.response.statusText);
  //     });
  // };

  // async function getXRPToken() {
  //     await getXRP();
  // }
}

module.exports = { onReady };
require('dotenv').config();
// Node's native file system module. fs is used to read the commands directory and identify our command files.
const fs = require('node:fs');
// Node's native path utility module. path helps construct paths to access files and directories. One of the advantages of the path module is that it automatically detects the operating system and uses the appropriate joiners.
const path = require('node:path');
//const client = require('../index');
const { REST, Routes, Collection, ChannelType, ActivityType, MembershipScreeningFieldType } = require('discord.js');
const axios = require('axios');
const { getAddressBalance } = require('../main/getBalance');
const { getAllRows } = require('../main/dbOperations');
const { getPrices } = require('../utils/getPrices');
const { getNodes } = require('../utils/getNodes');
const { clearRoles, setRed, setGreen } = require('../utils/setRoles')

async function onReady(client) {
    console.log(`Ready! Logged in as ${client.user.tag}`)
    
    client.commands = new Collection();

    const commands = [];

    const commandsPath = path.join(__dirname, '..', 'commands');
    const commandFiles = fs.readdirSync(commandsPath).filter(file => file.endsWith('.js'));

    for (const file of commandFiles) {
        const filePath = path.join(commandsPath, file);
        const command = require(filePath);
        // Set a new item in the Collection with the key as the command name and the value as the exported module
        if ('data' in command && 'execute' in command) {
            client.commands.set(command.data.name, command);
            commands.push(command.data.toJSON());
        } else {
            console.log(`[WARNING] The command at ${filePath} is missing a required "data" or "execute" property.`);
        }
    }

    // Construct and prepare an instance of the REST module
    const rest = new REST({ version: '10' }).setToken(process.env.BOT_TOKEN);

    // and deploy your commands!
    (async () => {
	    try {
		    // The put method is used to fully refresh all commands in the guild with the current set.
		    const data = await rest.put(
			    Routes.applicationGuildCommands(
                    process.env.CLIENT_ID, 
                    process.env.GUILD_ID
                    ),
			    { body: commands },
		    );

		    console.log(`Successfully loaded ${data.length} application (/) commands.`);
	    } catch (error) {
		    // Catch and log any errors.
		    console.error(error);
	    }
    })();

    // This is an example of how to run a function based on a time value
    // In this example, getting XRP price and updating it every 5 minutes
    //getXRPToken(); 
    //setInterval(getXRPToken, Math.max(1, 5 || 1) * 60 * 1000);

    //const addressToCheck = "0xf87A639bCE2064aBA1833a2ADeB1caD5800b46bD"

    // let symbol = 'plugin'
    // let fixed = '4'
    // getPrices(symbol, fixed)
    // setInterval(() => {
    //     try {
    //         getPrices(symbol, fixed)
    //     } catch (error) {
    //         console.error('Error getting prices:', error)
    //     }
    // }, Math.max(1, 5 || 1) * 60 * 1000) // every 5 minutes

    async function checkBalance() {
        try {
            const balance = await getAddressBalance(addressToCheck);
            console.log(`Balance of ${addressToCheck}: ${balance} XDC`);
        } catch (error) {
            console.error('Error:', error);
        }
    }

    var lastPrice
    var arrow

    async function setPresence() {
        try {
            //let symbol = 'plugin'
            let fixed = '4'
            
            const up = "\u2B08"
            const down = "\u2B0A"
            const mid = "\u22EF"

            //const { currentPrice, priceChange } = await getPrices(symbol, fixed)
            const currentPrice = await getPrices(fixed)

            const guild = await client.guilds.cache.get(`${process.env.GUILD_ID}`)
            const member = await guild.members.cache.get(`${process.env.CLIENT_ID}`)

            if (typeof lastPrice === 'undefined') {
                await clearRoles(guild, member)
                arrow = mid
            } else if (currentPrice > lastPrice) {
                arrow = up
                await setGreen(guild, member)
            } else if (currentPrice < lastPrice) {
                arrow = down
                await setRed(guild, member)
            } else {
                // no change
            }
            
            lastPrice = currentPrice
            
            const nodeCount = await getNodes()
            //console.log(nodeCount)
            //console.log(client.user)
            member.setNickname(`PLI ${arrow} $${currentPrice}`)
            client.user.setPresence({
                activities: [{
                name: `v2.4 nodes: ${nodeCount}`,
                type: ActivityType.Watching
                }]
            })
        } catch (error) {
            console.error("Error setting presence:", error)
        }
    }


    //getNodes()
    setPresence()
    setInterval(setPresence, Math.max(1, 5 || 1) * 60 * 1000);

    //checkBalance()
    //setInterval(checkBalance, 60 * 1000)

    //setInterval(monitorAddressesThread, 60 * 1000)

}    


async function getXRP() {
    await axios.get(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=plugin`).then(res => {
               if (res.data && res.data[0].current_price) {
                const currentXRP = res.data[0].current_price.toFixed(4) || 0 
                console.log("XRP current price: " + currentXRP);
                module.exports.currentXRP = currentXRP;
            } else {
                console.log("Error loading coin data")
            }
            //return;
        }).catch(err => {
            console.log("An error with the Coin Gecko api call: ", err.response.status, err.response.statusText);
    });
};

// async function getXRPToken() {
//     await getXRP();
// }

module.exports = { 
    onReady
}

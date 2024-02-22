const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');
const client = require('../index');
const { testWebSocketReachability, testWSSBlockchainSync } = require('../main/wssCheck');
const { checkWss,  } = require('../main/dbOperations');

// const embed = new EmbedBuilder()
// 	.setTitle('check-wss')
// 	.setDescription('health check the wss nodes in my databanks.')

module.exports = {
	data: new SlashCommandBuilder()
		.setName('check-public-wss')
		.setDescription('Health check the public wss nodes in my databanks.'),
		async execute(interaction) {
			try {
				await interaction.deferReply()
				await initialEmbed(interaction)
			} catch (error) {
				console.error(error.message)
				await interaction.reply("An error occurred while processing the command.")
				}
		}
};

async function embedCombined(interaction, setDesc, setFields) {
	const embedCombined = new EmbedBuilder()
	.setColor('DarkRed')
	.setTitle(`Welcome to The Terminal`)
	//.setAuthor({ name: client.user.username })
	.setDescription(setDesc)
	.setThumbnail(client.user.avatarURL())
	.addFields(setFields)
	//.setImage('https://onxrp-marketplace.s3.us-east-2.amazonaws.com/nft-images/00081AF4B6C6354AE81B765895498071D5E681DB44D3DE8F1589271700000598-32c83d6e902f8.png')
	.setTimestamp()
	//.setFooter({ text: `${address}` })

	return embedCombined
}

async function initialEmbed(interaction) {
	try {
		const results = await initialCheck()
		//console.log(results)

		let setDesc = 'This is a health check of the following public WSS:'
		let setFields = results.map(({ wssURL, result }) => {
			if (result !== undefined) {
				if (result.error !== undefined) {
					return {
						name: `MN: ${wssURL}`,
						value: `:warning: Error reaching WSS endpoint: ${result.error}`
					};
				} else if (result.syncing !== undefined && result.syncing !== false) {
					return {
						name: `MN: ${wssURL}`,
						value: `:x: WSS ${wssURL} is reachable but not fully synchronized, currentBlock: ${result.syncing.currentBlock} highestBlock: ${result.syncing.highestBlock}`
					};
				} else {
					return {
						name: `MN: ${wssURL}`,
						value: `:white_check_mark: WSS is reachable and fully synchronized.`
					};
				}
			} else {
				// Check for invalid JSON response
				if (typeof result === 'string' && result.includes('Invalid JSON')) {
					return {
						name: `MN: ${wssURL}`,
						value: `:warning: Error reaching WSS endpoint: Invalid JSON WSS response`
					}
				} else {
					return {
						name: `MN: ${wssURL}`,
						value: `:warning: Error reaching WSS endpoint: Unknown error`
					}
				}
			}
		})

	const embedCombinedInitial = await embedCombined(interaction, setDesc, setFields)
		
	await interaction.editReply({ embeds: [embedCombinedInitial], components: [] })
	} catch (error) {
		console.log(error)
	}
}

async function initialCheck() {
	// get Wss list from DB
	const wssList = await checkWss(true)
	const results = []
	//console.log("wsslist :", wssList)
	for (const wss of wssList) {
		try {
		  const result = await testWSSBlockchainSync(wss.mn)
		  //console.log(`WSS ${wss.mn} is reachable. Latest block: ${result}`)
		  results.push({ wssURL: wss.mn, result })
		} catch (error) {
			//console.error(`Error processing ${wss.mn}:`, error.message)
			results.push({ wssURL: wss.mn, error: error.message })
		}
	}
	return results
}
const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');
const client = require('../index');
const { testRPCReachability, testRPCBlockchainSync, testLatestBlockNumber } = require('../main/rpcCheck');
const { checkRpc } = require('../main/dbOperations');

// const embed = new EmbedBuilder()
// 	.setTitle('check-rpc')
// 	.setDescription('health check the rpc nodes in my databanks.')

module.exports = {
	data: new SlashCommandBuilder()
		.setName('check-public-rpc')
		.setDescription('Health check the public RPC nodes in my databanks.'),
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

		let setDesc = 'This is a health check of the following public RPC:'
		let setFields = results.map(({ rpcURL, result }) => {
			if (result !== undefined) {
				if (result.error !== undefined) {
					return {
						name: `MN: ${rpcURL}`,
						value: `:warning: Error reaching RPC endpoint: ${result.error}`
					};
				} else if (result.syncing !== undefined && result.syncing !== false) {
					return {
						name: `MN: ${rpcURL}`,
						value: `:x: RPC ${rpcURL} is reachable but not fully synchronized, currentBlock: ${result.syncing.currentBlock} highestBlock: ${result.syncing.highestBlock}`
					};
				} else {
					return {
						name: `MN: ${rpcURL}`,
						value: `:white_check_mark: RPC is reachable and fully synchronized.`
					};
				}
			} else {
				// Check for invalid JSON response
				if (typeof result === 'string' && result.includes('Invalid JSON')) {
					return {
						name: `MN: ${rpcURL}`,
						value: `:warning: Error reaching RPC endpoint: Invalid JSON RPC response`
					}
				} else {
					return {
						name: `MN: ${rpcURL}`,
						value: `:warning: Error reaching RPC endpoint: Unknown error`
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
	const rpcList = await checkRpc(true)
	const results = []
	//console.log(rpcList)
	for (const rpc of rpcList) {
		try {
		  //const result = await testRPCReachability(rpc.mn)
		  const result = await testRPCBlockchainSync(rpc.mn)
		  //console.log("result: ", result)
		  if (result.syncing === false) {
		  	//console.log(`RPC ${rpc.mn} is reachable and fully synchronized.`)
		  } else {
			//console.log(`RPC ${rpc.mn} is reachable but not fully synchronized, currentBlock: ${result.syncing.currentBlock} highestBlock: ${result.syncing.highestBlock}`)
		  }
		  results.push({ rpcURL: rpc.mn, result })
		} catch (error) {
			//console.error(`Error processing ${rpc.mn}:`, error.message)
			results.push({ rpcURL: rpc.mn, error: error.message })
		}
	}
	return results
}

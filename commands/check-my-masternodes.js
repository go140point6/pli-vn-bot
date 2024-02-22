const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');
const client = require('../index');
const { testRPCReachability, testRPCBlockchainSync, testLatestBlockNumber } = require('../main/rpcCheck');
const { testWebSocketReachability, testWSSBlockchainSync } = require('../main/wssCheck');
const { checkWss, checkRpc } = require('../main/dbOperations');

// const embed = new EmbedBuilder()
// 	.setTitle('my-MN')
// 	.setDescription('health check my own MN.')

module.exports = {
	data: new SlashCommandBuilder()
		.setName('my-masternodes')
		.setDescription('Health check your own mainnet and apothem masternodes.'),
		async execute(interaction, inProgress) {
			console.log(inProgress)
			try {
				if (inProgress.has(interaction.user.id)) {
					console.log(inProgress)
						await interaction.reply({ content: "Do not run commands until the last one had been completed or canceled. You have been warned.", ephemeral: true })	
					return
				} 
				console.log(inProgress)
				//await interaction.deferReply()
				await initialEmbed(interaction, inProgress)
			} catch (error) {
				console.error(error.message)
				await interaction.reply("An error occurred while processing the command.")
				}
		}
};

async function embedCombined(interaction, setDesc, setFields) {
	const embedCombined = new EmbedBuilder()
	.setColor('DarkRed')
	.setTitle(`Welcome to Plugin Bot`)
	//.setAuthor({ name: client.user.username })
	.setDescription(setDesc)
	.setThumbnail(client.user.avatarURL())
	.addFields(setFields)
	//.setImage('https://onxrp-marketplace.s3.us-east-2.amazonaws.com/nft-images/00081AF4B6C6354AE81B765895498071D5E681DB44D3DE8F1589271700000598-32c83d6e902f8.png')
	.setTimestamp()
	//.setFooter({ text: `${address}` })

	return embedCombined
}

async function initialEmbed(interaction, inProgress) {
	try {
		//await interaction.deferReply()
		const { rpcResults, wssResults } = await initialCheck(interaction)
		console.log(rpcResults)
		console.log(wssResults)
		
		let setDesc = 'This is a health check of your masternodes:'
		let setFields = []

		rpcResults.forEach(({ rpcURL, rpcResult }) => {
			if (rpcResult !== undefined) {
				if (rpcResult.error !== undefined) {
					setFields.push({
						name: `MN: ${rpcURL}`,
						value: `:warning: Error reaching RPC endpoint: ${rpcResult.error}`
					})
				} else if (rpcResult.syncing !== undefined && rpcResult.syncing !== false) {
					setFields.push({
						name: `MN: ${rpcURL}`,
						value: `:x: RPC ${rpcURL} is reachable but not fully synchronized, currentBlock: ${rpcResult.syncing.currentBlock} highestBlock: ${rpcResult.syncing.highestBlock}`
					})
				} else {
					setFields.push({
						name: `MN: ${rpcURL}`,
						value: `:white_check_mark: RPC is reachable and fully synchronized.`
					})
				}
			} else {
				setFields.push({
					name: `MN: ${rpcURL}`,
					value: `:warning: Error reaching RPC endpoint: Unknown error`
				})
			}
		})

		wssResults.forEach(({ wssURL, wssResult }) => {
			if (wssResult !== undefined) {
				if (wssResult.error !== undefined) {
					setFields.push({
						name: `MN: ${wssURL}`,
						value: `:warning: Error reaching WSS endpoint: ${wssResult.error}`
					})
				} else if (wssResult.syncing !== undefined && wssResult.syncing !== false) {
					setFields.push({
						name: `MN: ${wssURL}`,
						value: `:x: WSS ${wssURL} is reachable but not fully synchronized, currentBlock: ${wssResult.syncing.currentBlock} highestBlock: ${wssResult.syncing.highestBlock}`
					})
				} else {
					setFields.push({
						name: `MN: ${wssURL}`,
						value: `:white_check_mark: WSS is reachable and fully synchronized.`
					})
				}
			} else {
				setFields.push({
					name: `MN: ${wssURL}`,
					value: `:warning: Error reaching WSS endpoint: Unknown error`
				})
			}
		})

		if (setFields.length === 0) {
			setFields.push({
				name: 'You have no masternode in my databanks.',
				value: 'Add your masternode(s) using the /edit-masternode command.'
			})
		}

	const embedCombinedInitial = await embedCombined(interaction, setDesc, setFields)
		
	await interaction.reply({ embeds: [embedCombinedInitial], ephemeral: true })
	} catch (error) {
		console.log(error)
	}
}

async function initialCheck(interaction) {
	//console.log(interaction.user.id)
	let userId = interaction.user.id
	const rpcList = await checkRpc(false, userId)
	//console.log(rpcList)
	const wssList = await checkWss(false, userId)
	//console.log(wssList)
	const rpcResults = []
	const wssResults = []
	for (const rpc of rpcList) {
		try {
		  const rpcResult = await testRPCBlockchainSync(rpc.mn)
		  rpcResults.push({ rpcURL: rpc.mn, rpcResult})
		} catch (error) {
			rpcResults.push({ wssURL: rpc.mn, error: error.message })
		}
	}
	//console.log(rpcResults)
	for (const wss of wssList) {
		try {
		  const wssResult = await testWSSBlockchainSync(wss.mn)
		  //console.log(`WSS ${wss.mn} is reachable. Latest block: ${result}`)
		  wssResults.push({ wssURL: wss.mn, wssResult })
		} catch (error) {
			//console.error(`Error processing ${wss.mn}:`, error.message)
			wssResults.push({ wssURL: wss.mn, error: error.message })
		}
	}
	//console.log(wssResults)
	return { rpcResults, wssResults }
}
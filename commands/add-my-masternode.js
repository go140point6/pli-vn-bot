const { 
	SlashCommandBuilder, 
	EmbedBuilder, 
	ModalBuilder, 
	TextInputBuilder, 
	ActionRowBuilder, 
	TextInputStyle, 
	ButtonBuilder, 
	ButtonStyle, 
	ComponentType, 
	InteractionCollector } = require('discord.js');
const client = require('../index');
const { checkMn, addMn, removeMn, verifyOwner } = require('../main/dbOperations');

const modal = new ModalBuilder()
	.setCustomId('mn-modal')
	.setTitle('Add Masternode')

	// Create the text input components
	const rpcInput = new TextInputBuilder()
		.setCustomId('rpcInput')
		// The label is the prompt the user sees for this input
		.setLabel("Enter your masternode RPC:")
		// Short means only a single line of text
		.setStyle(TextInputStyle.Short);

	const wssInput = new TextInputBuilder()
		.setCustomId('wssInput')
		.setLabel("Enter your masternode WS:")
		// Paragraph means multiple lines of text.
		.setStyle(TextInputStyle.Short);

	// An action row only holds one text input,
	// so you need one action row per text input.
	const firstActionRow = new ActionRowBuilder().addComponents(rpcInput);
	const secondActionRow = new ActionRowBuilder().addComponents(wssInput);

	// Add inputs to the modal
	modal.addComponents(firstActionRow, secondActionRow);

module.exports = {
	data: new SlashCommandBuilder()
		.setName('edit-masternode')
		.setDescription('Edit your masternode RPC and Websocket.'),
	
		async execute(interaction) {
			try {
				await interaction.showModal(modal)
				await initialMessage(interaction)
			} catch (error) {
				console.error(error.message)
				await interaction.reply("An error occurred while processing the command.")
			}
		}
}

async function embedCombined(setDesc, setFields) {
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

async function initialMessage(interaction) {
	try {
		const modalResponse = await interaction.awaitModalSubmit({
			filter: (i) =>
			i.customId === 'mn-modal' && i.user.id === interaction.user.id,
			time: 60000,
		})

		let owner = interaction.user.id
		let rpc
		let wss

		if (modalResponse.isModalSubmit()) {
			rpc = modalResponse.fields.getTextInputValue('rpcInput')
			wss = modalResponse.fields.getTextInputValue('wssInput')

			// Regex validation
			const validationResults = await validateResponses(rpc, wss)

			if (!validationResults.rpc == true || !validationResults.wss == true) {
				const embedDescFieldsResults = await embedDescFields(validationResults, rpc, wss)

				const embedCombinedValidation = await embedCombined(embedDescFieldsResults.desc, embedDescFieldsResults.fields)
				await modalResponse.reply({ embeds: [embedCombinedValidation] })
				return
			} 

			// checkMnResults will have 0 (missing) or 1 (present) for rpc or wss
			const checkMnResults = await checkMn(rpc, wss)

			// Verify ownership of masternodes
			if (checkMnResults.rpc == 'rpc.1' || checkMnResults.wss == 'wss.1') {
				const verifyOwnerResults = await verifyOwner(rpc, wss, owner)
				//console.log(verifyOwnerResults.rpc)
				//console.log(verifyOwnerResults.wss)
				//console.log(verifyOwnerResults)
				
				if (verifyOwnerResults.rpc == 'rpc.2' || verifyOwnerResults.wss == 'wss.2') {
					setDesc = `You are not the owner of one or both of these resources. Please check and run the command again.`
					setFields = [
						{ name: `RPC:`, value: `${rpc}` },
						{ name: `Websocket`, value: `${wss}` }
					] 

					const embedCombinedOwner = await embedCombined(setDesc, setFields)
					await modalResponse.reply({ embeds: [embedCombinedOwner], ephemeral: true })
					return
				}
			}

			const embedButtonsResults = await embedButtons(checkMnResults, rpc, wss)

			const embedCombinedInitial = await embedCombined(embedButtonsResults.desc, embedButtonsResults.fields)
			let rpcButtonDisabled = false
			let wssButtonDisabled = false
			const row = await buildButtons(embedButtonsResults.buttons, rpcButtonDisabled, wssButtonDisabled)

			//await interaction.reply({ embeds: [embedCombinedInitial], ephemeral: true })
			await modalResponse.reply({ embeds: [embedCombinedInitial], components: [row], ephemeral: true })

			const collector = interaction.channel.createMessageComponentCollector({ componentType: ComponentType.Button, time: 20000 })

			collector.on('collect', async i => {
				await i.deferUpdate()

				let setDesc
				let setFields

				if (i.user.id === interaction.user.id && i.customId === 'rpc') {

					collector.resetTimer()
					const collectorCollectResults = await collectorCollect(checkMnResults.wss, rpc, wss)
					let mn = rpc
					if (checkMnResults.rpc == "rpc.0") {
						const verifyMnResult = await addMn('rpc', mn, owner)

						const checkMnResultsRecheck = await checkMn(rpc, wss)
						const collectorCollectResultsRecheck = await collectorCollect(checkMnResultsRecheck.wss, rpc, wss)

						setDesc = `You requested addition of RPC ${rpc}, this has been completed.`
						setFields = [
							{ name: 'RPC is now present in my databanks:', value: `:white_check_mark: ${rpc}` },
							{ name: `${collectorCollectResultsRecheck.rpcOrWssName}`, value: `${collectorCollectResultsRecheck.rpcOrWssValue}` }
						]

						const embedButtonsResults = await embedButtons(checkMnResultsRecheck, rpc, wss)
						const embedCombinedRpc = await embedCombined(setDesc, setFields)
						rpcButtonDisabled = true // Set rpcButtonDisabled to true, wssButtonDisable keeps current state (may be true or false)
						const row = await buildButtons(embedButtonsResults.buttons, rpcButtonDisabled, wssButtonDisabled)

						await modalResponse.editReply({ embeds: [embedCombinedRpc], components: [row], ephemeral: true })

					} else if (checkMnResults.rpc == "rpc.1") {
						const verifyMnResult = await removeMn('rpc', mn, owner)

						const checkMnResultsRecheck = await checkMn(rpc, wss)
						//console.log("MN recheck:", checkMnResultsRecheck.rpc)
						//console.log("MN recheck:", checkMnResultsRecheck.wss)
						const collectorCollectResultsRecheck = await collectorCollect(checkMnResultsRecheck.wss, rpc, wss)

						setDesc = `You requested removal of RPC ${rpc}, this has been completed.`
						setFields = [
							{ name: 'RPC no longer present in my databanks', value: `:x: ${rpc}` },
							{ name: `${collectorCollectResults.rpcOrWssName}`, value: `${collectorCollectResultsRecheck.rpcOrWssValue}` }
						]

						const embedButtonsResults = await embedButtons(checkMnResultsRecheck, rpc, wss)
						const embedCombinedRpc = await embedCombined(setDesc, setFields)
						rpcButtonDisabled = true // Set rpcButtonDisabled to true, wssButtonDisable keeps current state (may be true or false)
						const row = await buildButtons(embedButtonsResults.buttons, rpcButtonDisabled, wssButtonDisabled)

						await modalResponse.editReply({ embeds: [embedCombinedRpc], components: [row], ephemeral: true })

					}
					
				} else if (i.user.id === interaction.user.id && i.customId === 'wss') {
					collector.resetTimer()
					const collectorCollectResults = await collectorCollect(checkMnResults.rpc, rpc, wss)
					let mn = wss
					if (checkMnResults.wss == "wss.0") {
						const verifyMnResult = await addMn('wss', mn, owner)
						//console.log("true = mn was added, false = mn was removed:", verifyMnResult)

						const checkMnResultsRecheck = await checkMn(rpc, wss)
						//console.log("MN recheck:", checkMnResultsRecheck.rpc)
						//console.log("MN recheck:", checkMnResultsRecheck.wss)
						const collectorCollectResultsRecheck = await collectorCollect(checkMnResultsRecheck.rpc, rpc, wss)

						setDesc = `You requested addition of Websocket ${wss}, this has been completed.`
						setFields = [
							{ name: 'Websocket is now present in my databanks:', value: `:white_check_mark: ${wss}` },
							{ name: `${collectorCollectResultsRecheck.rpcOrWssName}`, value: `${collectorCollectResultsRecheck.rpcOrWssValue}` }
						]

						const embedButtonsResults = await embedButtons(checkMnResultsRecheck, rpc, wss)
						const embedCombinedWss = await embedCombined(setDesc, setFields)
						wssButtonDisabled = true // Set wssButtonDisabled to true, rpcButtonDisable keeps current state (may be true or false)
						const row = await buildButtons(embedButtonsResults.buttons, rpcButtonDisabled, wssButtonDisabled)

						await modalResponse.editReply({ embeds: [embedCombinedWss], components: [row], ephemeral: true })

					} else if (checkMnResults.wss == "wss.1") {
						const verifyMnResult = await removeMn('wss', mn, owner)
						//console.log("true = mn was added, false = mn was removed:", verifyMnResult)

						const checkMnResultsRecheck = await checkMn(rpc, wss)
						//console.log("MN recheck:", checkMnResultsRecheck.rpc)
						//console.log("MN recheck:", checkMnResultsRecheck.wss)
						const collectorCollectResultsRecheck = await collectorCollect(checkMnResultsRecheck.rpc, rpc, wss)

						setDesc = `You requested removal of Websocket ${wss}, this has been completed.`
						setFields = [
							{ name: 'Websocket no longer present in my databanks', value: `:x: ${wss}` },
							{ name: `${collectorCollectResultsRecheck.rpcOrWssName}`, value: `${collectorCollectResultsRecheck.rpcOrWssValue}` }
						]

						const embedButtonsResults = await embedButtons(checkMnResultsRecheck, rpc, wss)
						const embedCombinedWss = await embedCombined(setDesc, setFields)
						wssButtonDisabled = true // Set wssButtonDisabled to true, rpcButtonDisable keeps current state (may be true or false)
						const row = await buildButtons(embedButtonsResults.buttons, rpcButtonDisabled, wssButtonDisabled)

						await modalResponse.editReply({ embeds: [embedCombinedWss], components: [row], ephemeral: true })
					}

				} else if (i.user.id === interaction.user.id && i.customId === 'cancel') {
					collector.stop()
				}
			})
			
			collector.on('end', async (collected, reason) => {
				let setDesc = 'Operation cancelled or timed out, systems shutting down.'
				let setFields = []

				const embedCombinedShutdown = await embedCombined(setDesc, setFields)
				await modalResponse.editReply({ embeds: [embedCombinedShutdown], components: [] })
			})
		}
	} catch (error) {
		console.error(error)
	}
}

async function collectorCollect(rpcOrwss, rpc, wss) {
	let rpcOrWssName
	let rpcOrWssValue

	// When the RPC button is clicked
		if (rpcOrwss == "wss.1") {
			rpcOrWssName = 'Websocket is present in my databanks:'
			rpcOrWssValue = `:white_check_mark: ${wss}`
		} else if (rpcOrwss == "wss.0") {
			rpcOrWssName = 'Websocket not present in my databanks:'
			rpcOrWssValue = `:x: ${wss}`
		// When the Websocket button is clicked
		} else if (rpcOrwss == "rpc.1") {
			rpcOrWssName = 'RPC is present in my databanks:'
			rpcOrWssValue = `:white_check_mark: ${rpc}`
		} else if (rpcOrwss == "rpc.0") {
			rpcOrWssName = 'RPC not present in my databanks:'
			rpcOrWssValue = `:x: ${rpc}`
		}

	return {
		rpcOrWssName,
		rpcOrWssValue
	}
}

async function validateResponses(rpc, wss) {
	const rpcRegex = /^(http|https):\/\/(?:[\w-]+\.)+[\w-]+(?::\d+)?(?:\/\S*)?$/
    const wssRegex = /^(ws|wss):\/\/(?:[\w-]+\.)+[\w-]+(?::\d+)?(?:\/\S*)?$/

	// Validate rpc and wss using regular expressions
    const rpcIsValid = rpcRegex.test(rpc)
    const wssIsValid = wssRegex.test(wss)

	// Return validation result
	return {
		rpc: rpcIsValid,
		wss: wssIsValid
	}
}

async function buildButtons(buttons, rpcButtonDisabled, wssButtonDisabled) {

	//console.log(buttons)

	const row = new ActionRowBuilder()
	.addComponents(
		new ButtonBuilder()
			.setCustomId('rpc')
			.setLabel(buttons['rpc'].label)
			.setStyle(buttons['rpc'].style)
			.setDisabled(rpcButtonDisabled),
		new ButtonBuilder()
			.setCustomId('wss')
			.setLabel(buttons['wss'].label)
			.setStyle(buttons['wss'].style)
			.setDisabled(wssButtonDisabled),
		new ButtonBuilder()
			.setCustomId('cancel')
			.setLabel('Cancel')
			.setStyle(ButtonStyle.Secondary)
	)
	//console.log(row)
	return row
}

async function embedDescFields(validationResults, rpc, wss){
	let setDesc = "You requested to add RPC and Websocket:"
	let setFields = []

	if (!validationResults.rpc || !validationResults.wss) {
		setDesc = "Something is wrong with one or both of your inputs. Please correct and run the command again."
		
		if (!validationResults.rpc) {
			setFields.push({
				name: 'RPC:',
				value: `:x: RPC ${rpc} should start with http or https and be a valid URI.`
			})
		} else {
			setFields.push({
				name: 'RPC:',
				value: `:white_check_mark: RPC ${rpc} is a correctly formatted URI.`
			})
		}
		
		if (!validationResults.wss) {
			setFields.push({
				name: 'WSS:',
				value: `:x: Websocket ${wss} should start with ws or wss and be a valid URI.`
			})
		} else {
			setFields.push({
				name: 'WSS:',
				value: `:white_check_mark: Websocket ${wss} is a correctly formatted URI.`
			})
		}
	}
	return { 
		desc: setDesc, 
		fields: setFields 
	}
}

async function embedButtons(checkMnResults, rpc, wss) {
	let setDesc = "You requested to add RPC and Websocket:"
	let setFields = []
	let setButtons = {}
	let rpcAdd
	let wssAdd

	if (checkMnResults.rpc == "rpc.1") {
		setFields.push({
			name: 'RPC already present in my databanks:',
			value: `:white_check_mark: ${rpc}`
		})
		setButtons.rpc = {
			label: 'Remove RPC',
			style: ButtonStyle.Danger,
		}
		rpcAdd = 0 // Remove the RPC MN
	} else if (checkMnResults.rpc == "rpc.0") {
		setFields.push({
			name: 'RPC not present in my databanks:',
			value: `:x: ${rpc}`
		})
		setButtons.rpc = {
			label: 'Add RPC',
			style: ButtonStyle.Success
		}
		rpcAdd = 1 // Add the RPC MN
	}

	if (checkMnResults.wss == "wss.1") {
		setFields.push({
			name: 'Websocket already present in my databanks:',
			value: `:white_check_mark: ${wss}`
		})
		setButtons.wss = {
			label: 'Remove Websocket',
			style: ButtonStyle.Danger
		}
		wssAdd = 0 // Remove the WSS MN
	} else if (checkMnResults.wss == "wss.0") {
		setFields.push({
			name: 'Websocket not present in my databanks:',
			value: `:x: ${wss}`
		})
		setButtons.wss = {
			label: 'Add Websocket',
			style: ButtonStyle.Success
		}
		wssAdd = 1 // Add the WSS MN
	}

	return { 
		desc: setDesc, 
		fields: setFields,
		buttons: setButtons,
		rpcAdd: rpcAdd,
		wssAdd: wssAdd
	}
}
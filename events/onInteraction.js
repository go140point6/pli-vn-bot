const inProgress = new Set()

async function onInteraction(interaction) {
    if (interaction.isChatInputCommand()) {
        const command = interaction.client.commands.get(interaction.commandName);
        await command.execute(interaction, inProgress);
    } else if (interaction.isButton()) {
        //console.log(interaction);
    } else {
        return;
    }
};

module.exports = { 
    onInteraction
}
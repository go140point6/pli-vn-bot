const { SlashCommandBuilder } = require('discord.js');
const { MessageFlags } = require('discord-api-types/v10');
const Xdc3 = require('xdc3');
const FluxABI = require('@goplugin/contracts/abi/v0.6/FluxAggregator.json');
const Database = require('better-sqlite3');
const path = require('path');

const xdc3 = new Xdc3(new Xdc3.providers.HttpProvider(process.env.RPCURL));
const db = new Database(path.join(__dirname, '../data/validators.db'));

module.exports = {
  data: new SlashCommandBuilder()
    .setName('my-validator')
    .setDescription('Check statistics on your validator'),

  async execute(interaction) {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const discordId = interaction.user.id;

    // Get node address from validators table
    const validator = db.prepare(`
      SELECT address FROM validators WHERE discord_id = ?
    `).get(discordId);

    if (!validator) {
      return interaction.editReply(`❌ Your Discord ID is **not** an active validator.`);
    }

    const nodeAddr = validator.address;
    if (!nodeAddr || nodeAddr.trim() === '') {
      return interaction.editReply(`⚠️ You are a validator, but there is no record of your node address.`);
    }

    // Get all active contracts
    const contracts = db.prepare(`
      SELECT address, pair FROM contracts WHERE active = 1
    `).all();

    const results = [];
    let balanceInXDC = '0';

    try {
      const balance = await xdc3.eth.getBalance(nodeAddr);
      balanceInXDC = xdc3.utils.fromWei(balance, 'ether');
    } catch (error) {
      console.error('Error fetching balance:', error.message);
    }

    for (const { address: contractAddr, pair } of contracts) {
      try {
        const contract = new xdc3.eth.Contract(FluxABI, contractAddr);
        const oracles = await contract.methods.getOracles().call();

        if (!oracles.some(o => o.toLowerCase() === nodeAddr.toLowerCase())) {
          continue;
        }

        const raw = await contract.methods.withdrawablePayment(nodeAddr).call();
        const pli = xdc3.utils.fromWei(raw, 'ether');

        results.push(`✅ **${pair}** → **${pli} PLI**`);
      } catch (err) {
        console.error(`Error with contract ${contractAddr}:`, err.message);
        results.push(`⚠️ **${pair || contractAddr}** → Error: ${err.message}`);
      }
    }

    if (results.length === 0) {
      return interaction.editReply(`😕 Your node address is not listed on any active contracts.`);
    }

    await interaction.editReply(
      `📊 Withdrawable payments for your node:\n **${nodeAddr}**\n\n` +
      results.join('\n') +
      '\n\n' +
      `⛽ Current XDC gas balance:\n💰 **${balanceInXDC} XDC**`
    );

    // DM confirmation logic using DB
    const user = db.prepare(`SELECT accepts_dm FROM users WHERE discord_id = ?`).get(discordId);
    const canDM = user?.accepts_dm === 1;

    if (!canDM) {
      try {
        await interaction.user.send(
          '👍 I can DM you! You’ll receive low gas and other alerts here.'
        );

        // Update accepts_dm to 1
        db.prepare(`UPDATE users SET accepts_dm = 1 WHERE discord_id = ?`).run(discordId);
        console.log(`✅ DMs enabled and confirmed for ${discordId}`);
      } catch (dmError) {
        console.warn(`❌ Could not DM user ${discordId}:`, dmError.message);

        await interaction.followUp({
          content:
            "⚠️ I wasn't able to send you a DM. Please enable DMs from server members in **User Settings → Privacy & Safety** if you'd like to receive alerts.",
          ephemeral: true,
        });

        // Re-confirm that DMs are disabled
        db.prepare(`UPDATE users SET accepts_dm = 0 WHERE discord_id = ?`).run(discordId);
      }
    }
  }
};
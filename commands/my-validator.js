// commands/my-validator.js
const { SlashCommandBuilder } = require('discord.js');
const { MessageFlags } = require('discord-api-types/v10');
const { JsonRpcProvider, Contract, formatEther } = require('ethers');
const FluxABI = require('@goplugin/contracts/abi/v0.6/FluxAggregator.json');
const { getDb } = require('../db'); // singleton DB from db/index.js

// --- Provider (ethers v6) ---
// Prefer XDC_RPC_HTTP; fall back to legacy RPCURL_50 for compatibility.
const RPC_URL = process.env.XDC_RPC_HTTP || process.env.RPCURL_50;
if (!RPC_URL) {
  // Fail fast so you notice the misconfig immediately.
  throw new Error('Missing RPC URL. Set XDC_RPC_HTTP (preferred) or RPCURL_50 in your .env');
}
const provider = new JsonRpcProvider(RPC_URL, { name: 'xdc', chainId: 50 });

// Small helper: show pair if available, else base/quote, else address
function labelFor(pair, base, quote, addr) {
  if (pair && String(pair).trim() !== '') return pair;
  if (base && quote) return `${base}/${quote}`;
  return addr;
}

const db = getDb();

// Prepared statements
const selValidatorAddr = db.prepare(`SELECT address FROM validators WHERE discord_id = ?`);
const selActiveContracts = db.prepare(`SELECT address, pair, base, quote FROM contracts WHERE active = 1`);
const selUserDM = db.prepare(`SELECT accepts_dm FROM users WHERE discord_id = ?`);
const setDM = db.prepare(`UPDATE users SET accepts_dm = ? WHERE discord_id = ?`);

module.exports = {
  data: new SlashCommandBuilder()
    .setName('my-validator')
    .setDescription('Check statistics on your validator'),

  async execute(interaction) {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const discordId = interaction.user.id;

    // Get node address from validators table (stored canonical 0x lowercase)
    const validator = selValidatorAddr.get(discordId);
    if (!validator) {
      return interaction.editReply(`❌ Your Discord ID is **not** an active validator.`);
    }

    const nodeAddr = validator.address;
    if (!nodeAddr || nodeAddr.trim() === '') {
      return interaction.editReply(`⚠️ You are a validator, but there is no record of your node address.`);
    }

    // Active contracts
    const contracts = selActiveContracts.all();

    const results = [];
    let balanceInXDC = '0';

    try {
      const balanceWei = await provider.getBalance(nodeAddr);
      balanceInXDC = formatEther(balanceWei);
    } catch (error) {
      console.error('Error fetching balance:', error.message);
    }

    for (const { address: contractAddr, pair, base, quote } of contracts) {
      const display = labelFor(pair, base, quote, contractAddr);
      try {
        const contract = new Contract(contractAddr, FluxABI, provider);

        const oracles = await contract.getOracles(); // array<string>
        const isParticipating = oracles.some(o => String(o).toLowerCase() === nodeAddr.toLowerCase());
        if (!isParticipating) continue;

        // withdrawablePayment(address oracle) -> uint256 (18 decimals for PLI)
        const raw = await contract.withdrawablePayment(nodeAddr);
        const pli = formatEther(raw);

        results.push(`✅ **${display}** → **${pli} PLI**`);
      } catch (err) {
        console.error(`Error with contract ${contractAddr}:`, err.message);
        results.push(`⚠️ **${display}** → Error: ${err.message}`);
      }
    }

    if (results.length === 0) {
      return interaction.editReply(`😕 Your node address is not listed on any active contracts.`);
    }

    await interaction.editReply(
      `📊 Withdrawable payments for your node:\n**${nodeAddr}**\n\n` +
      results.join('\n') +
      '\n\n' +
      `⛽ Current XDC gas balance:\n💰 **${balanceInXDC} XDC**`
    );

    // DM confirmation logic
    const user = selUserDM.get(discordId);
    const canDM = user?.accepts_dm === 1;

    if (!canDM) {
      try {
        await interaction.user.send(
          '👍 I can DM you! You’ll receive low gas and other alerts here.'
        );
        setDM.run(1, discordId);
        console.log(`✅ DMs enabled and confirmed for ${discordId}`);
      } catch (dmError) {
        console.warn(`❌ Could not DM user ${discordId}:`, dmError.message);

        await interaction.followUp({
          content:
            "⚠️ I wasn't able to send you a DM. Please enable DMs from server members in **User Settings → Privacy & Safety** if you'd like to receive alerts.",
          ephemeral: true,
        });

        setDM.run(0, discordId);
      }
    }
  }
};
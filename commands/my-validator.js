const { SlashCommandBuilder } = require('discord.js');
const { MessageFlags } = require('discord-api-types/v10');
const Xdc3 = require('xdc3');
const FluxABI = require('@goplugin/contracts/abi/v0.6/FluxAggregator.json');

const xdc3 = new Xdc3(new Xdc3.providers.HttpProvider(process.env.RPCURL));

//const CONTRACT_ADDR = '0x6Fb5E127F59f49c848CC204A9a9a575CA52C1cD3';
//const CONTRACT_ADDR = '0xEe28565FF7583dcB188554BE434665b4235fBCDb';
//const NODE_ADDR     = '0xf87A639bCE2064aBA1833a2ADeB1caD5800b46bD';

// Map of Discord user IDs → Node addresses
const USER_NODE_MAP = {
  '852173737901817906': '0xef3eDD2A4bc9f0B89d85A9F864861367724b5677',
  '879268108471246849': '0x80a173Bf40399ea6FF73a4d7840097CA936eB9E7',
  '779370797264273479': '0x40b66A878B4ED273d8DEc50baf8C94180A68A317',
  '567425551229386758': '0xf87A639bCE2064aBA1833a2ADeB1caD5800b46bD',
  '921461299433582592': '',
  '372291963111604224': '0x8Cbe01A3fDD4b5f46ae0F458DBC81A8942014D91',
  '891449142881185823': '0xb4c1d912e72a73CE9Db619Ebf85f362B2afFf75E',
  '862957519503753226': '0xC7df2F7c7b3BF1306b4c9E9eD7fEd17fF616DCC8',
  '1206088204839551067': '0xC95e8E2e5c352804e4A92c9d65550e00080e7219',
  '835144024105025597': '0x863a5e49f36141aB82a33a5CF1B80a0A71Ab382f',
  '852434672793288724': '0x844164dFCCEa1dE1FC7fcf8b2Af7466869714AE4',
  '868345686750547968': '0xF2fa19442a0F0321d89ed82C92dE6b6Fe94B6803',
  '894779081638432778': '0xB58025956dc1E3DB04dCB33AB8F3Ff8BF47D6B95',
  '874715940976226376': '0x1BB113F429Fd87A37e10008394BDe5Bd50EcDE47',
  '934254613270523994': '0x1B114bB44C7E138d65A1740D73edBDf49ca89daF',
  '566025247783125022': '0xc18667740E58E856C91e3D7Cefc642A995037d99',
  '409851282438881280': '0x91D3F0b746bEbfbCA9c613F8C77d6271A3cd7190',
  '1058071222396649473': '0x95e2f7a41eB04FBadDe4550Aa12B5e014a402F66',
  '911654996393726004': '0x155e87c1CCC244d6b517Ff14e94f4612982e9538',
};

// List of 11 FluxAggregator contract addresses
const CONTRACTS = [
  '0x4Ef427Fb5dD965107b4A2Bd23da7e4F056141A03',
  '0x0F0367D51C5fFF8e556528696e5C80ACC961E73C',
  '0x61588842DD47B09A7c44291f0E1484C4388d5998',
  '0x124Ed1d4E3b8f5Bc8d757f27db615fD6D4E2f284',
  '0x6Fb5E127F59f49c848CC204A9a9a575CA52C1cD3',
  '0x499b32df5dA30F5c531180AaDC7A32e208d86E7D',
  '0x3A5C5Cb8959d2026B7a711772027D00a26BEf574',
  '0x1a1484F5522C706497c29B61a924DfcEfDBc4109',
  '0x7C1E05d37894A090E3f6277775C740bd4DbCBFAf',
  '0x367fbb72B5123d14Ff4C749B21A01509FC4C6703',
  '0xE1653a997363B5082C8240311BFe29F184dFad91',
];

module.exports = {
  data: new SlashCommandBuilder()
    .setName('my-validator')
    .setDescription('Check statistics on your validator'),

  async execute(interaction) {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const discordId = interaction.user.id;
    //console.log(discordId)

    if (!(discordId in USER_NODE_MAP)) {
      return interaction.editReply(`❌ Your Discord ID is **not** an active validator.`);
    }

    const nodeAddr = USER_NODE_MAP[discordId];

    if (!nodeAddr || nodeAddr.trim() === '') {
      return interaction.editReply(`⚠️ You are a validator, but there is no record of your node address for this command.`);
    }

    const results = [];
    let balanceInXDC ='0'

    try {
      const balance = await xdc3.eth.getBalance(nodeAddr);
      balanceInXDC = xdc3.utils.fromWei(balance, 'ether');
    } catch (error) {
      console.error('Error fetching balance:', error.message);
    }

    for (const contractAddr of CONTRACTS) {
      try {
        const contract = new xdc3.eth.Contract(FluxABI, contractAddr);

        const oracles = await contract.methods.getOracles().call();
        const desc = await contract.methods.description().call();

        if (!oracles.some(o => o.toLowerCase() === nodeAddr.toLowerCase())) {
          //console.log(`Node not registered in contract ${contractAddr}`);
          continue;
        }

        const raw = await contract.methods.withdrawablePayment(nodeAddr).call();
        const pli = xdc3.utils.fromWei(raw, 'ether');

        results.push(`✅ **${contractAddr} (${desc})** → **${pli} PLI**`);

      } catch (err) {
        console.error(`Error with contract ${contractAddr}:`, err.message);
        results.push(`⚠️ **${contractAddr}** → Error: ${err.message}`);
      }
    }

    if (results.length === 0) {
      return interaction.editReply(`😕 Your node address is not listed on any active contracts.`);
    }

    await interaction.editReply(
      `📊 Withdrawable payments for your node \`${nodeAddr}\`:\n\n` +
      results.join('\n') +
      '\n\n' +
      `⛽ Current amount of XDC gas available to your node \`${nodeAddr}\`:\n\n` +
      `💰 **${balanceInXDC}**\n`
    );
  },
};
const { getPrices } = require('../utils/getPrices');
const { getNodes } = require('../utils/getNodes');
const { clearRoles, setRed, setGreen } = require('../utils/setRoles');

let lastPrice;
let arrow;

async function setPresence(client) {
  try {
    let fixed = '4';

    const up = "\u2B08";
    const down = "\u2B0A";
    const mid = "\u22EF";

    const decimals = 4;
    const currentPrice = await getPrices(decimals); // Number
    const guild = await client.guilds.cache.get(`${process.env.GUILD_ID}`);
    const member = await guild.members.cache.get(`${process.env.CLIENT_ID}`);

    if (typeof lastPrice === 'undefined') {
      await clearRoles(guild, member);
      arrow = mid;
    } else if (currentPrice > lastPrice) {
      arrow = up;
      await setGreen(guild, member);
    } else if (currentPrice < lastPrice) {
      arrow = down;
      await setRed(guild, member);
    }

    lastPrice = currentPrice;

    const nodeCount = await getNodes();

    member.setNickname(`PLI ${arrow} $${currentPrice.toFixed(decimals)}`);
    client.user.setPresence({
      activities: [{
        name: `v2.4 nodes: ${nodeCount}`,
        type: 3 // ActivityType.Watching
      }]
    });
  } catch (error) {
    console.error("Error setting presence:", error);
  }
}

module.exports = { setPresence };
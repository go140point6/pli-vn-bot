// src/utils/discord.js
function isDiscordId(v) {
  return typeof v === 'string' && /^\d{17,20}$/.test(v.trim());
}

module.exports = { isDiscordId };
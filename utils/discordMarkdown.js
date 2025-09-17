// utils/discordMarkdown.js

/**
 * Escape characters that Discord Markdown treats specially in inline text.
 * Escapes: _ * ` ~
 */
function escapeDiscord(text) {
  if (text == null) return '';
  return String(text).replace(/([_*`~])/g, '\\$1');
}

module.exports = { escapeDiscord };
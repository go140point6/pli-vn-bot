// services/dm.js
// Safe Discord DMs with chunking + sensible error handling.
// - Splits messages under Discord's 2000-char limit
// - Optional "neverDisable" (use for admins) so we never flip accepts_dm for them
// - Only disables DM on Discord 50007 ("Cannot send messages to this user")

const { setAcceptsDM } = require('../db/statements');

const DISCORD_LIMIT = 2000;
// Leave room for tiny suffix like "… (1/3)" and any markdown we add
const CHUNK_TARGET = 1900;
const SEND_DELAY_MS = 350; // gentle pacing between chunks

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function getDiscordErrorCode(err) {
  return err?.code ?? err?.rawError?.code ?? err?.error?.code ?? null;
}
function shouldDisableDM(err) {
  // Only when the user blocks DMs / can't be messaged by the bot.
  return Number(getDiscordErrorCode(err)) === 50007;
}

/**
 * Split text into Discord-safe chunks, preferring paragraph/line/space boundaries.
 */
function chunkForDiscord(text, maxLen = CHUNK_TARGET) {
  if (text.length <= maxLen) return [text];

  const chunks = [];
  let remaining = text;

  while (remaining.length > maxLen) {
    const head = remaining.slice(0, maxLen);
    let cut =
      head.lastIndexOf('\n\n') !== -1 ? head.lastIndexOf('\n\n')
        : head.lastIndexOf('\n') !== -1 ? head.lastIndexOf('\n')
        : head.lastIndexOf(' ') !== -1 ? head.lastIndexOf(' ')
        : maxLen;

    // Guard: don't cut too early if we found a boundary too close to the start
    if (cut < Math.floor(maxLen * 0.4)) cut = maxLen;

    chunks.push(remaining.slice(0, cut).trimEnd());
    remaining = remaining.slice(cut).trimStart();
  }

  if (remaining.length) chunks.push(remaining);
  return chunks;
}

/**
 * Send a (possibly long) DM by chunking it. Returns true/false for overall success.
 * Options:
 *   - neverDisable: if true, never flip accepts_dm to 0
 *   - isAdmin: maps to neverDisable=true (admins never get disabled)
 *   - addCounters: append "_1/3_" counters to chunks (suppressed if chunk contains ``` code fences)
 *   - prefix: string placed before the first chunk
 */
async function sendDM(
  client,
  discordId,
  text,
  {
    neverDisable = false,
    isAdmin = false,
    addCounters = true,
    prefix = ''
  } = {}
) {
  if (!client) return false;

  let user;
  try {
    user = await client.users.fetch(discordId);
  } catch (e) {
    const code = getDiscordErrorCode(e);
    console.warn(`[dm] fetch failed for ${discordId}: ${e.message} (code=${code ?? 'n/a'})`);
    return false;
  }

  const full = prefix ? `${prefix}\n${text}` : text;

  // Split into Discord-safe chunks and hard-cap to platform limit
  const chunks = chunkForDiscord(full, CHUNK_TARGET).map(c => c.slice(0, DISCORD_LIMIT));
  const total = chunks.length;

  // Admins (or explicit neverDisable) should never get auto-disabled on 50007
  const localNeverDisable = Boolean(neverDisable || isAdmin);

  for (let i = 0; i < total; i++) {
    let body = chunks[i];

    // If this chunk contains a code fence, don't append counters.
    // This helps keep backticks pristine so Discord renders the block reliably.
    const containsFence = body.includes('```');

    if (addCounters && total > 1 && !containsFence) {
      body += `\n_${i + 1}/${total}_`;
    }

    try {
      await user.send(body);
    } catch (e) {
      const code = getDiscordErrorCode(e);
      console.warn(`[dm] send failed to ${discordId}: ${e.message} (code=${code ?? 'n/a'})`);

      // Only disable DMs when it's the "Cannot send messages to this user" case
      if (!localNeverDisable && shouldDisableDM(e)) {
        try {
          setAcceptsDM.run(discordId);
          console.log(`[dm] accepts_dm -> 0 for ${discordId} (Discord 50007)`);
        } catch (dbErr) {
          console.error('[dm] failed to update accepts_dm:', dbErr.message);
        }
      } else {
        console.log('[dm] not disabling accepts_dm (non-50007 error or neverDisable/isAdmin set).');
      }

      return false; // stop on first failed chunk
    }

    if (i < total - 1) await sleep(SEND_DELAY_MS);
  }

  return true;
}

module.exports = { sendDM, chunkForDiscord };

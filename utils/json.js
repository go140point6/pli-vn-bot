// src/utils/json.js
function jsonOrNull(v) {
  if (v === null || v === undefined || String(v).trim() === '') return null;
  const s = String(v).trim();
  try {
    JSON.parse(s);
    return s; // store as-is to avoid reformatting secrets/headers
  } catch {
    return null;
  }
}

module.exports = { jsonOrNull };
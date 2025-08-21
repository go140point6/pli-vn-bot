// src/utils/address.js
const { keccak256 } = require('js-sha3'); // npm i js-sha3

function toEip55(lowercaseAddr) {
  const addr = lowercaseAddr.replace(/^0x/, '').toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(addr)) throw new Error('toEip55: invalid lowercase 0x address body');
  const hash = keccak256(addr);
  let out = '0x';
  for (let i = 0; i < addr.length; i++) {
    const nibble = parseInt(hash[i], 16);
    out += nibble >= 8 ? addr[i].toUpperCase() : addr[i];
  }
  return out;
}

function isValidEip55(mixed) {
  if (typeof mixed !== 'string') return false;
  if (!/^0x[0-9a-fA-F]{40}$/.test(mixed)) return false;
  const body = mixed.slice(2);
  // If it's all lower or all upper, treat as "unchecksummed, acceptable"
  if (body === body.toLowerCase() || body === body.toUpperCase()) return true;
  return toEip55('0x' + body.toLowerCase()) === '0x' + body;
}

function xdcTo0x(s) {
  if (typeof s !== 'string') return s;
  return s.replace(/^xdc/i, '0x');
}

function zeroXToXdc(s) {
  if (typeof s !== 'string') return s;
  return s.replace(/^0x/i, 'xdc');
}

/**
 * Normalize any input ("0x..." or "xdc...") to canonical forms.
 * - Returns:
 *   { lower, eip55, format } where format is '0x' or 'xdc' (the *input* style)
 * Throws on invalid hex or bad EIP-55 when mixed-case is present.
 */
function normalizeAddressAny(input) {
  if (typeof input !== 'string') throw new Error('address must be string');
  let s = input.trim();
  let format = '0x';
  if (/^xdc/i.test(s)) {
    s = xdcTo0x(s);
    format = 'xdc';
  }
  if (!/^0x[0-9a-fA-F]{40}$/.test(s)) {
    throw new Error('not a valid EVM address');
  }
  const body = s.slice(2);
  const isMixed = !(body === body.toLowerCase() || body === body.toUpperCase());
  if (isMixed && !isValidEip55(s)) {
    throw new Error('invalid EIP-55 checksum');
  }
  const lower = '0x' + body.toLowerCase();
  const eip55 = toEip55(lower);
  return { lower, eip55, format };
}

/**
 * Utility: ensure a lowercase 0x address without checksum checks.
 * Useful when you already trust the source and just need canonical key.
 */
function toLower0x(input) {
  const s = xdcTo0x(String(input || '').trim());
  if (!/^0x[0-9a-fA-F]{40}$/.test(s)) throw new Error('toLower0x: invalid address');
  return '0x' + s.slice(2).toLowerCase();
}

module.exports = {
  toEip55,
  isValidEip55,
  normalizeAddressAny,
  toLower0x,
  xdcTo0x,
  zeroXToXdc,
};
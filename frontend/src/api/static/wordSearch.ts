/* Whole-word matching for the search worker, in one place so it can be tested.
 *
 * The rule: a keyword matches only when it stands on its own, bounded on
 * both sides by the start or end of the text or by a character that is not
 * part of a word. A word character is a Unicode letter, a Unicode number or
 * an underscore, so "tree" matches "tree" and not "Matree", "trees" or
 * "tree2", while a Greek letter next to the term still counts as part of a
 * word and a separator like · does not.
 *
 * The ASCII path scans bytes, as the worker always has, and only decodes a
 * neighbouring character when it is not ASCII, so the common case costs
 * nothing extra. The Unicode path is a single regular expression, built once
 * per search.
 */

const decoder = new TextDecoder();

/** Fold an ASCII letter to lower case; every other byte is left alone. */
function fold(byte: number): number {
  return byte >= 65 && byte <= 90 ? byte + 32 : byte;
}

/** Whether the byte is an ASCII word character: a letter, a digit or _. */
function isWordByte(byte: number): boolean {
  return (
    (byte >= 48 && byte <= 57) || // 0-9
    (byte >= 65 && byte <= 90) || // A-Z
    (byte >= 97 && byte <= 122) || // a-z
    byte === 95 // _
  );
}

/** Whether a character is a word character: a Unicode letter, a Unicode number or _. */
function isWordChar(char: string): boolean {
  return /[\p{L}\p{N}_]/u.test(char);
}

/** Whether the character ending just before byte `at` is a word character.
 *  `at` lies inside a row that starts at `rowStart`; a non-ASCII neighbour
 *  is decoded so a Greek letter counts as a word character and a separator
 *  like · does not. */
function wordBefore(blob: Uint8Array, at: number, rowStart: number): boolean {
  if (at <= rowStart) return false;
  const byte = blob[at - 1];
  if (byte < 128) return isWordByte(byte);
  // Walk back to the lead byte of this UTF-8 character.
  let lead = at - 1;
  while (lead > rowStart && (blob[lead] & 0xc0) === 0x80) lead--;
  return isWordChar(decoder.decode(blob.subarray(lead, at)));
}

/** Whether the character starting at byte `at` is a word character.
 *  `at` lies inside a row that ends at `rowEnd`. */
function wordAfter(blob: Uint8Array, at: number, rowEnd: number): boolean {
  if (at >= rowEnd) return false;
  const byte = blob[at];
  if (byte < 128) return isWordByte(byte);
  // `at` starts a character; skip its continuation bytes.
  let end = at + 1;
  while (end < rowEnd && (blob[end] & 0xc0) === 0x80) end++;
  return isWordChar(decoder.decode(blob.subarray(at, end)));
}

/** Whether the row's bytes in [start, end) contain `needle` as a whole word.
 *  `needle` is the keyword already folded to lower case and encoded; the row
 *  is folded byte by byte as it is scanned, as before. */
export function rowHasWord(
  blob: Uint8Array,
  start: number,
  end: number,
  needle: Uint8Array,
): boolean {
  const first = needle[0];
  const width = needle.length;
  const last = end - width + 1;
  for (let at = start; at < last; at++) {
    if (fold(blob[at]) !== first) continue;
    let position = 1;
    while (position < width && fold(blob[at + position]) === needle[position]) position++;
    if (position !== width) continue;
    // A word character on either side means the keyword sits inside a
    // longer word, as "tree" does in "Matree".
    if (wordBefore(blob, at, start)) continue;
    if (wordAfter(blob, at + width, end)) continue;
    return true;
  }
  return false;
}

/** Escape a string for use inside a regular expression. */
function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** A pattern matching `needle` only as a whole word, for the non-ASCII path.
 *  The needle is already folded to lower case, as is the text it is tested
 *  against, so no case-insensitive flag is needed. */
export function wholeWordPattern(needle: string): RegExp {
  return new RegExp(`(?<![\\p{L}\\p{N}_])${escapeRegExp(needle)}(?![\\p{L}\\p{N}_])`, "u");
}

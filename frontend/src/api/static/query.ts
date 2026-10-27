/* The search, over the container: filter, match, group, and shape as JSON.
 *
 * A port of the query path in backend/src/epigcorpus/core/search.py and the
 * serializers in backend/src/epigcorpus/api/serialize.py. The rules it keeps:
 *
 * - material, province and place match a value exactly; language and category
 *   match on token overlap, so "Greek, Latin" answers both filters;
 * - values within one filter are alternatives, different filters must all hold;
 * - the keyword is a case-insensitive substring, not a word or a regex;
 * - the map draws one circle per (place, 5-decimal coordinate), and places
 *   with the fewest inscriptions come first, so the largest draw on top.
 */
import type { Corpus, Mode } from "./container";
import { MISSING_YEAR } from "./container";
import type { FilterName, Selections } from "./phrasing";

/** Inscriptions listed inside a result popup before it is truncated. */
const EXAMPLE_LIMIT = 8;
/** A popup text longer than this is cut, as the server cuts it. */
const EXAMPLE_WIDTH = 160;

interface Index {
  place: Map<string, number>;
  province: Map<string, number>;
  material: Map<string, number>;
  language: Map<string, number>;
  category: Map<string, number>;
}

const indexes = new WeakMap<Corpus, Index>();

function dictionary(values: string[], reservedZero: boolean): Map<string, number> {
  const lookup = new Map<string, number>();
  for (let position = reservedZero ? 1 : 0; position < values.length; position++) {
    lookup.set(values[position], position);
  }
  return lookup;
}

function indexOf(corpus: Corpus): Index {
  let index = indexes.get(corpus);
  if (!index) {
    index = {
      // Place, province and material reserve index 0 for a missing value, and
      // no filter offers it, so a row without one stays unselectable alike.
      place: dictionary(corpus.places, true),
      province: dictionary(corpus.provinces, true),
      material: dictionary(corpus.materials, true),
      // Languages and categories are token lists with no missing slot: every
      // entry, index 0 included, is a value a row can carry.
      language: dictionary(corpus.languages, false),
      category: dictionary(corpus.categories, false),
    };
    indexes.set(corpus, index);
  }
  return index;
}

function chosenIds(lookup: Map<string, number>, values: string[]): Set<number> | null {
  if (!values.length) return null;
  const chosen = new Set<number>();
  for (const value of values) {
    const id = lookup.get(value);
    if (id !== undefined) chosen.add(id);
  }
  // A filter whose values are all unknown matches nothing, as IN does.
  return chosen;
}

/** Whether any of a row's tokens (offsets [row, row + 1)) is selected. */
function overlaps(
  offsets: Uint32Array,
  ids: Uint16Array,
  row: number,
  chosen: Set<number>,
): boolean {
  for (let at = offsets[row]; at < offsets[row + 1]; at++) {
    if (chosen.has(ids[at])) return true;
  }
  return false;
}

/** The rows a query matches, ascending, as the server returns them. */
export function matchRows(
  corpus: Corpus,
  selections: Selections,
  hits: Uint8Array | null,
): Uint32Array {
  const index = indexOf(corpus);
  const chosen: Record<FilterName, Set<number> | null> = {
    place: chosenIds(index.place, selections.place ?? []),
    province: chosenIds(index.province, selections.province ?? []),
    material: chosenIds(index.material, selections.material ?? []),
    language: chosenIds(index.language, selections.language ?? []),
    category: chosenIds(index.category, selections.category ?? []),
  };
  const matched = new Uint32Array(corpus.rows);
  let count = 0;
  for (let row = 0; row < corpus.rows; row++) {
    if (hits && hits[row] === 0) continue;
    if (chosen.place && !chosen.place.has(corpus.place[row])) continue;
    if (chosen.province && !chosen.province.has(corpus.province[row])) continue;
    if (chosen.material && !chosen.material.has(corpus.material[row])) continue;
    if (chosen.language && !overlaps(corpus.languageOffsets, corpus.languageIds, row, chosen.language))
      continue;
    if (chosen.category && !overlaps(corpus.categoryOffsets, corpus.categoryIds, row, chosen.category))
      continue;
    matched[count++] = row;
  }
  return matched.subarray(0, count);
}

export interface PlaceGroup {
  place: string;
  /** The coordinate pair's index, which is how popup examples are keyed. */
  coordinate: number;
  latitude: number;
  longitude: number;
  count: number;
}

/** One entry per findspot, smallest first so the map draws the largest on top. */
export function aggregatePlaces(corpus: Corpus, rows: Uint32Array): PlaceGroup[] {
  const found = new Map<number, PlaceGroup>();
  for (const row of rows) {
    // Exact while there are fewer than two million places, which is four
    // orders of magnitude more than EDCS records.
    const key = corpus.place[row] * 4294967296 + corpus.coord[row];
    const group = found.get(key);
    if (group) {
      group.count += 1;
      continue;
    }
    const coordinate = corpus.coord[row];
    found.set(key, {
      place: corpus.places[corpus.place[row]],
      coordinate,
      latitude: corpus.coordLatitude[coordinate],
      longitude: corpus.coordLongitude[coordinate],
      count: 1,
    });
  }
  return [...found.values()].sort((left, right) => left.count - right.count);
}

/** A row's category values, in the order the corpus carries them. */
export function categoryTokens(corpus: Corpus, row: number): string[] {
  const values: string[] = [];
  for (let at = corpus.categoryOffsets[row]; at < corpus.categoryOffsets[row + 1]; at++) {
    values.push(corpus.categories[corpus.categoryIds[at]]);
  }
  return values;
}

export function edcsId(corpus: Corpus, row: number): string {
  return `EDCS-${String(corpus.edcs[row]).padStart(8, "0")}`;
}

export function recordId(corpus: Corpus, row: number): string {
  return `${edcsId(corpus, row)}-${corpus.index[row]}`;
}

export interface Summary {
  record_id: string;
  edcs_id: string;
  province: string | null;
  place: string;
  latitude: number;
  longitude: number;
  not_before: number | null;
  not_after: number | null;
  language: string | null;
  material_en: string | null;
  category_en: string[];
  inscription_text: string | null;
  inscription_text_conservative: string | null;
  inscription_text_interpretive: string | null;
}

/**
 * One row, as the API serializes it.
 *
 * Two deliberate differences from the API: only the searched mode's text is
 * filled (the other two are null, since only that mode's texts are loaded),
 * and the coordinates are the rounded pair the map plots rather than the
 * unrounded value the corpus carries.
 */
export function summary(
  corpus: Corpus,
  row: number,
  mode: Mode,
  text: string | null,
): Summary {
  const coordinate = corpus.coord[row];
  const year = (value: number) => (value === MISSING_YEAR ? null : value);
  return {
    record_id: recordId(corpus, row),
    edcs_id: edcsId(corpus, row),
    province: corpus.provinces[corpus.province[row]] || null,
    place: corpus.places[corpus.place[row]],
    latitude: corpus.coordLatitude[coordinate],
    longitude: corpus.coordLongitude[coordinate],
    not_before: year(corpus.notBefore[row]),
    not_after: year(corpus.notAfter[row]),
    language: corpus.languageText[corpus.languageTextCode[row]] || null,
    material_en: corpus.materials[corpus.material[row]] || null,
    category_en: categoryTokens(corpus, row),
    inscription_text: mode === "raw" ? text : null,
    inscription_text_conservative: mode === "conservative" ? text : null,
    inscription_text_interpretive: mode === "interpretive" ? text : null,
  };
}

/** The columns the static build's TSV carries, in order.
 *
 * The API's export is the whole cleaned corpus; this build holds the search
 * frame, so the download carries the columns the app itself shows, in the
 * cleaned corpus's own order (raw, then conservative, then interpretive).
 */
export const TSV_COLUMNS = [
  "record_id",
  "edcs_id",
  "province",
  "place",
  "latitude",
  "longitude",
  "not_before",
  "not_after",
  "language",
  "material_en",
  "category_en",
  "inscription_text",
  "inscription_text_conservative",
  "inscription_text_interpretive",
] as const;

/** One cell, quoted only when it has to be, as pandas writes it. */
export function tsvCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = Array.isArray(value) ? value.join(", ") : String(value);
  return /[\t\n\r"]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export function tsvLine(values: readonly unknown[]): string {
  return values.map(tsvCell).join("\t");
}

/** One row of the TSV, in TSV_COLUMNS order. */
export function tsvValues(row: Summary): unknown[] {
  return TSV_COLUMNS.map((column) => row[column]);
}

/** The popup text for a row: whitespace collapsed, then cut at 160. */
export function exampleText(value: string): string {
  const collapsed = value.split(/\s+/).filter(Boolean).join(" ");
  return collapsed.length > EXAMPLE_WIDTH ? `${collapsed.slice(0, EXAMPLE_WIDTH - 3)}\u2026` : collapsed;
}

export const EXAMPLE_TRUNCATION = { EXAMPLE_LIMIT, EXAMPLE_WIDTH };

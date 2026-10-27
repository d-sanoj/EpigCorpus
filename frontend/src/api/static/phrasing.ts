/* A query in words: the title and subtitle the map card shows.
 *
 * A verbatim port of `describe_query` in backend/src/epigcorpus/core/search.py,
 * so a search reads the same on the hosted page as it does with a server
 * behind it. The condensation rule is the subtle part: a sentence that runs
 * past one line collapses its wordiest selections into counts, one at a time,
 * longest first, and only then is shortened further or trimmed on a word.
 */
import type { Mode } from "./container";

export type FilterName = "language" | "material" | "category" | "province" | "place";
export type Selections = Record<FilterName, string[]>;

/** How each filter reads in a sentence; the order is the order they are named. */
export const FILTER_PHRASING: Record<
  FilterName,
  { preposition: string; conjunction: string; plural: string }
> = {
  language: { preposition: "in", conjunction: "or", plural: "languages" },
  material: { preposition: "on", conjunction: "or", plural: "materials" },
  category: { preposition: "classified as", conjunction: "or", plural: "categories" },
  province: { preposition: "from", conjunction: "and", plural: "provinces" },
  place: { preposition: "at", conjunction: "and", plural: "places" },
};

export const FILTER_ORDER = Object.keys(FILTER_PHRASING) as FilterName[];

/** Beyond this many values a filter is summarised as a count, not listed. */
const PHRASE_LIST_LIMIT = 3;
/** A subtitle longer than this is condensed so it fits on one line. */
const SUBTITLE_WIDTH = 132;

const SEARCH_MODE_PHRASING: Record<Mode, string> = {
  raw: "raw inscription text",
  conservative: "conservative cleaned text",
  interpretive: "interpretive cleaned text",
};

const SEARCH_MODE_PHRASING_SHORT: Record<Mode, string> = {
  raw: "raw text",
  conservative: "conservative text",
  interpretive: "interpretive text",
};

/** `a`, `a or b`, `a, b or c`. */
function joinValues(values: string[], conjunction: string): string {
  if (values.length === 1) return values[0];
  return `${values.slice(0, -1).join(", ")} ${conjunction} ${values[values.length - 1]}`;
}

/** One filter as a phrase, either spelled out or summarised as a count. */
function clause(column: FilterName, chosen: string[], condensed: boolean): string {
  const { preposition, conjunction, plural } = FILTER_PHRASING[column];
  if (condensed || chosen.length > PHRASE_LIST_LIMIT) {
    return `${preposition} ${chosen.length} ${plural}`;
  }
  return `${preposition} ${joinValues(chosen, conjunction)}`;
}

export function describeQuery(
  term: string,
  mode: Mode,
  selections: Selections,
): { title: string; subtitle: string } {
  const active = FILTER_ORDER.filter((column) => selections[column]?.length);
  let title: string;
  if (term) {
    title = `Inscriptions matching "${term}"`;
  } else if (active.length) {
    title = "Inscriptions across the Roman Empire";
  } else {
    return {
      title: "All known places in the EDCS corpus",
      subtitle: "Every place EDCS records, sized by inscription count",
    };
  }

  const condensed = new Set<FilterName>();

  function compose(shortMode: boolean): string {
    const parts = active.map((column) => clause(column, selections[column], condensed.has(column)));
    let sentence = parts.join(", ");
    if (term) {
      const phrasing = shortMode ? SEARCH_MODE_PHRASING_SHORT : SEARCH_MODE_PHRASING;
      const searched = `searched in ${phrasing[mode] ?? SEARCH_MODE_PHRASING.raw}`;
      sentence = sentence ? `${sentence} \u2014 ${searched}` : searched;
    }
    return sentence ? sentence.slice(0, 1).toUpperCase() + sentence.slice(1) : "";
  }

  let subtitle = compose(false);

  // Collapse the wordiest selections first; a single-value filter has nothing
  // to gain from collapsing, so it is never touched.
  const savings = new Map<FilterName, number>();
  for (const column of active) {
    if (selections[column].length <= 1) continue;
    savings.set(
      column,
      clause(column, selections[column], false).length -
        clause(column, selections[column], true).length,
    );
  }
  const wordiest = [...savings.keys()].sort((a, b) => savings.get(b)! - savings.get(a)!);
  for (const column of wordiest) {
    if (subtitle.length <= SUBTITLE_WIDTH) break;
    condensed.add(column);
    subtitle = compose(false);
  }

  if (subtitle.length > SUBTITLE_WIDTH) subtitle = compose(true);
  if (subtitle.length > SUBTITLE_WIDTH) {
    // Trim on a word boundary rather than mid-word.
    const cut = subtitle.slice(0, SUBTITLE_WIDTH - 1);
    const space = cut.lastIndexOf(" ");
    const head = space === -1 ? cut : cut.slice(0, space);
    subtitle = `${head.replace(/[ ,\u2014]+$/u, "")}\u2026`;
  }
  return { title, subtitle };
}

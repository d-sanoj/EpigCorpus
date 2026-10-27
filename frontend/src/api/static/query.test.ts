/* The query port: filtering, grouping and serializing, on five known rows.
 *
 * The fixture states its own expectations - which rows a token-overlap filter
 * matches, which place groups merge, what a missing value serializes as - so
 * the rules the server implements in pandas are pinned here in TypeScript.
 */
import { describe, expect, test } from "vitest";
import { MISSING_YEAR, type Corpus, type Mode } from "./container";
import type { Selections } from "./phrasing";
import {
  aggregatePlaces,
  exampleText,
  matchRows,
  summary,
  TSV_COLUMNS,
  tsvCell,
  tsvLine,
  tsvValues,
} from "./query";

const TEXTS: Record<Mode, string[]> = {
  raw: ["D M S", "via", "", "aqua", "iter"],
  conservative: ["D M S", "via", "", "aqua", "iter"],
  interpretive: ["Dis Manibus Sacrum", "via", "", "aqua", "iter"],
};

function offsets(texts: string[]): Uint32Array {
  const bytes = texts.map((text) => new TextEncoder().encode(text).length);
  const result = new Uint32Array(texts.length + 1);
  bytes.forEach((length, position) => {
    result[position + 1] = result[position] + length;
  });
  return result;
}

function fixture(): Corpus {
  return {
    fingerprint: "fixture",
    rows: 5,
    // Index 0 of every dictionary column is the missing value.
    places: ["?", "Ostia", "Roma"],
    provinces: ["", "Italia"],
    materials: ["", "marble", "stone"],
    // Token dictionaries have no reserved slot: index 0 is a value.
    languages: ["Greek", "Latin"],
    languageText: ["", "Greek", "Greek, Latin", "Latin"],
    categories: ["men", "milestones", "tomb inscriptions"],
    place: Uint32Array.from([2, 2, 0, 1, 2]),
    // Coordinates, sorted as pairs: (41.5, 12.0), (41.75, 12.3), (41.9028, 12.4964).
    coord: Uint32Array.from([2, 2, 0, 1, 1]),
    province: Uint16Array.from([1, 1, 0, 1, 1]),
    material: Uint16Array.from([2, 1, 2, 2, 2]),
    edcs: Uint32Array.from([1, 2, 3, 4, 5]),
    index: Uint16Array.from([0, 0, 1, 0, 0]),
    notBefore: Int32Array.from([100, MISSING_YEAR, 150, -50, 300]),
    notAfter: Int32Array.from([200, MISSING_YEAR, 150, 50, 300]),
    languageOffsets: Uint32Array.from([0, 1, 3, 4, 5, 6]),
    languageIds: Uint16Array.from([1, 0, 1, 1, 0, 1]),
    languageTextCode: Uint16Array.from([3, 2, 3, 1, 3]),
    categoryOffsets: Uint32Array.from([0, 2, 3, 3, 4, 5]),
    categoryIds: Uint16Array.from([2, 0, 1, 0, 0]),
    coordLatitude: Float64Array.from([41.5, 41.75, 41.9028]),
    coordLongitude: Float64Array.from([12.0, 12.3, 12.4964]),
    textOffsets: {
      raw: offsets(TEXTS.raw),
      conservative: offsets(TEXTS.conservative),
      interpretive: offsets(TEXTS.interpretive),
    },
  };
}

function selections(partial: Partial<Record<keyof Selections, string[]>> = {}): Selections {
  return { language: [], material: [], category: [], province: [], place: [], ...partial };
}

const rows = (corpus: Corpus, chosen = selections(), hits: Uint8Array | null = null) =>
  [...matchRows(corpus, chosen, hits)];

describe("matchRows", () => {
  test("no filter, no keyword: every row, in order", () => {
    expect(rows(fixture())).toEqual([0, 1, 2, 3, 4]);
  });

  test("a keyword keeps only the rows it hits", () => {
    const hits = Uint8Array.from([0, 1, 0, 0, 1]);
    expect(rows(fixture(), selections(), hits)).toEqual([1, 4]);
  });

  test("place, province and material match exactly", () => {
    const corpus = fixture();
    expect(rows(corpus, selections({ place: ["Roma"] }))).toEqual([0, 1, 4]);
    expect(rows(corpus, selections({ province: ["Italia"] }))).toEqual([0, 1, 3, 4]);
    expect(rows(corpus, selections({ material: ["stone"] }))).toEqual([0, 2, 3, 4]);
    // The "?" place and the missing province are real rows, but unselectable.
    expect(rows(corpus, selections({ place: ["?"] }))).toEqual([]);
    expect(rows(corpus, selections({ province: [""] }))).toEqual([]);
  });

  test("language and category match on token overlap", () => {
    const corpus = fixture();
    // Row 1 reads "Greek, Latin" and answers the Greek filter.
    expect(rows(corpus, selections({ language: ["Greek"] }))).toEqual([1, 3]);
    expect(rows(corpus, selections({ category: ["men"] }))).toEqual([0, 3, 4]);
  });

  test("values within a filter are alternatives; filters must all hold", () => {
    const corpus = fixture();
    expect(rows(corpus, selections({ material: ["marble", "stone"] }))).toEqual([0, 1, 2, 3, 4]);
    expect(rows(corpus, selections({ province: ["Italia"], language: ["Greek"] }))).toEqual([1, 3]);
    expect(rows(corpus, selections({ category: ["men"], language: ["Greek"] }))).toEqual([3]);
  });

  test("a filter value nothing carries matches nothing, as IN does", () => {
    expect(rows(fixture(), selections({ province: ["Nowhere"] }))).toEqual([]);
  });
});

describe("aggregatePlaces", () => {
  test("one circle per place and coordinate, smallest first", () => {
    const corpus = fixture();
    const places = aggregatePlaces(corpus, matchRows(corpus, selections(), null));
    expect(places.map((place) => place.place)).toEqual(["?", "Ostia", "Roma", "Roma"]);
    expect(places.map((place) => place.count)).toEqual([1, 1, 1, 2]);
    // The two Roma rows that share a findspot merge; the one at another
    // coordinate keeps its own circle.
    expect([...places.map((place) => place.coordinate)]).toEqual([0, 1, 1, 2]);
    expect(places[3].latitude).toBeCloseTo(41.9028, 4);
    expect(places[3].longitude).toBeCloseTo(12.4964, 4);
  });
});

describe("summary", () => {
  test("missing values come back as null, not as the dictionary's placeholder", () => {
    const row = summary(fixture(), 2, "raw", null);
    expect(row).toMatchObject({
      record_id: "EDCS-00000003-1",
      edcs_id: "EDCS-00000003",
      province: null,
      place: "?",
      latitude: 41.5,
      longitude: 12,
      not_before: 150,
      not_after: 150,
      language: "Latin",
      material_en: "stone",
      category_en: [],
    });
    // A year the corpus lacks is the container's sentinel, and reads as null.
    const undated = summary(fixture(), 1, "raw", null);
    expect(undated.not_before).toBeNull();
    expect(undated.not_after).toBeNull();
  });

  test("only the searched mode's text is filled", () => {
    const corpus = fixture();
    const raw = summary(corpus, 0, "raw", TEXTS.raw[0]);
    expect(raw.inscription_text).toBe("D M S");
    expect(raw.inscription_text_interpretive).toBeNull();
    expect(raw.inscription_text_conservative).toBeNull();

    const interpretive = summary(corpus, 0, "interpretive", TEXTS.interpretive[0]);
    expect(interpretive.inscription_text).toBeNull();
    expect(interpretive.inscription_text_interpretive).toBe("Dis Manibus Sacrum");
  });

  test("categories keep the corpus's order and the language its own string", () => {
    const row = summary(fixture(), 0, "raw", null);
    expect(row.category_en).toEqual(["tomb inscriptions", "men"]);
    expect(row.language).toBe("Latin");
    expect(summary(fixture(), 1, "raw", null).language).toBe("Greek, Latin");
  });
});

describe("the TSV the download writes", () => {
  test("a cell is quoted only when it has to be", () => {
    expect(tsvCell("Roma")).toBe("Roma");
    expect(tsvCell(null)).toBe("");
    expect(tsvCell(["men", "milestones"])).toBe("men, milestones");
    expect(tsvCell('he said "vidi"')).toBe('"he said ""vidi"""');
    expect(tsvCell("two\ttabs")).toBe('"two\ttabs"');
    expect(tsvLine(["a", "b"])).toBe("a\tb");
  });

  test("a summary becomes the fourteen columns, in order", () => {
    // Raw first, then conservative, then interpretive: the cleaned corpus's
    // own order, so the download's columns line up with the API's export.
    expect([...TSV_COLUMNS].slice(-3)).toEqual([
      "inscription_text",
      "inscription_text_conservative",
      "inscription_text_interpretive",
    ]);
    const values = tsvValues(summary(fixture(), 3, "raw", "aqua"));
    expect(values).toEqual([
      "EDCS-00000004-0",
      "EDCS-00000004",
      "Italia",
      "Ostia",
      41.75,
      12.3,
      -50,
      50,
      "Greek",
      "stone",
      ["men"],
      "aqua",
      null,
      null,
    ]);
  });
});

describe("popup text", () => {
  test("whitespace is collapsed, then a long text is cut", () => {
    expect(exampleText("D M\n  S ")).toBe("D M S");
    // Anything past 160 characters is cut at 157 plus an ellipsis, as the
    // server cuts it; anything shorter is left alone.
    expect(exampleText("x".repeat(161))).toHaveLength(158);
    expect(exampleText("x".repeat(200)).endsWith("\u2026")).toBe(true);
    expect(exampleText("x".repeat(160))).toHaveLength(160);
  });
});

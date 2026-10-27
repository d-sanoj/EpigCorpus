/* The phrasing port, against the server's own answers.
 *
 * Every expected string below was printed by the Python implementation
 * (`epigcorpus.core.search.describe_query`) for the same input, so a port that
 * drifts - in the order the filters are named, in which selection collapses
 * first, or in how a run-on sentence is trimmed - fails here.
 */
import { describe, expect, test } from "vitest";
import { describeQuery, type Selections } from "./phrasing";

function selections(partial: Partial<Record<keyof Selections, string[]>>): Selections {
  return { language: [], material: [], category: [], province: [], place: [], ...partial };
}

describe("describeQuery", () => {
  test("a keyword alone", () => {
    expect(describeQuery("viator", "raw", selections({}))).toEqual({
      title: 'Inscriptions matching "viator"',
      subtitle: "Searched in raw inscription text",
    });
  });

  test("no keyword and no filters is the corpus-wide view", () => {
    expect(describeQuery("", "raw", selections({}))).toEqual({
      title: "All known places in the EDCS corpus",
      subtitle: "Every place EDCS records, sized by inscription count",
    });
  });

  test("single-value filters are spelled out, in the server's order", () => {
    expect(
      describeQuery("", "raw", selections({ material: ["stone"], category: ["tomb inscriptions"], language: ["Latin"] })),
    ).toEqual({
      title: "Inscriptions across the Roman Empire",
      // language, material, category: the order FILTER_PHRASING lists them.
      subtitle: "In Latin, on stone, classified as tomb inscriptions",
    });
  });

  test("a run-on sentence collapses its wordiest selections into counts", () => {
    expect(
      describeQuery(
        "",
        "conservative",
        selections({
          province: ["A very long province name one", "A very long province name two"],
          place: ["A place with a remarkably long name", "Another remarkably long findspot"],
          material: ["stone"],
          category: ["tomb inscriptions"],
          language: ["Latin"],
        }),
      ),
    ).toEqual({
      title: "Inscriptions across the Roman Empire",
      subtitle:
        "In Latin, on stone, classified as tomb inscriptions, from 2 provinces, at 2 places",
    });
  });

  test("a keyword names the mode it searched in", () => {
    expect(
      describeQuery("viator", "interpretive", selections({ province: ["Noricum", "Italia", "Gallia", "Hispania"] })),
    ).toEqual({
      title: 'Inscriptions matching "viator"',
      // More than three values are a count, not a list.
      subtitle: "From 4 provinces \u2014 searched in interpretive cleaned text",
    });
  });

  test("a sentence that still will not fit is trimmed on a word", () => {
    // Every filter here holds one value, and single values are never collapsed,
    // so the server trims the sentence instead.
    expect(
      describeQuery(
        "",
        "raw",
        selections({
          province: ["Provincia with an exceedingly long name"],
          place: ["A findspot whose name alone fills a whole line of text"],
          material: ["a material with a long name"],
          category: ["a category with a long name"],
          language: ["a language with a long name"],
        }),
      ),
    ).toEqual({
      title: "Inscriptions across the Roman Empire",
      subtitle:
        "In a language with a long name, on a material with a long name, classified as a category with a long name, from Provincia with an\u2026",
    });
  });
});

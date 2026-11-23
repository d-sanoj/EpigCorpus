import { describe, expect, test } from "vitest";

import { rowHasWord, wholeWordPattern } from "./wordSearch";

const encode = (value: string) => new TextEncoder().encode(value);

function hasWord(text: string, needle: string): boolean {
  const blob = encode(text);
  return rowHasWord(blob, 0, blob.length, encode(needle.toLowerCase()));
}

describe("rowHasWord (the ASCII path)", () => {
  test("tree does not match Matree", () => {
    expect(hasWord("Matree", "tree")).toBe(false);
    expect(hasWord("Matree[3]", "tree")).toBe(false);
  });

  test("tree matches tree wherever it stands alone", () => {
    expect(hasWord("tree", "tree")).toBe(true);
    expect(hasWord("a tree grows", "tree")).toBe(true);
    expect(hasWord("under the tree", "tree")).toBe(true);
  });

  test("matching is case-insensitive", () => {
    expect(hasWord("TREE", "tree")).toBe(true);
    expect(hasWord("Tree", "TREE")).toBe(true);
  });

  test("a word inside another word does not match", () => {
    expect(hasWord("trees", "tree")).toBe(false);
    expect(hasWord("treehouse", "tree")).toBe(false);
    expect(hasWord("tree2", "tree")).toBe(false);
    expect(hasWord("tree_house", "tree")).toBe(false);
    expect(hasWord("convixit", "vixit")).toBe(false);
    expect(hasWord("vixit", "vixit")).toBe(true);
  });

  test("punctuation and hyphens are boundaries", () => {
    expect(hasWord("tree.", "tree")).toBe(true);
    expect(hasWord("(tree)", "tree")).toBe(true);
    expect(hasWord("tree-house", "tree")).toBe(true);
    expect(hasWord("tree, tree", "tree")).toBe(true);
  });

  test("a Greek letter next to the term is part of a word, a separator is not", () => {
    expect(hasWord("αtree", "tree")).toBe(false);
    expect(hasWord("treeα", "tree")).toBe(false);
    expect(hasWord("tree·x", "tree")).toBe(true);
    expect(hasWord("x·tree", "tree")).toBe(true);
  });

  test("a phrase matches only as a phrase", () => {
    expect(hasWord("a tree house here", "tree house")).toBe(true);
    expect(hasWord("a treehouse here", "tree house")).toBe(false);
  });
});

describe("wholeWordPattern (the non-ASCII path)", () => {
  test("a Greek word matches whole, not inside another word", () => {
    const pattern = wholeWordPattern("νίκη");
    expect(pattern.test("ἡ νίκη".toLowerCase())).toBe(true);
    expect(pattern.test("νίκης".toLowerCase())).toBe(false);
    expect(pattern.test("ανίκη".toLowerCase())).toBe(false);
  });

  test("an ASCII term through the pattern behaves the same way", () => {
    const pattern = wholeWordPattern("tree");
    expect(pattern.test("matree")).toBe(false);
    expect(pattern.test("a tree grows")).toBe(true);
  });

  test("the term is not a regex", () => {
    const pattern = wholeWordPattern("c++");
    expect(pattern.test("c++")).toBe(true);
    expect(pattern.test("cxx")).toBe(false);
  });
});

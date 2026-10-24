import { dating, formatInteger, plural } from "./format";

describe("format", () => {
  it("groups thousands", () => expect(formatInteger(568560)).toBe("568,560"));
  it("pluralises", () => {
    expect(plural(1, "match", "matches")).toBe("1 match");
    expect(plural(1200, "match", "matches")).toBe("1,200 matches");
  });
  it("dates as EDCS does", () => {
    expect(dating(null, null)).toBe("");
    expect(dating(150, 150)).toBe("150");
    expect(dating(30, 74)).toBe("30 – 74");
    expect(dating(null, -50)).toBe("? – -50");
  });
});

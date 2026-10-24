import { lighten, radiusFor } from "./epigMap";

describe("map styling", () => {
  it("sizes places on a log scale, with a floor for empty ones", () => {
    expect(radiusFor(0)).toBe(1.5);
    expect(radiusFor(9)).toBeCloseTo(1.2 + 1.9);
    expect(radiusFor(999)).toBeCloseTo(1.2 + 1.9 * 3);
  });
  it("lightens a colour towards white", () => {
    expect(lighten("#000000", 0.5)).toBe("rgb(128, 128, 128)");
    expect(lighten("#5b4b8a", 0)).toBe("rgb(91, 75, 138)");
  });
});

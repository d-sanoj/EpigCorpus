import L from "leaflet";
import { describePixels, groupIntoCells, isMostlyBright, lighten, radiusFor } from "./epigMap";

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

describe("grouping known places", () => {
  // A stand-in projection: one degree is ten pixels.
  const toPixel = (p: { latitude: number; longitude: number }) =>
    L.point(p.longitude * 10, -p.latitude * 10);
  const place = (latitude: number, longitude: number, count: number) => ({
    place: "x", latitude, longitude, count,
  });

  it("sums places and inscriptions per cell, at the weighted centre", () => {
    const cells = groupIntoCells(
      [place(40.1, 10.1, 1), place(40.2, 10.3, 3), place(45, 20, 0)],
      toPixel,
      26,
    );
    expect(cells).toHaveLength(2);
    const [empty, full] = cells; // fullest last
    expect(empty).toMatchObject({ places: 1, inscriptions: 0, intensity: 0 });
    expect(full).toMatchObject({ places: 2, inscriptions: 4, intensity: 1 });
    expect(full.center.lat).toBeCloseTo((40.1 * 1 + 40.2 * 3) / 4);
    expect(full.center.lng).toBeCloseTo((10.1 * 1 + 10.3 * 3) / 4);
  });
});

describe("spotting CARTO's error tile", () => {
  const pixels = (count: number, rgb: [number, number, number]) =>
    Array.from({ length: count }, () => [...rgb, 255]).flat();

  it("accepts a dark basemap tile", () => {
    // dark_nolabels: near-black sea, dark grey land, a lighter coastline.
    const tile = [...pixels(900, [14, 14, 14]), ...pixels(80, [38, 38, 38]), ...pixels(20, [80, 80, 80])];
    expect(isMostlyBright(tile)).toBe(false);
  });

  it("rejects a tile with light text or a light background", () => {
    const tile = [...pixels(900, [14, 14, 14]), ...pixels(100, [230, 230, 230])];
    expect(isMostlyBright(tile)).toBe(true);
    expect(isMostlyBright(pixels(1000, [240, 240, 240]))).toBe(true);
  });
});

describe("tile signatures", () => {
  const tile = (shade: number, accent: number) =>
    Array.from({ length: 64 }, (_, i) => (i === 10 ? [accent, accent, accent, 255] : [shade, shade, shade, 255])).flat();

  it("calls a single-colour tile flat", () => {
    expect(describePixels(tile(14, 14)).flat).toBe(true);
    expect(describePixels(tile(14, 90)).flat).toBe(false);
  });

  it("gives equal tiles equal signatures and different tiles different ones", () => {
    expect(describePixels(tile(14, 90)).signature).toBe(describePixels(tile(14, 90)).signature);
    expect(describePixels(tile(14, 90)).signature).not.toBe(describePixels(tile(14, 91)).signature);
  });
});

import L from "leaflet";
import { groupIntoCells, lighten, radiusFor, withoutLabels } from "./epigMap";

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

describe("the basemap style", () => {
  it("loses its text and icon layers, and keeps the rest in order", () => {
    const style = {
      version: 8 as const,
      sources: {},
      layers: [
        { id: "background", type: "background" as const },
        { id: "place_label", type: "symbol" as const, source: "x" },
        { id: "water", type: "fill" as const, source: "x" },
        { id: "road_label", type: "symbol" as const, source: "x" },
      ],
    };
    expect(withoutLabels(style).layers.map((layer) => layer.id)).toEqual(["background", "water"]);
    expect(style.layers).toHaveLength(4); // the original is left alone
  });
});

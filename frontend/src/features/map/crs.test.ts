import { CENTER_LAT, CENTER_LON, project, unproject } from "./crs";

describe("equal-area projection", () => {
  it("puts the centre at the origin", () => {
    const [x, y] = project(CENTER_LAT, CENTER_LON);
    expect(Math.abs(x)).toBeLessThan(1e-6);
    expect(Math.abs(y)).toBeLessThan(1e-6);
  });

  it("round-trips places across the empire", () => {
    for (const [lat, lon] of [
      [41.9, 12.5], // Roma
      [51.5, -0.1], // Londinium
      [30.0, 31.2], // Memphis
      [38.0, -9.1], // Olisipo
      [36.2, 37.2], // Beroea
      [70.0, -40.0], // Greenland, far off the empire
    ]) {
      const [back_lat, back_lon] = unproject(...project(lat, lon));
      expect(back_lat).toBeCloseTo(lat, 9);
      expect(back_lon).toBeCloseTo(lon, 9);
    }
  });

  it("keeps areas true: equal 1° cells at 30° and 55° differ only by cos(latitude)", () => {
    const area = (lat: number) => {
      const [x0, y0] = project(lat, 10);
      const [x1, y1] = project(lat, 10.01);
      const [x2, y2] = project(lat + 0.01, 10);
      return Math.abs((x1 - x0) * (y2 - y0) - (y1 - y0) * (x2 - x0));
    };
    const ratio = area(55) / area(30);
    const expected = Math.cos(55 * Math.PI / 180) / Math.cos(30 * Math.PI / 180);
    expect(ratio).toBeCloseTo(expected, 3);
  });
});

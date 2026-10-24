/* The map's coordinate system: Lambert azimuthal equal-area, centred on the
 * Mediterranean.
 *
 * Web Mercator (what tile maps such as CARTO use) inflates areas with
 * latitude: Britannia drawn half again as large as it is next to Africa
 * proconsularis. Our map draws only vector data, so it can use an equal-area
 * projection instead: every province and every cluster of places keeps its
 * true relative size, and shapes stay close to true across the empire.
 *
 * Spherical formulas (Snyder, Map Projections: A Working Manual, pp. 185-187);
 * the backend clips land to the same centre (config/settings.py MAP_CENTER).
 */
import L from "leaflet";

export const CENTER_LAT = 40;
export const CENTER_LON = 15;
const R = 6371008.8; // mean Earth radius, metres
const RAD = Math.PI / 180;
const phi1 = CENTER_LAT * RAD;
const sinPhi1 = Math.sin(phi1);
const cosPhi1 = Math.cos(phi1);

export function project(lat: number, lon: number): [number, number] {
  const phi = lat * RAD;
  const dLambda = (lon - CENTER_LON) * RAD;
  const cosC = sinPhi1 * Math.sin(phi) + cosPhi1 * Math.cos(phi) * Math.cos(dLambda);
  // The antipode has no position; nothing we draw comes near it.
  const k = Math.sqrt(2 / Math.max(1e-12, 1 + cosC));
  return [
    R * k * Math.cos(phi) * Math.sin(dLambda),
    R * k * (cosPhi1 * Math.sin(phi) - sinPhi1 * Math.cos(phi) * Math.cos(dLambda)),
  ];
}

export function unproject(x: number, y: number): [number, number] {
  const rho = Math.hypot(x, y);
  if (rho < 1e-9) return [CENTER_LAT, CENTER_LON];
  const c = 2 * Math.asin(Math.min(1, rho / (2 * R)));
  const sinC = Math.sin(c);
  const cosC = Math.cos(c);
  const lat = Math.asin(cosC * sinPhi1 + (y * sinC * cosPhi1) / rho) / RAD;
  const lon =
    CENTER_LON + Math.atan2(x * sinC, rho * cosPhi1 * cosC - y * sinPhi1 * sinC) / RAD;
  return [lat, ((lon + 540) % 360) - 180];
}

const projection: L.Projection = {
  project: (latlng: L.LatLng) => {
    const [x, y] = project(latlng.lat, latlng.lng);
    return new L.Point(x, y);
  },
  unproject: (point: L.Point) => {
    const [lat, lon] = unproject(point.x, point.y);
    return new L.LatLng(lat, lon);
  },
  bounds: new L.Bounds([-2 * R, -2 * R], [2 * R, 2 * R]),
};

// Metres per pixel at zoom 0. Chosen so each zoom level shows the empire at
// the scale Web Mercator showed it at latitude 40°, keeping the old zoom
// levels, marker sizes and default view.
const RESOLUTION_0 = (2 * Math.PI * 6378137 * Math.cos(CENTER_LAT * RAD)) / 256;

export const EqualArea: L.CRS = L.Util.extend({}, L.CRS.Earth, {
  code: "EPIGCORPUS:LAEA",
  projection,
  transformation: new L.Transformation(1, 0, -1, 0),
  scale: (zoom: number) => Math.pow(2, zoom) / RESOLUTION_0,
  zoom: (scale: number) => Math.log2(scale * RESOLUTION_0),
  infinite: true,
  wrapLng: undefined,
});

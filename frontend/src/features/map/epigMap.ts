/* The interactive map: one solid circle per place, sized by inscription count.
 *
 * Plain Leaflet, kept imperative on purpose: the layer stacking below relies on
 * every marker sharing one canvas in a fixed draw order, which a declarative
 * wrapper would fight. React owns the page; this module owns the map element.
 *
 * The background is CARTO's dark basemap without labels, set up exactly as the
 * original Streamlit map on main set it up (src/edcs_streamlit_map.py). The
 * only names on the map are the EDCS province names, drawn in the colour of
 * the shading beneath them.
 *
 * Known places (about 22,000) are grouped into a grid of cells while zoomed
 * out, one dot per cell shaded by its inscriptions, and drawn one by one from
 * DETAIL_ZOOM in. Search results are always drawn one by one.
 */
import L from "leaflet";
import type { CityPoint, ProvinceLabel } from "../../api/client";
import { formatInteger, plural } from "../../lib/format";

// Solid markers in bright, well-separated hues over muted province fills.
// A thin dark outline keeps neighbouring and overlapping circles apart.
export const COLORS = {
  known: "#2dd4bf", // teal: every known place
  empty: "#64748b", // slate: a place with no inscription
  city: "#facc15", // yellow: cities (Hanson 2016)
  search: "#f97316", // orange: search results
  road: "#e2e8f0",
  edge: "#0b0f17",
  labelFallback: "#cbd5e1",
};
const PROVINCE_FILL_OPACITY = 0.6;
// Results sit on top of everything and must stay findable even where a place
// has a single match: they never draw smaller than this.
const SEARCH_MIN_RADIUS = 3.6;
const CITY_RADIUS = 2.1;
const RADIUS_BASE = 1.2;
const RADIUS_SCALE = 1.9;
const EMPTY_RADIUS = 1.5;
export const LEGEND_STOPS = [1, 10, 100, 1000];
const CENTER: L.LatLngTuple = [41.9028, 12.4964]; // Rome, as EDCS
const ZOOM = 4;
// Known places are drawn one by one from this zoom in, grouped below it.
export const DETAIL_ZOOM = 7;
// Known places drawn one by one never grow past their default-zoom size.
const KNOWN_CAP = 1;
// Grid cell size on screen while grouped, in pixels.
const CELL_PX = 26;
// CARTO's dark basemap without labels, as on main (CARTO_PLAIN there).
const CARTO_URL = "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png";
const CARTO_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> ' +
  '&copy; <a href="https://carto.com/attributions">CARTO</a>';

export interface MapPlace {
  place: string;
  latitude: number;
  longitude: number;
  count: number;
  examples?: string[];
}

export type LayerKey = "all" | "search" | "provinces" | "roads" | "cities";
export type LayerState = Record<LayerKey, { available: boolean; on: boolean }>;

/** Size is continuous in the count, on a log scale: counts span 1 to thousands. */
export function radiusFor(count: number): number {
  if (count <= 0) return EMPTY_RADIUS;
  return RADIUS_BASE + (RADIUS_SCALE * Math.log(count + 1)) / Math.LN10;
}

/** Mix a #rrggbb colour towards white by `amount` (0..1). */
export function lighten(hex: string, amount: number): string {
  const n = parseInt(hex.slice(1), 16);
  const channels = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map((c) =>
    Math.round(c + (255 - c) * amount),
  );
  return `rgb(${channels.join(", ")})`;
}

function escapeHtml(value: unknown): string {
  return String(value).replace(
    /[&<>"']/g,
    (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]!,
  );
}

// Order inside the shared canvas: later layers draw on top and win the hover.
const LAYER_ORDER: LayerKey[] = ["all", "roads", "cities", "search"];

export class EpigMap {
  private readonly map: L.Map;
  private readonly markerRenderer: L.Canvas;
  private readonly layers: Record<LayerKey | "provinceLabels", L.LayerGroup>;
  // Markers whose radius follows the zoom; `cap` limits how far they grow.
  private scaled: { marker: L.CircleMarker; base: number; cap?: number }[] = [];
  private labelMarkers: L.Marker[] = [];
  private available: Record<LayerKey, boolean> = {
    all: false, search: false, provinces: false, roads: false, cities: false,
  };
  // Provinces are the base and start switched on; roads and cities are optional.
  private wanted: Record<LayerKey, boolean> = {
    all: true, search: true, provinces: true, roads: false, cities: false,
  };
  // "known" with no query, "results" with one. The known-places layer can be
  // switched on or off in either; each mode remembers its own choice. By
  // default it shows with no query and steps aside for a query's results.
  private mode: "known" | "results" = "known";
  private knownWanted = { known: true, results: false };
  private knownPlaces: MapPlace[] | null = null;
  // The zoom the known-places layer was last drawn for (null: redraw).
  private knownDrawnAt: number | null = null;
  private listeners = new Set<(state: LayerState) => void>();

  constructor(element: HTMLElement) {
    // The same map options as main: whole zoom steps, so tiles are always
    // drawn at their own size.
    const map = L.map(element, { preferCanvas: true, worldCopyJump: true }).setView(CENTER, ZOOM);
    this.map = map;

    // Stacking, bottom to top: land, provinces (the base), known places, roads,
    // cities, search results; province names above all, as text. Leaflet binds
    // mouse events to each canvas element, so only the TOPMOST canvas can be
    // hovered. Places, roads, cities and results therefore share one canvas,
    // where stacking follows draw order (LAYER_ORDER); roads are drawn there as
    // non-interactive lines, so hovering passes through them.
    this.pane("provincesPane", 400, false);
    this.pane("markersPane", 410, true);
    this.pane("labelsPane", 450, false);
    this.addBasemap();

    this.markerRenderer = L.canvas({ pane: "markersPane", padding: 0.5, tolerance: 5 });
    this.layers = {
      all: L.layerGroup(),
      search: L.layerGroup(),
      provinces: L.layerGroup(),
      roads: L.layerGroup(),
      cities: L.layerGroup(),
      provinceLabels: L.layerGroup(),
    };

    map.on("zoomend", () => {
      this.applyZoomScale();
      this.drawKnown();
    });
    map.on("zoomend moveend", () => this.declutter());
    this.addResetControl();
  }

  destroy() {
    this.listeners.clear();
    this.map.remove();
  }

  /** The layer toggles' state, for the React layer panel. */
  onLayersChange(listener: (state: LayerState) => void): () => void {
    this.listeners.add(listener);
    listener(this.layerState());
    return () => this.listeners.delete(listener);
  }

  layerState(): LayerState {
    const state = {} as LayerState;
    for (const key of Object.keys(this.available) as LayerKey[]) {
      state[key] = { available: this.available[key], on: this.available[key] && this.wanted[key] };
    }
    return state;
  }

  setLayerWanted(key: LayerKey, on: boolean) {
    this.wanted[key] = on;
    if (key === "all") this.knownWanted[this.mode] = on;
    this.setLayerVisible(key);
    this.emit();
  }

  resetView() {
    this.map.setView(CENTER, ZOOM);
  }

  invalidateSize() {
    this.map.invalidateSize();
  }

  // ─── data ──────────────────────────────────────────────────────────────────

  // ─── basemap ───────────────────────────────────────────────────────────────

  private addBasemap() {
    L.tileLayer(CARTO_URL, {
      maxZoom: 19,
      subdomains: "abcd",
      attribution: CARTO_ATTRIBUTION,
    }).addTo(this.map);
  }

  /** Every known place, built once; usable with or without a query. */
  setKnown(places: MapPlace[]) {
    if (this.knownPlaces === places) return;
    this.knownPlaces = places;
    this.knownDrawnAt = null;
    this.drawKnown();
    this.available.all = places.length > 0;
    this.applyMode();
    this.restackAbove("all");
  }

  /** No query: the corpus-wide teal layer, every known place. */
  showKnown(places: MapPlace[]) {
    this.mode = "known";
    this.forget(this.layers.search);
    this.available.search = false;
    this.setKnown(places);
    this.applyMode();
  }

  /** A query: the orange results go on top and the view fits them. Known
   *  places stay available underneath, off unless switched on. */
  showResults(places: MapPlace[]) {
    this.mode = "results";
    this.forget(this.layers.search);
    for (const place of places) this.resultMarker(place).addTo(this.layers.search);
    this.available.search = places.length > 0;
    this.applyMode();
    if (places.length) {
      // The pane may have just changed size (the results table opens under
      // it); fit against its current size, not the one Leaflet last measured.
      this.map.invalidateSize();
      const lats = places.map((p) => p.latitude);
      const lons = places.map((p) => p.longitude);
      this.map.fitBounds(
        [
          [Math.min(...lats), Math.min(...lons)],
          [Math.max(...lats), Math.max(...lons)],
        ],
        { padding: [24, 24] },
      );
    }
  }

  setProvinces(data: GeoJSON.GeoJsonObject) {
    this.layers.provinces = L.geoJSON(data, {
      pane: "provincesPane",
      style: (feature) => ({
        fillColor: (feature?.properties && feature.properties._color) || "#f0f0f0",
        color: "rgba(226, 232, 240, 0.28)",
        weight: 0.7,
        fillOpacity: PROVINCE_FILL_OPACITY,
      }),
    });
    this.overlayReady("provinces");
  }

  setRoads(data: GeoJSON.GeoJsonObject) {
    // Leaflet hands extra options on to each path it creates; the typings
    // leave `renderer` out of GeoJSONOptions.
    this.layers.roads = L.geoJSON(data, {
      renderer: this.markerRenderer,
      interactive: false,
      style: () => ({ color: COLORS.road, weight: 1.1, opacity: 0.7 }),
    } as L.GeoJSONOptions);
    this.overlayReady("roads");
  }

  setCities(cities: CityPoint[]) {
    this.layers.cities = L.layerGroup(
      cities.map((city) => {
        const dot = L.circleMarker([city.latitude, city.longitude], {
          renderer: this.markerRenderer,
          radius: CITY_RADIUS * this.zoomScale(),
          color: COLORS.edge,
          weight: 0.8,
          opacity: 1,
          fillColor: COLORS.city,
          fillOpacity: 0.95,
        });
        dot.bindTooltip(escapeHtml(city.label || "Unnamed city"), { direction: "top", sticky: true });
        this.scaled.push({ marker: dot, base: CITY_RADIUS });
        return dot;
      }),
    );
    this.overlayReady("cities");
  }

  setProvinceLabels(labels: ProvinceLabel[]) {
    this.labelMarkers = labels.map((label) => {
      const text = document.createElement("span");
      text.textContent = label.name;
      // The fills are muted; the name uses a lighter tint of the same hue.
      text.style.color = label.color ? lighten(label.color, 0.55) : COLORS.labelFallback;
      return L.marker([label.latitude, label.longitude], {
        pane: "labelsPane",
        interactive: false,
        keyboard: false,
        icon: L.divIcon({ className: "province-label", html: text, iconSize: [0, 0] }),
      });
    });
    for (const marker of this.labelMarkers) this.layers.provinceLabels.addLayer(marker);
    this.setLayerVisible("provinces");
  }

  // ─── internals ─────────────────────────────────────────────────────────────

  private pane(name: string, zIndex: number, interactive: boolean) {
    const pane = this.map.createPane(name);
    pane.style.zIndex = String(zIndex);
    if (!interactive) pane.style.pointerEvents = "none";
  }

  private emit() {
    const state = this.layerState();
    for (const listener of this.listeners) listener(state);
  }

  // Radii are calibrated for the default zoom and scale with the zoom level.
  private zoomScale(): number {
    return Math.min(2.2, Math.max(0.62, Math.pow(2, (this.map.getZoom() - ZOOM) * 0.3)));
  }

  private applyZoomScale() {
    const factor = this.zoomScale();
    for (const entry of this.scaled) {
      entry.marker.setRadius(entry.base * Math.min(entry.cap ?? Infinity, factor));
    }
  }

  private forget(group: L.LayerGroup) {
    this.scaled = this.scaled.filter((entry) => !group.hasLayer(entry.marker));
    group.clearLayers();
  }

  private overlayReady(key: LayerKey) {
    this.available[key] = true;
    this.setLayerVisible(key);
    this.emit();
  }

  private applyMode() {
    this.wanted.all = this.knownWanted[this.mode];
    this.setLayerVisible("all");
    this.setLayerVisible("search");
    this.emit();
  }

  private setLayerVisible(key: LayerKey) {
    const layer = this.layers[key];
    const on = this.available[key] && this.wanted[key];
    if (on && !this.map.hasLayer(layer)) {
      layer.addTo(this.map);
      this.restackAbove(key);
    }
    if (!on && this.map.hasLayer(layer)) this.map.removeLayer(layer);
    // The province names come and go with the province shading.
    if (key === "provinces") {
      const labels = this.layers.provinceLabels;
      if (on && this.labelMarkers.length && !this.map.hasLayer(labels)) labels.addTo(this.map);
      if (!on && this.map.hasLayer(labels)) this.map.removeLayer(labels);
      this.declutter();
    }
  }

  // Whenever a layer appears, everything meant to be above it is re-added, so
  // the order holds however and whenever layers load.
  private restackAbove(key: LayerKey) {
    const from = LAYER_ORDER.indexOf(key);
    if (from < 0) return;
    for (const above of LAYER_ORDER.slice(from + 1)) {
      const layer = this.layers[above];
      if (this.map.hasLayer(layer)) {
        this.map.removeLayer(layer);
        layer.addTo(this.map);
      }
    }
  }

  // Hide any name that would overlap one already shown. Labels arrive ordered
  // by inscription count, so the larger province keeps its name.
  private declutter() {
    if (!this.map.hasLayer(this.layers.provinceLabels)) return;
    const shown: DOMRect[] = [];
    for (const marker of this.labelMarkers) {
      const element = marker.getElement();
      const text = element?.firstElementChild;
      if (!element || !text) continue;
      element.style.visibility = "visible";
      const box = text.getBoundingClientRect();
      const clash = shown.some(
        (other) =>
          !(box.right < other.left || box.left > other.right ||
            box.bottom < other.top || box.top > other.bottom),
      );
      if (clash) element.style.visibility = "hidden";
      else shown.push(box);
    }
  }

  /** (Re)build the known-places layer for the current zoom: one dot per place
   *  from DETAIL_ZOOM in, one dot per grid cell below it. */
  private drawKnown() {
    const places = this.knownPlaces;
    if (!places) return;
    const zoom = this.map.getZoom();
    const detailed = zoom >= DETAIL_ZOOM;
    const drawn = this.knownDrawnAt;
    if (drawn !== null && (detailed ? drawn >= DETAIL_ZOOM : drawn === zoom)) return;
    this.knownDrawnAt = zoom;

    this.forget(this.layers.all);
    if (detailed) {
      for (const place of places) this.knownMarker(place).addTo(this.layers.all);
    } else {
      for (const cell of groupIntoCells(places, (p) => this.map.project([p.latitude, p.longitude], zoom))) {
        this.cellMarker(cell).addTo(this.layers.all);
      }
    }
    // New paths go to the top of the shared canvas: put the layers that
    // belong above known places back above them.
    if (this.map.hasLayer(this.layers.all)) this.restackAbove("all");
  }

  private cellMarker(cell: Cell): L.CircleMarker {
    const intensity = cell.intensity;
    const marker = L.circleMarker(cell.center, {
      renderer: this.markerRenderer,
      radius: CELL_PX * (0.12 + 0.28 * intensity),
      color: COLORS.edge,
      weight: 0.6,
      opacity: 0.8,
      fillColor: cell.inscriptions > 0 ? COLORS.known : COLORS.empty,
      fillOpacity: 0.3 + 0.6 * intensity,
      fill: true,
    });
    marker.bindTooltip(
      `${plural(cell.places, "place", "places")} · ` +
        `${plural(cell.inscriptions, "inscription", "inscriptions")}` +
        '<br><span class="tooltip-hint">Click or zoom in to see each place</span>',
      { direction: "top", sticky: true },
    );
    marker.on("click", () => {
      const target = Math.min(DETAIL_ZOOM, this.map.getZoom() + 2);
      this.map.setView(cell.center, target);
    });
    return marker;
  }

  private knownMarker(place: MapPlace): L.CircleMarker {
    const { count } = place;
    const hasInscriptions = count > 0;
    const base = radiusFor(count);
    const marker = L.circleMarker([place.latitude, place.longitude], {
      renderer: this.markerRenderer,
      // Zoomed in this far, places are many and close: they keep their
      // default-zoom size rather than growing with the zoom.
      radius: base * Math.min(KNOWN_CAP, this.zoomScale()),
      color: COLORS.edge,
      weight: 0.8,
      opacity: 0.9,
      fillColor: hasInscriptions ? COLORS.known : COLORS.empty,
      fillOpacity: hasInscriptions ? 0.75 : 0.6,
      fill: true,
    });
    const name = escapeHtml(place.place || "?");
    marker.bindTooltip(
      hasInscriptions
        ? `${name} · ${plural(count, "inscription", "inscriptions")}`
        : `${name} · no inscriptions in this corpus`,
      { direction: "top", sticky: true },
    );
    this.scaled.push({ marker, base, cap: KNOWN_CAP });
    return marker;
  }

  private resultMarker(place: MapPlace): L.CircleMarker {
    const { count } = place;
    const items = place.examples ?? [];
    const name = escapeHtml(place.place || "?");
    const base = Math.max(SEARCH_MIN_RADIUS, radiusFor(count) + 1);
    const marker = L.circleMarker([place.latitude, place.longitude], {
      renderer: this.markerRenderer,
      radius: base * this.zoomScale(),
      color: COLORS.edge,
      weight: 1.1,
      opacity: 1,
      fillColor: COLORS.search,
      fillOpacity: 0.95,
      fill: true,
    });
    const matches = plural(count, "match", "matches");
    marker.bindTooltip(`${name} · ${matches}`, { direction: "top", sticky: true });
    const listed = items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const remainder = count - items.length;
    marker.bindPopup(
      `<div class="overview-popup"><h4>${name}<br>${matches}</h4><ul>${listed}</ul>` +
        (remainder > 0 ? `<div class="more">+ ${formatInteger(remainder)} more</div>` : "") +
        "</div>",
      { maxWidth: 420 },
    );
    this.scaled.push({ marker, base });
    return marker;
  }

  private addResetControl() {
    const reset = new L.Control({ position: "topleft" });
    reset.onAdd = () => {
      const wrapper = L.DomUtil.create("div", "leaflet-bar overview-map-reset");
      const button = L.DomUtil.create("button", "", wrapper);
      button.type = "button";
      button.title = "Reset view";
      button.setAttribute("aria-label", "Reset view");
      button.innerHTML =
        '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">' +
        '<path d="M12 3l9 8h-3v9h-5v-6h-2v6H6v-9H3z" fill="currentColor"></path></svg>';
      L.DomEvent.disableClickPropagation(wrapper);
      L.DomEvent.on(button, "click", () => this.resetView());
      return wrapper;
    };
    reset.addTo(this.map);
  }
}

export interface Cell {
  center: L.LatLng;
  places: number;
  inscriptions: number;
  /** 0..1: log of the cell's inscriptions against the fullest cell's. */
  intensity: number;
}

/** Group places into square screen-space cells of CELL_PX at one zoom.
 *  A cell sits at the inscription-weighted centre of its places. */
export function groupIntoCells(
  places: MapPlace[],
  toPixel: (place: MapPlace) => L.Point,
  cellPx = CELL_PX,
): Cell[] {
  const cells = new Map<string, { lat: number; lon: number; weight: number; places: number; inscriptions: number }>();
  for (const place of places) {
    const pixel = toPixel(place);
    const key = `${Math.floor(pixel.x / cellPx)}:${Math.floor(pixel.y / cellPx)}`;
    const weight = Math.max(1, place.count);
    const cell = cells.get(key);
    if (cell) {
      cell.lat += place.latitude * weight;
      cell.lon += place.longitude * weight;
      cell.weight += weight;
      cell.places += 1;
      cell.inscriptions += place.count;
    } else {
      cells.set(key, {
        lat: place.latitude * weight,
        lon: place.longitude * weight,
        weight,
        places: 1,
        inscriptions: place.count,
      });
    }
  }
  const largest = Math.max(1, ...[...cells.values()].map((cell) => cell.inscriptions));
  return [...cells.values()]
    .map((cell) => ({
      center: L.latLng(cell.lat / cell.weight, cell.lon / cell.weight),
      places: cell.places,
      inscriptions: cell.inscriptions,
      intensity: Math.log1p(cell.inscriptions) / Math.log1p(largest),
    }))
    // Fullest cells last, so they draw on top.
    .sort((a, b) => a.inscriptions - b.inscriptions);
}

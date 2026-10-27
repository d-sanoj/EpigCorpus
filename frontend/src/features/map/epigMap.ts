/* The interactive map: one solid circle per place, sized by inscription count.
 *
 * Plain Leaflet, kept imperative on purpose: the layer stacking below relies on
 * every marker sharing one canvas in a fixed draw order, which a declarative
 * wrapper would fight. React owns the page; this module owns the map element.
 *
 * The background is an OpenFreeMap vector basemap, drawn by MapLibre GL inside
 * the Leaflet map (maplibre-gl-leaflet): "positron" in the light theme, "dark"
 * in the dark one, the same designs CARTO served, with no API key and no usage
 * limit. Their place-name layers are removed, so the only names on the map
 * are the EDCS province names, drawn in the colour of the shading beneath
 * them. Every colour on the map comes from the theme's palette, and changing
 * theme restyles everything in place.
 *
 * Known places (about 22,000) are grouped into a grid of cells while zoomed
 * out, one dot per cell shaded by its inscriptions, and drawn one by one from
 * DETAIL_ZOOM in. Search results are always drawn one by one.
 */
import L from "leaflet";
import type { StyleSpecification } from "maplibre-gl";
import type { CityPoint, ProvinceLabel } from "../../api/client";
import { formatInteger, plural } from "../../lib/format";

export type MapTheme = "light" | "dark";

// Solid markers in well-separated hues over muted province fills: teal known
// places, terracotta search results, ochre cities. A thin outline in the
// basemap's own tone keeps neighbouring and overlapping circles apart.
export const PALETTES = {
  light: {
    known: "#0f766e",
    empty: "#a8a29e",
    city: "#b7791f",
    search: "#e05a1c",
    road: "#57534e",
    edge: "#ffffff",
    provinceOpacity: 0.3,
    provinceBorder: "rgba(28, 25, 23, 0.22)",
    labelFallback: "#44403c",
  },
  dark: {
    known: "#2dd4bf",
    empty: "#64748b",
    city: "#f5c542",
    search: "#fb7a3c",
    road: "#e2e8f0",
    edge: "#0b0f17",
    provinceOpacity: 0.55,
    provinceBorder: "rgba(226, 232, 240, 0.28)",
    labelFallback: "#cbd5e1",
  },
} as const;

// OpenFreeMap: free vector tiles from OpenStreetMap data, no key, no limits.
// Each style carries its attribution (OpenFreeMap, OpenMapTiles, OpenStreetMap).
export const BASEMAP_STYLES: Record<MapTheme, string> = {
  light: "https://tiles.openfreemap.org/styles/positron",
  dark: "https://tiles.openfreemap.org/styles/dark",
};
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

/** Mix a #rrggbb colour towards black by `amount` (0..1). */
export function darken(hex: string, amount: number): string {
  const n = parseInt(hex.slice(1), 16);
  const channels = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map((c) =>
    Math.round(c * (1 - amount)),
  );
  return `rgb(${channels.join(", ")})`;
}

/** How deep a canvas sits in the stack: its pane's z-index. */
function paneDepth(canvas: HTMLCanvasElement): number {
  const pane = canvas.closest(".leaflet-pane") ?? canvas.parentElement;
  return pane ? Number(getComputedStyle(pane).zIndex) || 0 : 0;
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
  private removed = false;
  private theme: MapTheme;
  private basemap: L.Layer | null = null;
  private basemapRequest = 0;
  private cityMarkers: L.CircleMarker[] = [];
  private resultMarkers: L.CircleMarker[] = [];
  private provinceLabels: ProvinceLabel[] = [];

  constructor(element: HTMLElement, theme: MapTheme) {
    this.theme = theme;
    // Zoom buttons are React's (MapView); attribution stays Leaflet's.
    const map = L.map(element, {
      preferCanvas: true,
      worldCopyJump: true,
      zoomControl: false,
    }).setView(CENTER, ZOOM);
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
  }

  private get colors() {
    return PALETTES[this.theme];
  }

  /** Switch theme: basemap, markers, provinces, roads and labels restyle in place. */
  setTheme(theme: MapTheme) {
    if (theme === this.theme) return;
    this.theme = theme;
    this.addBasemap();
    const colors = this.colors;
    if (this.layers.provinces instanceof L.GeoJSON) {
      (this.layers.provinces as L.GeoJSON).setStyle((feature) => this.provinceStyle(feature));
    }
    if (this.layers.roads instanceof L.GeoJSON) {
      (this.layers.roads as L.GeoJSON).setStyle({ color: colors.road });
    }
    for (const dot of this.cityMarkers) dot.setStyle({ color: colors.edge, fillColor: colors.city });
    for (const dot of this.resultMarkers) dot.setStyle({ color: colors.edge, fillColor: colors.search });
    this.knownDrawnAt = null;
    this.drawKnown();
    this.colorLabels();
  }

  zoomIn() {
    this.map.zoomIn();
  }

  zoomOut() {
    this.map.zoomOut();
  }

  destroy() {
    this.removed = true;
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

  /**
   * The map as a PNG, for a build with no server to render one.
   *
   * The map is several canvases in stacked panes: the basemap at the bottom,
   * then provinces, then the places and the results. This draws them onto one
   * canvas in that order. The province names are HTML rather than canvas, so
   * they are not in the picture. A canvas the browser will not let this page
   * read (a tile served without CORS) gives no picture at all, rather than a
   * half-drawn one.
   */
  async capturePng(): Promise<Blob | null> {
    const container = this.map.getContainer();
    const { clientWidth: width, clientHeight: height } = container;
    if (!width || !height) return null;

    const canvases = [...container.querySelectorAll("canvas")]
      .map((canvas) => ({ canvas, depth: paneDepth(canvas) }))
      .sort((left, right) => left.depth - right.depth);
    if (!canvases.length) return null;

    const composite = document.createElement("canvas");
    composite.width = width;
    composite.height = height;
    const context = composite.getContext("2d");
    if (!context) return null;

    const frame = container.getBoundingClientRect();
    for (const { canvas } of canvases) {
      const box = canvas.getBoundingClientRect();
      try {
        context.drawImage(
          canvas,
          box.left - frame.left,
          box.top - frame.top,
          box.width,
          box.height,
        );
      } catch {
        return null;
      }
    }
    return new Promise((resolve) => composite.toBlob((blob) => resolve(blob), "image/png"));
  }

  // ─── data ──────────────────────────────────────────────────────────────────

  // ─── basemap ───────────────────────────────────────────────────────────────

  private addBasemap() {
    // MapLibre is about 1 MB: load it apart from the app, so the markers and
    // controls show at once and the background fills in behind them.
    const request = ++this.basemapRequest;
    Promise.all([
      import("@maplibre/maplibre-gl-leaflet"),
      import("maplibre-gl/dist/maplibre-gl.css"),
      fetch(BASEMAP_STYLES[this.theme]).then(
        (response) => response.json() as Promise<StyleSpecification>,
      ),
    ])
      .then(([plugin, , style]) => {
        // A newer theme switch, or the map's removal, wins.
        if (this.removed || request !== this.basemapRequest) return;
        // preserveDrawingBuffer: the PNG export reads this canvas back, and a
        // WebGL buffer that is not preserved is empty by the time it is read.
        const next = plugin.maplibreGL({
          style: withoutLabels(style),
          canvasContextAttributes: { preserveDrawingBuffer: true },
        });
        next.addTo(this.map);
        if (this.basemap) this.map.removeLayer(this.basemap);
        this.basemap = next;
      })
      // Without the basemap the overlays still draw on the plain background.
      .catch(() => {});
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
    this.resultMarkers = places.map((place) => this.resultMarker(place));
    for (const marker of this.resultMarkers) marker.addTo(this.layers.search);
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
        { paddingTopLeft: [32, 88], paddingBottomRight: [32, 72] },
      );
    }
  }

  setProvinces(data: GeoJSON.GeoJsonObject) {
    this.layers.provinces = L.geoJSON(data, {
      pane: "provincesPane",
      style: (feature) => this.provinceStyle(feature),
    });
    this.overlayReady("provinces");
  }

  private provinceStyle(feature?: GeoJSON.Feature): L.PathOptions {
    return {
      fillColor: (feature?.properties && feature.properties._color) || "#f0f0f0",
      color: this.colors.provinceBorder,
      weight: 0.7,
      fillOpacity: this.colors.provinceOpacity,
    };
  }

  setRoads(data: GeoJSON.GeoJsonObject) {
    // Leaflet hands extra options on to each path it creates; the typings
    // leave `renderer` out of GeoJSONOptions.
    this.layers.roads = L.geoJSON(data, {
      renderer: this.markerRenderer,
      interactive: false,
      style: () => ({ color: this.colors.road, weight: 1.1, opacity: 0.7 }),
    } as L.GeoJSONOptions);
    this.overlayReady("roads");
  }

  setCities(cities: CityPoint[]) {
    this.cityMarkers = cities.map((city) => {
        const dot = L.circleMarker([city.latitude, city.longitude], {
          renderer: this.markerRenderer,
          radius: CITY_RADIUS * this.zoomScale(),
          color: this.colors.edge,
          weight: 0.8,
          opacity: 1,
          fillColor: this.colors.city,
          fillOpacity: 0.95,
        });
        dot.bindTooltip(escapeHtml(city.label || "Unnamed city"), { direction: "top", sticky: true });
        this.scaled.push({ marker: dot, base: CITY_RADIUS });
        return dot;
      });
    this.layers.cities = L.layerGroup(this.cityMarkers);
    this.overlayReady("cities");
  }

  setProvinceLabels(labels: ProvinceLabel[]) {
    this.provinceLabels = labels;
    this.labelMarkers = labels.map((label) => {
      const text = document.createElement("span");
      text.textContent = label.name;
      return L.marker([label.latitude, label.longitude], {
        pane: "labelsPane",
        interactive: false,
        keyboard: false,
        icon: L.divIcon({ className: "province-label", html: text, iconSize: [0, 0] }),
      });
    });
    for (const marker of this.labelMarkers) this.layers.provinceLabels.addLayer(marker);
    this.colorLabels();
    this.setLayerVisible("provinces");
  }

  /** Each name in the hue of its province: lighter on the dark map, deeper on
   *  the light one, so it reads against the shading in both. */
  private colorLabels() {
    this.labelMarkers.forEach((marker, index) => {
      const label = this.provinceLabels[index];
      const color = label?.color
        ? this.theme === "dark"
          ? lighten(label.color, 0.55)
          : darken(label.color, 0.35)
        : this.colors.labelFallback;
      const icon = marker.options.icon as L.DivIcon | undefined;
      const html = icon?.options.html;
      if (html instanceof HTMLElement) html.style.color = color;
    });
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
      color: this.colors.edge,
      weight: 0.6,
      opacity: 0.8,
      fillColor: cell.inscriptions > 0 ? this.colors.known : this.colors.empty,
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
      color: this.colors.edge,
      weight: 0.8,
      opacity: 0.9,
      fillColor: hasInscriptions ? this.colors.known : this.colors.empty,
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
      color: this.colors.edge,
      weight: 1.1,
      opacity: 1,
      fillColor: this.colors.search,
      fillOpacity: 0.95,
      fill: true,
    });
    const matches = plural(count, "match", "matches");
    marker.bindTooltip(`${name} · ${matches}`, { direction: "top", sticky: true });
    const listed = items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const remainder = count - items.length;
    marker.bindPopup(
      `<div class="overview-popup"><h4>${name}<br><span>${matches}</span></h4><ul>${listed}</ul>` +
        (remainder > 0 ? `<div class="more">+ ${formatInteger(remainder)} more</div>` : "") +
        "</div>",
      { maxWidth: 420 },
    );
    this.scaled.push({ marker, base });
    return marker;
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

/** The style without its text and icon layers: no place names on the map. */
export function withoutLabels(style: StyleSpecification): StyleSpecification {
  return { ...style, layers: style.layers.filter((layer) => layer.type !== "symbol") };
}

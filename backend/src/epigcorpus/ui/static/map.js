/* The interactive map: one circle per place, sized by inscription count.
 *
 * Ported from the Streamlit app's Leaflet template: same colours,
 * log-scaled radius, zoom scaling, shared marker canvas, layer control, reset
 * control, legend and popups. There is no tile basemap: the background is
 * Natural Earth land outlines served by the API (/v1/map/layers/land), so it
 * needs no tile service, no key and no network, and carries no labels. The
 * only names on the map are the EDCS province names, drawn with the "Roman
 * provinces" layer in the colour of the shading beneath them. The difference is where the
 * data comes from: the API, loaded once for the overlays and the known places
 * and again for each query's results, instead of being inlined per rerun.
 */
(function () {
  "use strict";

  // Solid markers in bright, well-separated hues over muted province fills.
  // A thin dark outline keeps neighbouring and overlapping circles apart.
  var KNOWN_COLOR = "#2dd4bf";   // teal: every known place
  var EMPTY_COLOR = "#64748b";   // slate: a place with no inscription
  var CITY_COLOR = "#facc15";    // yellow: cities (Hanson 2016)
  var SEARCH_COLOR = "#f97316";  // orange: search results
  var ROAD_COLOR = "#e2e8f0";
  var MARKER_EDGE = "#0b0f17";
  var PROVINCE_FILL_OPACITY = 0.6;
  // Results sit on top of everything and must stay findable even where a
  // place has a single match: they never draw smaller than this.
  var SEARCH_MIN_RADIUS = 3.6;
  var CITY_RADIUS = 2.1;
  var RADIUS_BASE = 1.2;
  var RADIUS_SCALE = 1.9;
  var EMPTY_RADIUS = 1.5;
  var LEGEND_STOPS = [1, 10, 100, 1000];
  var CENTER = [41.9028, 12.4964];  // Rome, as EDCS
  var ZOOM = 4;
  // The background: land drawn from data, sea is the map's own background.
  var LAND_FILL = "#1e2633";
  var LAND_EDGE = "#3b475c";
  var ATTRIBUTION = "Land: Natural Earth";
  var LABEL_FALLBACK_COLOR = "#cbd5e1";

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (ch) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch];
    });
  }
  function formatInteger(value) { return Number(value).toLocaleString("en-US"); }
  // Mix a #rrggbb colour towards white by `amount` (0..1).
  function lighten(hex, amount) {
    var n = parseInt(hex.slice(1), 16);
    var channels = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map(function (c) {
      return Math.round(c + (255 - c) * amount);
    });
    return "rgb(" + channels.join(", ") + ")";
  }
  // Size is continuous in the count, on a log scale, because these counts
  // span 1 to several thousand.
  function radiusFor(count) {
    if (count <= 0) return EMPTY_RADIUS;
    return RADIUS_BASE + RADIUS_SCALE * Math.log(count + 1) / Math.LN10;
  }

  function create(element) {
    if (typeof window.L === "undefined") {
      element.innerHTML =
        '<div class="map-fallback">The map library (Leaflet) could not be loaded. ' +
        "Search, results and downloads still work.</div>";
      return null;
    }
    var L = window.L;
    var map = L.map(element, { preferCanvas: true, worldCopyJump: true }).setView(CENTER, ZOOM);

    // Stacking, bottom to top: land, provinces (the base), known places,
    // roads, cities, search results; province names above all, as text.
    // Leaflet binds mouse events to each canvas element, so only the TOPMOST
    // canvas can be hovered. Places, roads, cities and results therefore share
    // one canvas, where stacking follows draw order (see LAYER_ORDER); roads
    // are drawn there as non-interactive lines, so hovering passes through them.
    var provincesPane = map.createPane("provincesPane");
    provincesPane.style.zIndex = "400";
    provincesPane.style.pointerEvents = "none";
    map.createPane("markersPane").style.zIndex = "410";

    // Province names: above everything drawn, never in the way of the mouse.
    var labelsPane = map.createPane("labelsPane");
    labelsPane.style.zIndex = "450";
    labelsPane.style.pointerEvents = "none";

    var landPane = map.createPane("landPane");
    landPane.style.zIndex = "200";
    landPane.style.pointerEvents = "none";
    map.attributionControl.addAttribution(ATTRIBUTION);

    function setLand(data) {
      L.geoJSON(data, {
        pane: "landPane",
        interactive: false,
        style: function () {
          return { fillColor: LAND_FILL, fillOpacity: 1, color: LAND_EDGE, weight: 0.7, opacity: 1 };
        },
      }).addTo(map);
    }

    // Radii are calibrated for the default zoom and scale with the zoom level.
    var scaled = [];
    function zoomScale() {
      return Math.min(2.2, Math.max(0.62, Math.pow(2, (map.getZoom() - ZOOM) * 0.3)));
    }
    function applyZoomScale() {
      var factor = zoomScale();
      for (var i = 0; i < scaled.length; i += 1) {
        scaled[i].marker.setRadius(scaled[i].base * factor);
      }
    }
    map.on("zoomend", applyZoomScale);

    var markerRenderer = L.canvas({ pane: "markersPane", padding: 0.5, tolerance: 5 });
    var layers = {
      all: L.layerGroup(),
      search: L.layerGroup(),
      provinces: L.layerGroup(),
      roads: L.layerGroup(),
      cities: L.layerGroup(),
      provinceLabels: L.layerGroup(),
    };
    var labelMarkers = [];
    var available = { all: false, search: false, provinces: false, roads: false, cities: false };
    // Provinces are the base and start switched on; roads and cities are optional.
    var wanted = { all: true, search: true, provinces: true, roads: false, cities: false };

    function forget(group) {
      scaled = scaled.filter(function (entry) { return !group.hasLayer(entry.marker); });
      group.clearLayers();
    }

    function setLayerVisible(key) {
      var layer = layers[key];
      var on = available[key] && wanted[key];
      if (on && !map.hasLayer(layer)) {
        layer.addTo(map);
        restackAbove(key);
      }
      if (!on && map.hasLayer(layer)) map.removeLayer(layer);
      // The province names come and go with the province shading.
      if (key === "provinces") {
        var labels = layers.provinceLabels;
        if (on && labelMarkers.length && !map.hasLayer(labels)) labels.addTo(map);
        if (!on && map.hasLayer(labels)) map.removeLayer(labels);
        declutter();
      }
    }

    // Hide any name that would overlap one already shown. Labels arrive
    // ordered by inscription count, so the larger province keeps its name.
    function declutter() {
      if (!map.hasLayer(layers.provinceLabels)) return;
      var shown = [];
      labelMarkers.forEach(function (marker) {
        var element = marker.getElement();
        if (!element || !element.firstChild) return;
        element.style.visibility = "visible";
        var box = element.firstChild.getBoundingClientRect();
        var clash = shown.some(function (other) {
          return !(box.right < other.left || box.left > other.right ||
                   box.bottom < other.top || box.top > other.bottom);
        });
        if (clash) element.style.visibility = "hidden";
        else shown.push(box);
      });
    }
    map.on("zoomend moveend", declutter);

    // Order inside the shared canvas: later layers draw on top and win the
    // hover. Whenever a layer appears, everything meant to be above it is
    // re-added, so the order holds however and whenever layers load.
    var LAYER_ORDER = ["all", "roads", "cities", "search"];
    function restackAbove(key) {
      var from = LAYER_ORDER.indexOf(key);
      if (from < 0) return;
      LAYER_ORDER.slice(from + 1).forEach(function (above) {
        var layer = layers[above];
        if (map.hasLayer(layer)) {
          map.removeLayer(layer);
          layer.addTo(map);
        }
      });
    }

    // ─── layer control ───────────────────────────────────────────────────────
    var control = L.control({ position: "topright" });
    var inputs = {};
    control.onAdd = function () {
      var node = L.DomUtil.create("div", "overview-map-layers");
      // Listed in stacking order, topmost first.
      node.innerHTML =
        '<label><input type="checkbox" data-map-layer="search"> <span class="overview-map-layer-swatch search"></span>Search results</label>' +
        '<label><input type="checkbox" data-map-layer="cities"> <span class="overview-map-layer-swatch cities"></span>Cities</label>' +
        '<label><input type="checkbox" data-map-layer="roads"> <span class="overview-map-layer-swatch roads"></span>Roads</label>' +
        '<label><input type="checkbox" data-map-layer="all"> <span class="overview-map-layer-swatch all"></span>Known places</label>' +
        "<hr>" +
        '<label><input type="checkbox" data-map-layer="provinces"> <span class="overview-map-layer-swatch provinces"></span>Roman provinces</label>';
      L.DomEvent.disableClickPropagation(node);
      L.DomEvent.disableScrollPropagation(node);
      node.querySelectorAll("input[data-map-layer]").forEach(function (input) {
        inputs[input.dataset.mapLayer] = input;
        input.addEventListener("change", function () {
          var key = input.dataset.mapLayer;
          wanted[key] = input.checked;
          if (key === "all") knownWanted[mode] = input.checked;
          setLayerVisible(key);
        });
      });
      return node;
    };
    control.addTo(map);

    function syncControl(key) {
      var input = inputs[key];
      if (!input) return;
      input.disabled = !available[key];
      input.checked = available[key] && wanted[key];
    }
    Object.keys(layers).forEach(syncControl);

    // ─── reset view ──────────────────────────────────────────────────────────
    var reset = L.control({ position: "topleft" });
    reset.onAdd = function () {
      var wrapper = L.DomUtil.create("div", "leaflet-bar overview-map-reset");
      var button = L.DomUtil.create("button", "", wrapper);
      button.type = "button";
      button.title = "Reset view";
      button.setAttribute("aria-label", "Reset view");
      button.innerHTML =
        '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">' +
        '<path d="M12 3l9 8h-3v9h-5v-6h-2v6H6v-9H3z" fill="currentColor"></path></svg>';
      L.DomEvent.disableClickPropagation(wrapper);
      L.DomEvent.on(button, "click", function () { map.setView(CENTER, ZOOM); });
      return wrapper;
    };
    reset.addTo(map);

    // ─── legend ──────────────────────────────────────────────────────────────
    var legend = L.control({ position: "bottomright" });
    legend.onAdd = function () {
      var node = L.DomUtil.create("div", "overview-map-legend");
      var rows = LEGEND_STOPS.map(function (stop) {
        var diameter = Math.max(6, radiusFor(stop) * 2);
        return '<div class="overview-map-legend-row"><span class="overview-map-legend-slot">' +
          '<span class="overview-map-legend-dot" style="width:' + diameter + "px;height:" + diameter +
          'px"></span></span><span>' + formatInteger(stop) + "</span></div>";
      }).join("");
      node.innerHTML =
        '<div class="overview-map-legend-title">Inscriptions per place</div>' +
        '<div class="overview-map-legend-series">' +
        '<span class="overview-map-layer-swatch all"></span><span>Known places</span>' +
        '<span class="overview-map-layer-swatch search"></span><span>Search results</span>' +
        '<span class="overview-map-layer-swatch cities"></span><span>Cities</span></div>' + rows;
      L.DomEvent.disableClickPropagation(node);
      return node;
    };
    legend.addTo(map);

    // ─── data ────────────────────────────────────────────────────────────────
    function knownMarker(place) {
      var count = place.count;
      var hasInscriptions = count > 0;
      var base = radiusFor(count);
      var marker = L.circleMarker([place.latitude, place.longitude], {
        renderer: markerRenderer,
        radius: base * zoomScale(),
        color: MARKER_EDGE,
        weight: 0.8,
        opacity: 0.9,
        fillColor: hasInscriptions ? KNOWN_COLOR : EMPTY_COLOR,
        fillOpacity: hasInscriptions ? 0.9 : 0.7,
        fill: true,
      });
      var name = place.place || "?";
      marker.bindTooltip(
        hasInscriptions
          ? escapeHtml(name) + " · " + formatInteger(count) + (count === 1 ? " inscription" : " inscriptions")
          : escapeHtml(name) + " · no inscriptions in this corpus",
        { direction: "top", sticky: true }
      );
      scaled.push({ marker: marker, base: base });
      return marker;
    }

    function resultMarker(place) {
      var count = place.count;
      var items = place.examples || [];
      var name = place.place || "?";
      var base = Math.max(SEARCH_MIN_RADIUS, radiusFor(count) + 1);
      var marker = L.circleMarker([place.latitude, place.longitude], {
        renderer: markerRenderer,
        radius: base * zoomScale(),
        color: MARKER_EDGE,
        weight: 1.1,
        opacity: 1,
        fillColor: SEARCH_COLOR,
        fillOpacity: 0.95,
        fill: true,
      });
      marker.bindTooltip(
        escapeHtml(name) + " · " + formatInteger(count) + (count === 1 ? " match" : " matches"),
        { direction: "top", sticky: true }
      );
      var listed = items.map(function (item) { return "<li>" + escapeHtml(item) + "</li>"; }).join("");
      var remainder = count - items.length;
      marker.bindPopup(
        '<div class="overview-popup"><h4>' + escapeHtml(name) + "<br>" + formatInteger(count) +
        (count === 1 ? " match" : " matches") + "</h4><ul>" + listed + "</ul>" +
        (remainder > 0 ? '<div class="more">+ ' + formatInteger(remainder) + " more</div>" : "") +
        "</div>",
        { maxWidth: 420 }
      );
      scaled.push({ marker: marker, base: base });
      return marker;
    }

    var knownPlaces = null;
    // "known" with no query, "results" with one. The known-places layer can be
    // switched on or off in either; each mode remembers its own choice. By
    // default it shows with no query and steps aside for a query's results.
    var mode = "known";
    var knownWanted = { known: true, results: false };

    function applyMode() {
      wanted.all = knownWanted[mode];
      ["all", "search"].forEach(function (key) { setLayerVisible(key); syncControl(key); });
    }

    // Every known place, built once; usable with or without a query.
    function setKnown(places) {
      if (knownPlaces === places) return;
      forget(layers.all);
      places.forEach(function (place) { knownMarker(place).addTo(layers.all); });
      knownPlaces = places;
      available.all = places.length > 0;
      applyMode();
      restackAbove("all");
    }

    // No query: the corpus-wide teal layer, every known place.
    function showKnown(places) {
      mode = "known";
      forget(layers.search);
      available.search = false;
      setKnown(places);
      applyMode();
    }

    // A query: the orange results go on top and the view fits them. Known
    // places stay available underneath, off unless switched on.
    function showResults(places) {
      mode = "results";
      forget(layers.search);
      places.forEach(function (place) { resultMarker(place).addTo(layers.search); });
      available.search = places.length > 0;
      applyMode();
      if (places.length) {
        var lats = places.map(function (p) { return p.latitude; });
        var lons = places.map(function (p) { return p.longitude; });
        map.fitBounds(
          [[Math.min.apply(null, lats), Math.min.apply(null, lons)],
           [Math.max.apply(null, lats), Math.max.apply(null, lons)]],
          { padding: [24, 24] }
        );
      }
    }

    function setProvinceLabels(data) {
      labelMarkers = data.map(function (label) {
        var text = document.createElement("span");
        text.textContent = label.name;
        // The fills are muted; the name uses a lighter tint of the same hue.
        text.style.color = label.color ? lighten(label.color, 0.55) : LABEL_FALLBACK_COLOR;
        return L.marker([label.latitude, label.longitude], {
          pane: "labelsPane",
          interactive: false,
          keyboard: false,
          icon: L.divIcon({ className: "province-label", html: text, iconSize: [0, 0] }),
        });
      });
      labelMarkers.forEach(function (marker) { layers.provinceLabels.addLayer(marker); });
      setLayerVisible("provinces");
    }

    // The historical overlays arrive once, in the background, switched off.
    function setOverlay(key, data) {
      if (key === "provinceLabels") {
        setProvinceLabels(data);
        return;
      }
      var layer;
      if (key === "provinces") {
        layer = L.geoJSON(data, {
          pane: "provincesPane",
          style: function (feature) {
            return {
              fillColor: (feature.properties && feature.properties._color) || "#f0f0f0",
              color: "rgba(226, 232, 240, 0.28)",
              weight: 0.7,
              fillOpacity: PROVINCE_FILL_OPACITY,
            };
          },
        });
      } else if (key === "roads") {
        layer = L.geoJSON(data, {
          renderer: markerRenderer,
          interactive: false,
          style: function () { return { color: ROAD_COLOR, weight: 1.1, opacity: 0.7 }; },
        });
      } else {
        layer = L.layerGroup(data.map(function (city) {
          var dot = L.circleMarker([city.latitude, city.longitude], {
            renderer: markerRenderer,
            radius: CITY_RADIUS * zoomScale(),
            color: MARKER_EDGE,
            weight: 0.8,
            opacity: 1,
            fillColor: CITY_COLOR,
            fillOpacity: 0.95,
          });
          dot.bindTooltip(escapeHtml(city.label || "Unnamed city"), { direction: "top", sticky: true });
          scaled.push({ marker: dot, base: CITY_RADIUS });
          return dot;
        }));
      }
      layers[key] = layer;
      available[key] = true;
      setLayerVisible(key);
      syncControl(key);
    }

    return {
      setKnown: setKnown,
      showKnown: showKnown,
      showResults: showResults,
      setOverlay: setOverlay,
      setLand: setLand,
    };
  }

  window.EpigMap = { create: create };
})();

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

  var KNOWN_COLOR = "#0A8A9F";   // teal
  var SEARCH_COLOR = "#E37222";  // tangerine
  var CITY_COLOR = "#FFCE00";    // yellow
  var ROAD_COLOR = "#CBD5E1";
  var EMPTY_COLOR = "#64748B";
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

    // Leaflet binds mouse events to each canvas element, so only the TOPMOST
    // interactive canvas hit-tests. Everything hoverable therefore shares one
    // canvas, where stacking and hit priority follow draw order, and the two
    // non-interactive layers get panes that let events fall straight through.
    var provincesPane = map.createPane("provincesPane");
    provincesPane.style.zIndex = "400";
    provincesPane.style.pointerEvents = "none";
    map.createPane("markersPane").style.zIndex = "410";
    var roadsPane = map.createPane("roadsPane");
    roadsPane.style.zIndex = "415";
    roadsPane.style.pointerEvents = "none";

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
    var wanted = { all: true, search: true, provinces: false, roads: false, cities: false };

    function forget(group) {
      scaled = scaled.filter(function (entry) { return !group.hasLayer(entry.marker); });
      group.clearLayers();
    }

    function setLayerVisible(key) {
      var layer = layers[key];
      var on = available[key] && wanted[key];
      if (on && !map.hasLayer(layer)) layer.addTo(map);
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

    // Order matters inside the shared canvas: later layers draw on top and win
    // the hover, so places sit under cities, which sit under search results.
    var MARKER_ORDER = ["all", "cities", "search"];
    function restackAbove(key) {
      var from = MARKER_ORDER.indexOf(key);
      if (from < 0) return;
      MARKER_ORDER.slice(from + 1).forEach(function (above) {
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
      node.innerHTML =
        '<label><input type="checkbox" data-map-layer="all"> <span class="overview-map-layer-swatch all"></span>Known places</label>' +
        '<label><input type="checkbox" data-map-layer="search"> <span class="overview-map-layer-swatch search"></span>Search results</label>' +
        "<hr>" +
        '<label><input type="checkbox" data-map-layer="provinces"> <span class="overview-map-layer-swatch provinces"></span>Roman provinces</label>' +
        '<label><input type="checkbox" data-map-layer="roads"> <span class="overview-map-layer-swatch roads"></span>Roads</label>' +
        '<label><input type="checkbox" data-map-layer="cities"> <span class="overview-map-layer-swatch cities"></span>Cities</label>';
      L.DomEvent.disableClickPropagation(node);
      L.DomEvent.disableScrollPropagation(node);
      node.querySelectorAll("input[data-map-layer]").forEach(function (input) {
        inputs[input.dataset.mapLayer] = input;
        input.addEventListener("change", function () {
          var key = input.dataset.mapLayer;
          wanted[key] = input.checked;
          setLayerVisible(key);
          if (input.checked) restackAbove(key);
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
        '<span class="overview-map-layer-swatch search"></span><span>Search results</span></div>' + rows;
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
        color: hasInscriptions ? KNOWN_COLOR : EMPTY_COLOR,
        weight: 0.6,
        opacity: hasInscriptions ? 0.55 : 0.35,
        fillColor: hasInscriptions ? KNOWN_COLOR : EMPTY_COLOR,
        fillOpacity: hasInscriptions ? 0.2 : 0.12,
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
      var base = radiusFor(count);
      var marker = L.circleMarker([place.latitude, place.longitude], {
        renderer: markerRenderer,
        radius: base * zoomScale(),
        color: SEARCH_COLOR,
        weight: 1,
        opacity: 1,
        fillColor: SEARCH_COLOR,
        fillOpacity: 0.55,
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

    // No query: the corpus-wide teal layer, every known place.
    function showKnown(places) {
      if (knownPlaces !== places) {
        forget(layers.all);
        places.forEach(function (place) { knownMarker(place).addTo(layers.all); });
        knownPlaces = places;
      }
      forget(layers.search);
      available.all = places.length > 0;
      available.search = false;
      ["all", "search"].forEach(function (key) { setLayerVisible(key); syncControl(key); });
      restackAbove("all");
    }

    // A query: the corpus-wide layer comes off, the tangerine results go on,
    // and the view fits them.
    function showResults(places) {
      forget(layers.search);
      places.forEach(function (place) { resultMarker(place).addTo(layers.search); });
      available.all = false;
      available.search = places.length > 0;
      ["all", "search"].forEach(function (key) { setLayerVisible(key); syncControl(key); });
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
        text.style.color = label.color || LABEL_FALLBACK_COLOR;
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
              color: "rgba(226, 232, 240, 0.35)",
              weight: 0.6,
              fillOpacity: 0.3,
            };
          },
        });
      } else if (key === "roads") {
        layer = L.geoJSON(data, {
          pane: "roadsPane",
          style: function () { return { color: ROAD_COLOR, weight: 1, opacity: 0.85 }; },
        });
      } else {
        layer = L.layerGroup(data.map(function (city) {
          var ring = L.circleMarker([city.latitude, city.longitude], {
            renderer: markerRenderer,
            radius: 2.2 * zoomScale(),
            color: CITY_COLOR,
            weight: 1.1,
            opacity: 0.95,
            fill: false,
          });
          ring.bindTooltip(escapeHtml(city.label || "Unnamed city"), { direction: "top", sticky: true });
          scaled.push({ marker: ring, base: 2.2 });
          return ring;
        }));
      }
      layers[key] = layer;
      available[key] = true;
      setLayerVisible(key);
      syncControl(key);
    }

    return {
      showKnown: showKnown,
      showResults: showResults,
      setOverlay: setOverlay,
      setLand: setLand,
    };
  }

  window.EpigMap = { create: create };
})();

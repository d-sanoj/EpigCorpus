from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests


BASE_RAW = "https://raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SUPPORT_DIR = DATA_DIR / "lat_epig_support"
OUTPUT_DIR = ROOT_DIR / "output_maps"
OUTPUT_FILE = OUTPUT_DIR / "edcs_interactive_map.html"


def _download_if_missing(url: str, destination: Path) -> Path:
    if destination.exists():
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    destination.write_bytes(resp.content)
    return destination


def _download_shapefile_components(base_url: str, stem: str, local_dir: Path) -> Path:
    required = [".shp", ".shx", ".dbf", ".prj"]
    optional = [".sbn", ".sbx", ".cpg"]

    for ext in required:
        _download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")

    for ext in optional:
        try:
            _download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")
        except Exception:
            pass

    return local_dir / f"{stem}.shp"


def _load_source_data(source_data: pd.DataFrame | None = None) -> pd.DataFrame:
    if source_data is not None:
        return source_data.copy()

    jsonl_file = DATA_DIR / "edcs_inscriptions.jsonl"
    if not jsonl_file.exists():
        raise FileNotFoundError(f"Missing dataset: {jsonl_file}")
    return pd.read_json(jsonl_file, lines=True)


def _choose_text_column(source_data: pd.DataFrame, text_column: str | None) -> str:
    if text_column and text_column in source_data.columns:
        return text_column

    for candidate in ["inscription_text_interpretive", "inscription_text_conservative", "inscription_text"]:
        if candidate in source_data.columns:
            return candidate

    raise ValueError("No inscription text column found.")


def _normalize_records(source_data: pd.DataFrame, text_column: str, term: str) -> pd.DataFrame:
    map_df = source_data.copy()
    map_df = map_df.replace(r"^\s*$", pd.NA, regex=True)
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude", text_column])
    map_df = map_df[map_df["latitude"].between(-90, 90) & map_df["longitude"].between(-180, 180)]

    if term:
        escaped = re.escape(term.strip())
        pattern = rf"\b{escaped}\b"
        map_df = map_df[map_df[text_column].astype(str).str.contains(pattern, case=False, na=False, regex=True)]

    if map_df.empty:
        raise ValueError(f"No records with term '{term}' and valid coordinates were found.")

    return map_df


def _geojson_string(gdf: gpd.GeoDataFrame) -> str:
    return json.dumps(json.loads(gdf.to_json()), ensure_ascii=False)


def _read_layers() -> tuple[str, str, str]:
    SUPPORT_DIR.mkdir(parents=True, exist_ok=True)

    roads_shp = _download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/ba_roads",
        "ba_roads",
        SUPPORT_DIR / "ba_roads",
    )
    provinces_shp = _download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/cultural_data/political_shading/roman_empire_ad_117/shape",
        "roman_empire_ad_117",
        SUPPORT_DIR / "roman_empire_ad_117",
    )
    cities_csv = _download_if_missing(
        f"{BASE_RAW}/cities/Hanson2016_Cities_OxREP.csv",
        SUPPORT_DIR / "Hanson2016_Cities_OxREP.csv",
    )

    roads = gpd.read_file(roads_shp).to_crs(epsg=4326)
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=4326)

    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"], crs="EPSG:4326"),
    )

    roman_union = provinces.geometry.union_all()
    roads = roads.clip(roman_union)
    cities = cities[cities.geometry.within(roman_union)]

    provinces = provinces.reset_index(drop=True).copy()
    province_colors = [
        "#f6dce0",
        "#dff0e2",
        "#f6f2df",
        "#e7def6",
        "#e8f4da",
        "#f2e6d8",
        "#e4d6ef",
        "#f3dfec",
    ]
    provinces["_color"] = [province_colors[i % len(province_colors)] for i in range(len(provinces))]

    return _geojson_string(provinces), _geojson_string(roads), _geojson_string(cities)


def _build_record_payload(map_df: pd.DataFrame, text_column: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []

    def _coerce_scalar(value: object) -> str:
        if value is None or value is pd.NA:
            return ""
        if pd.isna(value):
            return ""
        return str(value)

    def _coerce_list(value: object) -> list[str]:
        if value is None or value is pd.NA:
            return []
        if isinstance(value, list):
            return [str(item) for item in value if item is not None and item is not pd.NA]
        if pd.isna(value):
            return []
        return [str(value)]

    for _, row in map_df.iterrows():
        records.append(
            {
                "record_id": _coerce_scalar(row.get("record_id", "")),
                "edcs_id": _coerce_scalar(row.get("edcs_id", "")),
                "inscription_index": int(row.get("inscription_index", 0) or 0),
                "latitude": float(row.get("latitude")),
                "longitude": float(row.get("longitude")),
                "province": _coerce_scalar(row.get("province", "")),
                "place": _coerce_scalar(row.get("place", "")),
                "material": _coerce_scalar(row.get("material_en", row.get("material", ""))),
                "language": _coerce_scalar(row.get("language", "")),
                "not_before": _coerce_scalar(row.get("not_before", "")),
                "not_after": _coerce_scalar(row.get("not_after", "")),
                "category": _coerce_list(row.get("category", [])),
                "category_en": _coerce_list(row.get("category_en", [])),
                "belege": _coerce_list(row.get("belege", [])),
                "image_urls": _coerce_scalar(row.get("image_urls", "")),
                "text": _coerce_scalar(row.get(text_column, "")),
            }
        )
    return records


def _escape_js(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _build_html(records: list[dict[str, object]], provinces_geojson: str, roads_geojson: str, cities_geojson: str, title: str) -> str:
    records_json = json.dumps(records, ensure_ascii=False)
    header_title = html.escape(title)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Crimson+Text:wght@400;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: rgba(255, 252, 246, 0.96);
      --panel-strong: #fffaf1;
      --ink: #201b17;
      --muted: #6b6258;
      --line: rgba(70, 56, 41, 0.14);
      --accent: #a3342d;
      --accent-2: #415c76;
      --shadow: 0 18px 45px rgba(35, 26, 18, 0.14);
    }}

    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{
      background:
        radial-gradient(circle at top left, rgba(163, 52, 45, 0.10), transparent 30%),
        radial-gradient(circle at top right, rgba(65, 92, 118, 0.12), transparent 28%),
        linear-gradient(180deg, #f7f1e6 0%, #ede5d8 100%);
      color: var(--ink);
      font-family: Inter, system-ui, sans-serif;
      overflow: hidden;
    }}

    .app {{ display: grid; grid-template-columns: 380px 1fr; height: 100%; }}
    .sidebar {{
      padding: 22px 18px 18px;
      background: var(--panel);
      border-right: 1px solid var(--line);
      box-shadow: var(--shadow);
      z-index: 10;
      overflow: auto;
    }}
    .brand {{
      font-family: "Crimson Text", serif;
      font-size: 34px;
      line-height: 1.05;
      margin: 0 0 8px;
    }}
    .subtitle {{ color: var(--muted); margin: 0 0 18px; font-size: 14px; line-height: 1.45; }}
    .card {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.68);
      border-radius: 16px;
      padding: 14px;
      margin-bottom: 14px;
      backdrop-filter: blur(8px);
    }}
    .label {{ display: block; font-size: 12px; font-weight: 700; letter-spacing: .04em; text-transform: uppercase; margin-bottom: 8px; color: var(--accent-2); }}
    input, select, button {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(60, 47, 35, 0.20);
      background: var(--panel-strong);
      padding: 11px 12px;
      font: inherit;
      color: var(--ink);
      outline: none;
    }}
    input:focus, select:focus, button:focus {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(163, 52, 45, 0.14); }}
    .row {{ display: grid; gap: 10px; }}
    .row.two {{ grid-template-columns: 1fr 1fr; }}
    .actions {{ display: flex; gap: 10px; margin-top: 10px; }}
    .actions button {{ cursor: pointer; font-weight: 700; }}
    .actions .primary {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .actions .ghost {{ background: transparent; }}
    .stats {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .stat {{
      background: rgba(255,255,255,0.75);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
    }}
    .stat .value {{ font-size: 24px; font-weight: 800; color: var(--ink); }}
    .stat .key {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }}
    .results {{ margin-top: 12px; max-height: calc(100vh - 420px); overflow: auto; }}
    .result {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.74);
      padding: 10px 12px;
      margin-bottom: 10px;
      cursor: pointer;
    }}
    .result:hover {{ border-color: rgba(163, 52, 45, 0.30); box-shadow: 0 6px 18px rgba(35, 26, 18, 0.08); }}
    .result-title {{ font-weight: 700; margin-bottom: 4px; }}
    .result-meta {{ font-size: 12px; color: var(--muted); line-height: 1.45; }}
    .result-text {{ margin-top: 8px; font-family: "Crimson Text", serif; font-size: 17px; line-height: 1.32; color: #2a211b; }}
    .empty {{ color: var(--muted); font-style: italic; padding: 10px 2px; }}
    .map-shell {{ position: relative; }}
    #map {{ width: 100%; height: 100%; }}
    .header-tag {{
      position: absolute; top: 16px; left: 16px; z-index: 500;
      background: rgba(255, 250, 241, 0.92);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 9px 14px;
      box-shadow: var(--shadow);
      font-size: 13px;
      font-weight: 700;
      letter-spacing: .01em;
    }}
    .floating {{
      position: absolute; right: 16px; top: 16px; z-index: 500;
      width: 250px;
      background: rgba(255, 250, 241, 0.92);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }}
    .floating h3 {{ margin: 0 0 8px; font-size: 14px; text-transform: uppercase; letter-spacing: .05em; color: var(--accent-2); }}
    .layer-toggle {{ display: flex; align-items: center; gap: 8px; font-size: 14px; margin-bottom: 8px; }}
    .layer-toggle input {{ width: 16px; height: 16px; margin: 0; }}
    .popup-title {{ font-weight: 800; margin-bottom: 4px; }}
    .popup-text {{ font-family: "Crimson Text", serif; font-size: 17px; line-height: 1.35; }}
    @media (max-width: 980px) {{
      body {{ overflow: auto; }}
      .app {{ grid-template-columns: 1fr; height: auto; }}
      .sidebar {{ border-right: none; border-bottom: 1px solid var(--line); box-shadow: none; }}
      .results {{ max-height: none; }}
      .map-shell {{ height: 72vh; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <h1 class="brand">EDCS Interactive Atlas</h1>
      <p class="subtitle">Search inscriptions, switch the text source, and inspect matches on a layered Roman Empire map.</p>

      <div class="card">
        <span class="label">Search term</span>
        <input id="termInput" type="text" value="viator" placeholder="Enter a term, e.g. viator" />
        <div class="row two" style="margin-top: 10px;">
          <div>
            <span class="label">Text field</span>
            <select id="columnSelect"></select>
          </div>
          <div>
            <span class="label">Limit</span>
            <input id="limitInput" type="number" min="1" step="1" value="200" />
          </div>
        </div>
        <div class="actions">
          <button class="primary" id="applyBtn">Update map</button>
          <button class="ghost" id="resetBtn">Reset</button>
        </div>
      </div>

      <div class="card">
        <div class="stats">
          <div class="stat"><div class="value" id="matchCount">0</div><div class="key">Matches</div></div>
          <div class="stat"><div class="value" id="mapCount">0</div><div class="key">Shown</div></div>
          <div class="stat"><div class="value" id="firstMatch">-</div><div class="key">First ID</div></div>
          <div class="stat"><div class="value" id="lastMatch">-</div><div class="key">Last ID</div></div>
        </div>
      </div>

      <div class="card">
        <span class="label">Controls</span>
        <div class="layer-toggle"><input id="toggleProvinces" type="checkbox" checked /> <label for="toggleProvinces">Roman provinces</label></div>
        <div class="layer-toggle"><input id="toggleRoads" type="checkbox" checked /> <label for="toggleRoads">Roads</label></div>
        <div class="layer-toggle"><input id="toggleCities" type="checkbox" checked /> <label for="toggleCities">Cities</label></div>
        <div class="layer-toggle"><input id="toggleMarkers" type="checkbox" checked /> <label for="toggleMarkers">Inscriptions</label></div>
      </div>

      <div class="card">
        <span class="label">Matches</span>
        <div class="results" id="results"></div>
      </div>
    </aside>

    <section class="map-shell">
      <div class="header-tag">{header_title}</div>
      <div class="floating">
        <h3>Legend</h3>
        <div class="layer-toggle"><span style="width: 14px; height: 14px; border-radius: 50%; background: #cb2f2f; display: inline-block; border: 1px solid #2f2f2f;"></span> Inscriptions</div>
        <div class="layer-toggle"><span style="width: 18px; height: 0; border-top: 3px solid #666; display: inline-block;"></span> Roads</div>
        <div class="layer-toggle"><span style="width: 12px; height: 12px; border-radius: 50%; background: #777; display: inline-block;"></span> Cities</div>
      </div>
      <div id="map"></div>
    </section>
  </div>

  <script>
    const RECORDS = {records_json};
    const PROVINCES = {provinces_geojson};
    const ROADS = {roads_geojson};
    const CITIES = {cities_geojson};

    const columnSelect = document.getElementById('columnSelect');
    const termInput = document.getElementById('termInput');
    const limitInput = document.getElementById('limitInput');
    const resultsNode = document.getElementById('results');
    const matchCountNode = document.getElementById('matchCount');
    const mapCountNode = document.getElementById('mapCount');
    const firstMatchNode = document.getElementById('firstMatch');
    const lastMatchNode = document.getElementById('lastMatch');
    const applyBtn = document.getElementById('applyBtn');
    const resetBtn = document.getElementById('resetBtn');

    const map = L.map('map', {{ preferCanvas: true, zoomControl: true }}).setView([41.9, 12.5], 4);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 18
    }}).addTo(map);

    const provinceLayer = L.geoJSON(PROVINCES, {{
      style: feature => ({{
        fillColor: feature.properties._color,
        color: '#c4c4c4',
        weight: 0.5,
        fillOpacity: 0.45
      }})
    }}).addTo(map);

    const roadLayer = L.geoJSON(ROADS, {{
      style: () => ({{ color: '#666666', weight: 1.6, opacity: 0.95 }})
    }}).addTo(map);

    const cityLayer = L.layerGroup().addTo(map);
    CITIES.features.forEach(feature => {{
      const coords = feature.geometry.coordinates;
      L.circleMarker([coords[1], coords[0]], {{
        radius: 1.8,
        color: '#7a7a7a',
        weight: 0.7,
        fill: true,
        fillColor: '#7a7a7a',
        fillOpacity: 0.45
      }}).addTo(cityLayer);
    }});

    const markerLayer = L.layerGroup().addTo(map);
    const markerById = new Map();

    function escapeHtml(value) {{
      return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    function excerpt(text, length = 140) {{
      const clean = String(text || '').replace(/\\s+/g, ' ').trim();
      if (clean.length <= length) return clean;
      return clean.slice(0, length - 1) + '…';
    }}

    function normalizeForSearch(value) {{
      return String(value || '').toLowerCase();
    }}

    function buildPopup(record) {{
      const categories = Array.isArray(record.category_en) ? record.category_en.join(' | ') : String(record.category_en || '');
      const belege = Array.isArray(record.belege) ? record.belege.join(' | ') : String(record.belege || '');
      return `
        <div class="popup-title">${{escapeHtml(record.edcs_id || record.record_id || 'Inscription')}}</div>
        <div class="popup-text">${{escapeHtml(excerpt(record.text))}}</div>
        <div style="margin-top:8px; font-size:12px; line-height:1.45; color:#5f5a52;">
          <div><strong>Place:</strong> ${{escapeHtml(record.place || '-')}}</div>
          <div><strong>Province:</strong> ${{escapeHtml(record.province || '-')}}</div>
          <div><strong>Dates:</strong> ${{escapeHtml([record.not_before, record.not_after].filter(Boolean).join(' – ') || '-')}}</div>
          <div><strong>Language:</strong> ${{escapeHtml(record.language || '-')}}</div>
          <div><strong>Material:</strong> ${{escapeHtml(record.material || '-')}}</div>
          <div><strong>Categories:</strong> ${{escapeHtml(categories || '-')}}</div>
          <div><strong>Belege:</strong> ${{escapeHtml(belege || '-')}}</div>
        </div>
      `;
    }}

    function clearMarkers() {{
      markerLayer.clearLayers();
      markerById.clear();
    }}

    function setLayerState(layer, enabled) {{
      if (enabled) {{
        if (!map.hasLayer(layer)) layer.addTo(map);
      }} else if (map.hasLayer(layer)) {{
        map.removeLayer(layer);
      }}
    }}

    function renderResults(records) {{
      resultsNode.innerHTML = '';
      if (!records.length) {{
        resultsNode.innerHTML = '<div class="empty">No matches found for the current filter.</div>';
        return;
      }}

      records.forEach(record => {{
        const node = document.createElement('div');
        node.className = 'result';
        node.innerHTML = `
          <div class="result-title">${{escapeHtml(record.edcs_id || record.record_id || 'Inscription')}}</div>
          <div class="result-meta">${{escapeHtml([record.place, record.province].filter(Boolean).join(' · ') || '-')}}<br />${{escapeHtml([record.not_before, record.not_after].filter(Boolean).join(' – ') || '-')}}</div>
          <div class="result-text">${{escapeHtml(excerpt(record.text, 200))}}</div>
        `;
        node.addEventListener('click', () => {{
          const marker = markerById.get(record.record_id);
          if (!marker) return;
          map.setView(marker.getLatLng(), Math.max(map.getZoom(), 6), {{ animate: true }});
          marker.openPopup();
        }});
        resultsNode.appendChild(node);
      }});
    }}

    function renderMap(records) {{
      clearMarkers();
      const bounds = [];

      records.forEach(record => {{
        const marker = L.circleMarker([record.latitude, record.longitude], {{
          radius: 5,
          color: '#2f2f2f',
          weight: 0.4,
          fill: true,
          fillColor: '#cb2f2f',
          fillOpacity: 0.86
        }}).bindPopup(buildPopup(record));

        marker.addTo(markerLayer);
        markerById.set(record.record_id, marker);
        bounds.push(marker.getLatLng());
      }});

      if (bounds.length) {{
        map.fitBounds(L.latLngBounds(bounds), {{ padding: [30, 30] }});
      }}
    }}

    function updateStats(allMatches, visibleMatches) {{
      matchCountNode.textContent = allMatches.length.toString();
      mapCountNode.textContent = visibleMatches.length.toString();
      firstMatchNode.textContent = visibleMatches[0]?.edcs_id || '-';
      lastMatchNode.textContent = visibleMatches[visibleMatches.length - 1]?.edcs_id || '-';
    }}

    function filterRecords() {{
      const term = termInput.value.trim().toLowerCase();
      const column = columnSelect.value;
      const limit = Math.max(1, Number.parseInt(limitInput.value || '200', 10));

      const matches = RECORDS.filter(record => {{
        if (!term) return true;
        return normalizeForSearch(record[column]).includes(term);
      }});

      const visible = matches.slice(0, limit);
      updateStats(matches, visible);
      renderResults(visible);
      renderMap(visible);
    }}

    function resetFilters() {{
      termInput.value = 'viator';
      columnSelect.value = 'text';
      limitInput.value = '200';
      document.getElementById('toggleProvinces').checked = true;
      document.getElementById('toggleRoads').checked = true;
      document.getElementById('toggleCities').checked = true;
      document.getElementById('toggleMarkers').checked = true;
      provinceLayer.addTo(map);
      roadLayer.addTo(map);
      cityLayer.addTo(map);
      markerLayer.addTo(map);
      filterRecords();
    }}

    function populateColumns() {{
      const columns = [
        ['text', 'interpretive / source text'],
        ['record_id', 'record id'],
        ['edcs_id', 'EDCS id'],
        ['province', 'province'],
        ['place', 'place'],
        ['language', 'language'],
        ['material', 'material'],
        ['category_en', 'category (EN)']
      ];

      columns.forEach(([value, label]) => {{
        const option = document.createElement('option');
        option.value = value;
        option.textContent = label;
        columnSelect.appendChild(option);
      }});
      columnSelect.value = 'text';
    }}

    document.getElementById('toggleProvinces').addEventListener('change', event => setLayerState(provinceLayer, event.target.checked));
    document.getElementById('toggleRoads').addEventListener('change', event => setLayerState(roadLayer, event.target.checked));
    document.getElementById('toggleCities').addEventListener('change', event => setLayerState(cityLayer, event.target.checked));
    document.getElementById('toggleMarkers').addEventListener('change', event => setLayerState(markerLayer, event.target.checked));

    applyBtn.addEventListener('click', filterRecords);
    resetBtn.addEventListener('click', resetFilters);
    termInput.addEventListener('keydown', event => {{ if (event.key === 'Enter') filterRecords(); }});
    columnSelect.addEventListener('change', filterRecords);
    limitInput.addEventListener('change', filterRecords);

    populateColumns();
    filterRecords();
  </script>
</body>
</html>"""


def build_interactive_map(
    source_data: pd.DataFrame | None = None,
    term: str = "viator",
    text_column: str | None = None,
    output_file: Path = OUTPUT_FILE,
) -> Path:
    source_data = _load_source_data(source_data)
    text_column = _choose_text_column(source_data, text_column)
    map_df = _normalize_records(source_data, text_column=text_column, term=term)

    provinces_geojson, roads_geojson, cities_geojson = _read_layers()
    records = _build_record_payload(map_df, text_column=text_column)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        _build_html(
            records=records,
            provinces_geojson=provinces_geojson,
            roads_geojson=roads_geojson,
            cities_geojson=cities_geojson,
            title=f"Inscriptions containing '{term}'",
        ),
        encoding="utf-8",
    )

    return output_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate an interactive HTML map for EDCS inscriptions.")
    parser.add_argument("--term", default="viator", help="Search term to seed the map with.")
    parser.add_argument(
        "--text-column",
        default=None,
        help="Text column to search (defaults to the best available inscription text column).",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_FILE),
        help="Output HTML file path.",
    )
    return parser


def main() -> Path:
    args = build_parser().parse_args()
    out = build_interactive_map(term=args.term, text_column=args.text_column, output_file=Path(args.output))
    print(f"Saved interactive map to: {out}")
    return out


if __name__ == "__main__":
    main()
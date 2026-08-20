"""EpigCorpus Streamlit map, styled after the EDCS overview map.

The interactive map mirrors https://edcs.hist.uzh.ch/map: a CARTO light
basemap, one graduated circle per *place* rather than per inscription, a
purple "known places" layer with red "search results" drawn above it, and the
same five count bands, layer toggles, legend and reset control.

The historical layers this project vendors (Roman provinces, roads, cities)
remain available as optional overlays that start switched off, so nothing that
used to be on the map has been lost.
"""

from __future__ import annotations

import json
import math
import textwrap
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from matplotlib.lines import Line2D
from streamlit.components.v1 import html


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

# Base-map layers are vendored in the repository rather than fetched at runtime,
# so the map is reproducible offline and does not depend on any third-party
# repository staying online. Provenance and licence terms: data/map_layers/README.md
MAP_LAYERS_DIR = DATA_DIR / "map_layers"
CLEANED_JSONL_FILE = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"
CLEANED_JSONL_GZ = DATA_DIR / "edcs_inscriptions_cleaned.jsonl.gz"
LOOKUP_FILE = DATA_DIR / "edcs_lookup.json"
REPO_URL = "https://github.com/d-sanoj/EpigCorpus"
REPO_NAME = "d-sanoj/EpigCorpus"
PNG_CITATION = (
    "Sanoj Doddapaneni. (2026). EpigCorpus (Version 1.0) "
    "[Computer software]. GitHub. https://github.com/d-sanoj/EpigCorpus"
)

# Required columns - everything else is dropped to reduce memory load
REQUIRED_COLS = [
    "latitude",
    "longitude",
    "inscription_text",
    "inscription_text_interpretive",
    "inscription_text_conservative",
    "record_id",
    "edcs_id",
    "place",
    "province",
    "not_before",
    "not_after",
    "language",
    "material_en",
    "category_en",
]

# Fields whose cells hold several values at once: category_en is a list, and
# language arrives comma-joined ("Greek, Latin"). Selecting "Latin" has to match
# those rows too, so both are filtered on token overlap rather than equality.
MULTI_VALUE_FILTERS = ("category_en", "language")

# Roman Empire bounds. Wide enough for the whole empire as EDCS draws it:
# Britannia in the north-west through Aegyptus and Mesopotamia in the south-east.
ROMAN_BOUNDS = (20.0, 62.0, -12.0, 50.0)  # (min_lat, max_lat, min_lon, max_lon)

# Map view shared by the initial load and the reset control (EDCS uses Rome).
MAP_CENTER = (41.9028, 12.4964)
MAP_ZOOM = 4

# CARTO basemap. "light_all" carries place labels; "light_nolabels" is the same
# cartography without them, which is how the "Place names" toggle works.
# CARTO dark basemap ("dark_matter"), the backdrop used throughout the folium
# cartography tutorial this styling follows:
# https://nbviewer.org/github/vincentropy/python_cartography_tutorial/blob/master/part1_basic_folium_maps.ipynb
# "dark_all" carries place labels, "dark_nolabels" is the same cartography
# without them, which is how the "Place names" toggle works.
CARTO_LABELLED = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
CARTO_PLAIN = "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
CARTO_ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
    '&copy; <a href="https://carto.com/attributions">CARTO</a>'
)
LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
LEAFLET_JS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"

# The tutorial's two-colour scheme: teal for one state, tangerine for the other,
# with magnitude carried by circle size rather than by hue.
KNOWN_COLOR = "#0A8A9F"   # teal
SEARCH_COLOR = "#E37222"  # tangerine
CITY_COLOR = "#FFCE00"    # yellow
ROAD_COLOR = "#CBD5E1"
EMPTY_COLOR = "#64748B"
# The PNG prints on white paper, so the two colours that only work on the dark
# basemap get print-safe counterparts.
PRINT_SEARCH_EDGE = "#ffd9b8"
PRINT_KNOWN_EDGE = "#b8f1fb"
MAP_BACKGROUND = "#0b0f17"

# The tutorial sizes circles straight off the value (radius = count / 20). That
# is fine for a few hundred bike stations spanning tens of trips; here a single
# place can hold thousands of inscriptions, so the same idea is kept - size is
# continuous in the count, no binning - on a log scale that stays legible.
RADIUS_BASE = 1.2
RADIUS_SCALE = 1.9
EMPTY_RADIUS = 1.5
# Representative counts drawn in the size legend.
LEGEND_STOPS = [1, 10, 100, 1000]

# Inscriptions listed inside a search-result popup before it is truncated.
POPUP_ITEM_LIMIT = 8

MAP_HEIGHT = 620


def inject_professional_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #000000;
            color: #e5e7eb;
        }
        [data-testid="stSidebar"] {
            display: none !important;
        }
        .block-container {
            padding-top: 1.15rem;
            padding-bottom: 1.3rem;
        }
        [data-testid="stAppViewContainer"] {
            background: #000000;
        }
        [data-testid="stHeader"] {
            background: rgba(0, 0, 0, 0);
        }
        h1, h2, h3 {
            color: #e5e7eb;
            letter-spacing: 0.2px;
        }
        .main-title {
            text-align: center;
            margin: 0 0 0.2rem 0;
            color: #e5e7eb;
            font-size: 2rem;
            font-weight: 700;
            text-decoration: none !important;
        }
        .results-title {
            text-align: center;
            margin: 0.7rem 0 0.55rem 0;
            color: #e5e7eb;
            font-size: 1.45rem;
            font-weight: 650;
            text-decoration: none !important;
        }
        [data-testid="stCaptionContainer"] {
            color: #9ca3af;
        }
        [data-testid="stMarkdownContainer"] p {
            color: #d1d5db;
        }
        .matches-summary {
            text-align: center;
            margin: 0.5rem 0 0.35rem 0;
            color: #d1d5db;
            font-size: 0.95rem;
            font-weight: 500;
        }
        [data-testid="stDataFrame"] {
            border: 1px solid #2b3648;
            border-radius: 10px;
            background: rgba(12, 16, 22, 0.88);
        }
        iframe {
            border-radius: 10px;
            border: 1px solid #2b3648;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.28);
        }
        /* Hide sidebar controls because all filters are on main page */
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        button[kind="header"] {
            display: none !important;
        }
        [data-testid="stSidebarCollapseButton"] {
            display: none !important;
        }
        /* Dropdown options carry their short form on a second line. The label is
           one text node, so ::first-line is what separates the English form from
           the dimmed abbreviation beneath it. Streamlit truncates option text by
           default, hence the overflow and text-overflow overrides. */
        li[role="option"],
        li[role="option"] * {
            white-space: pre-line !important;
            text-overflow: clip !important;
            overflow: visible !important;
        }
        li[role="option"] {
            color: #6b7688 !important;
            font-size: 0.78rem !important;
            line-height: 1.2 !important;
        }
        li[role="option"]::first-line,
        li[role="option"] div::first-line {
            color: #e5e7eb;
            font-size: 0.95rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def map_layer_path(*parts: str) -> Path:
    """Resolve a vendored base-map file, failing with an actionable message."""
    path = MAP_LAYERS_DIR.joinpath(*parts)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing base-map layer: {path}\n"
            "These files are vendored in the repository under data/map_layers/. "
            "Restore them with: git checkout -- data/map_layers"
        )
    return path


@st.cache_data(show_spinner=False, ttl=3600)
def load_provinces() -> gpd.GeoDataFrame:
    """Load and cache the Roman provinces shapefile (AWMC, AD 117)."""
    provinces_shp = map_layer_path("roman_empire_ad_117", "roman_empire_ad_117.shp")
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=4326)
    return provinces


@st.cache_data(show_spinner=False, ttl=3600)
def load_roads() -> gpd.GeoDataFrame:
    """Load and cache the Roman roads shapefile (AWMC, Barrington Atlas)."""
    roads_shp = map_layer_path("ba_roads", "ba_roads.shp")
    roads = gpd.read_file(roads_shp).to_crs(epsg=4326)
    return roads


@st.cache_data(show_spinner=False, ttl=3600)
def load_cities() -> gpd.GeoDataFrame:
    """Load and cache the cities dataset (Hanson 2016, OXREP)."""
    cities_csv = map_layer_path("Hanson2016_Cities_OxREP.csv")
    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"], crs="EPSG:4326"),
    )
    return cities


def cleaned_corpus_path() -> Path:
    """Return the cleaned corpus, preferring the plain file over the .gz snapshot.

    pandas infers compression from the extension, so callers need no special
    handling and users never have to decompress anything by hand.
    """
    if CLEANED_JSONL_FILE.exists():
        return CLEANED_JSONL_FILE
    if CLEANED_JSONL_GZ.exists():
        return CLEANED_JSONL_GZ
    raise FileNotFoundError(
        f"Missing: {CLEANED_JSONL_FILE} (or {CLEANED_JSONL_GZ.name}). Run main.py first."
    )


@st.cache_data(show_spinner=False, ttl=3600)
def load_all_inscriptions(columns: tuple[str, ...] = tuple(REQUIRED_COLS)) -> pd.DataFrame:
    """Load JSONL with only needed columns - minimal memory footprint.

    The column set is an argument so that it lands in the cache key: adding a
    column to REQUIRED_COLS otherwise leaves a running app serving a cached
    frame that is missing it.
    """
    # Load JSONL and keep only required columns
    data = pd.read_json(cleaned_corpus_path(), lines=True)

    # Select only columns we need
    cols_to_keep = [c for c in columns if c in data.columns]
    data = data[cols_to_keep]

    # Fast numeric conversion and bounds check
    data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
    data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")

    # Drop invalid coords
    data = data.dropna(subset=["latitude", "longitude"])

    # Quick Roman bounds filter
    min_lat, max_lat, min_lon, max_lon = ROMAN_BOUNDS
    data = data[
        (data["latitude"] >= min_lat) & (data["latitude"] <= max_lat) &
        (data["longitude"] >= min_lon) & (data["longitude"] <= max_lon)
    ]

    return data.reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_full_cleaned_data() -> pd.DataFrame:
    """Load full cleaned dataset with all columns for detailed result table."""
    return pd.read_json(cleaned_corpus_path(), lines=True)


def _token_set(value: object) -> set[str]:
    """Split a cell into the set of values it carries."""
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    if value is None or isinstance(value, float):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


@st.cache_data(show_spinner=False, ttl=3600)
def short_forms() -> dict[str, dict[str, str]]:
    """Map each English filter value to its abbreviated form, where one exists.

    EDCS records materials and categories under a Latin token (`aes`,
    `tituli sepulcrales`) and languages under an ISO-ish code, alongside the
    English label the corpus columns carry. The dropdowns show both.
    """
    forms: dict[str, dict[str, str]] = {"material_en": {}, "category_en": {}, "language": {}}
    if not LOOKUP_FILE.exists():
        return forms

    with LOOKUP_FILE.open(encoding="utf-8") as handle:
        lookup = json.load(handle)

    for source, column, short_key in (
        ("materials", "material_en", "token"),
        ("categories", "category_en", "token"),
        ("languages", "language", "code"),
    ):
        for entry in (lookup.get(source) or {}).values():
            english = str(entry.get("en") or "").strip()
            short = str(entry.get(short_key) or "").strip()
            if english and short and short not in {english, "?"}:
                forms[column][english] = short
    return forms


def option_formatter(column: str, forms: dict[str, dict[str, str]]):
    """Render an option as `English\nshort form`; CSS dims the second line."""
    mapping = forms.get(column, {})

    def format_option(value: str) -> str:
        short = mapping.get(value)
        return f"{value}\n{short}" if short else value

    return format_option


@st.cache_data(show_spinner=False, ttl=3600)
def filter_options() -> dict[str, list[str]]:
    """Every value each filter can take, taken from the corpus itself."""
    data = load_all_inscriptions()
    options: dict[str, list[str]] = {}
    for column in ("material_en", "province", "place"):
        values = data[column].dropna().astype(str).str.strip()
        options[column] = sorted({v for v in values if v and v != "?"})
    for column in MULTI_VALUE_FILTERS:
        collected: set[str] = set()
        for value in data[column]:
            collected |= _token_set(value)
        options[column] = sorted(collected)
    return options


def apply_filters(
    data: pd.DataFrame,
    search_column: str,
    term: str,
    selections: dict[str, list[str]],
) -> pd.DataFrame:
    """Narrow the corpus by the selected filters and, last, the keyword."""
    result = data
    for column in ("material_en", "province", "place"):
        chosen = selections.get(column) or []
        if chosen:
            result = result[result[column].isin(chosen)]
    for column in MULTI_VALUE_FILTERS:
        chosen = set(selections.get(column) or [])
        if chosen:
            result = result[result[column].map(lambda v, want=chosen: bool(want & _token_set(v)))]
    if term.strip():
        haystack = result[search_column].fillna("").astype(str)
        result = result[haystack.str.contains(term, case=False, na=False, regex=False)]
    return result.reset_index(drop=True)


def filter_inscriptions(data: pd.DataFrame, search_column: str, term: str) -> pd.DataFrame:
    """Fast string filtering without GeoDataFrame overhead."""
    if not term.strip():
        return data.iloc[:0]  # Empty dataframe

    search_text = data[search_column].fillna("").astype(str)
    matches = search_text.str.contains(term, case=False, na=False, regex=False)
    return data[matches].reset_index(drop=True)


# --------------------------------------------------------------------------
# Place aggregation
#
# EDCS draws one circle per place scaled by how many monuments sit there, not
# one circle per monument. Everything below turns inscription rows into that
# per-place view.
# --------------------------------------------------------------------------


def aggregate_places(inscriptions: pd.DataFrame) -> pd.DataFrame:
    """Collapse inscription rows into one row per place with a monument count."""
    if inscriptions.empty:
        return pd.DataFrame(columns=["place", "latitude", "longitude", "count"])

    frame = inscriptions[["place", "latitude", "longitude"]].copy()
    frame["place"] = frame["place"].fillna("?").astype(str).str.strip().replace("", "?")
    # Round before grouping so float noise in the source data cannot split one
    # place across several near-identical coordinates.
    frame["latitude"] = frame["latitude"].round(5)
    frame["longitude"] = frame["longitude"].round(5)

    grouped = (
        frame.groupby(["place", "latitude", "longitude"], sort=False)
        .size()
        .reset_index(name="count")
    )
    # Ascending: callers draw in row order, so the largest, darkest circles are
    # painted last and end up on top instead of buried under the small ones.
    return grouped.sort_values("count").reset_index(drop=True)


def _place_result_items(inscriptions: pd.DataFrame, search_column: str) -> dict[tuple[float, float], list[str]]:
    """Collect a few example inscriptions per place for the result popups."""
    items: dict[tuple[float, float], list[str]] = {}
    counts: dict[tuple[float, float], int] = {}
    for row in inscriptions.itertuples(index=False):
        key = (round(float(row.latitude), 5), round(float(row.longitude), 5))
        counts[key] = counts.get(key, 0) + 1
        bucket = items.setdefault(key, [])
        if len(bucket) >= POPUP_ITEM_LIMIT:
            continue
        identifier = getattr(row, "edcs_id", None) or getattr(row, "record_id", "")
        text = getattr(row, search_column, "") or ""
        text = " ".join(str(text).split())
        if len(text) > 160:
            text = text[:157] + "…"
        bucket.append(f"{identifier} · {text}" if text else str(identifier))
    return items


@st.cache_data(show_spinner=False, ttl=3600)
def load_known_places() -> pd.DataFrame:
    """Every place EDCS knows, carrying this corpus' monument count.

    Places that the lookup table knows but that have no inscription in the
    corpus are kept with a count of 0, which is how EDCS renders them: a small
    grey dot rather than a graduated purple one.
    """
    known = aggregate_places(load_all_inscriptions())

    if not LOOKUP_FILE.exists():
        return known

    with LOOKUP_FILE.open(encoding="utf-8") as handle:
        lookup = json.load(handle)

    seen = {
        (round(lat, 4), round(lon, 4))
        for lat, lon in zip(known["latitude"], known["longitude"], strict=True)
    }
    min_lat, max_lat, min_lon, max_lon = ROMAN_BOUNDS
    extra: list[dict[str, object]] = []
    for entry in (lookup.get("places") or {}).values():
        try:
            lat = float(entry.get("lat"))
            lon = float(entry.get("lon"))
        except (TypeError, ValueError):
            continue
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue
        key = (round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        extra.append(
            {
                "place": str(entry.get("ort") or "?").strip() or "?",
                "latitude": round(lat, 5),
                "longitude": round(lon, 5),
                "count": 0,
            }
        )

    if not extra:
        return known
    combined = pd.concat([known, pd.DataFrame(extra)], ignore_index=True)
    # Re-sort after the zero-count places are appended, or they would paint over
    # every graduated circle on the map.
    return combined.sort_values("count").reset_index(drop=True)


def radius_for_count(count: int) -> float:
    """Circle radius at the default zoom, continuous in the inscription count."""
    if count <= 0:
        return EMPTY_RADIUS
    return RADIUS_BASE + RADIUS_SCALE * math.log10(count + 1)


# --------------------------------------------------------------------------
# Optional historical overlays (off by default)
# --------------------------------------------------------------------------


def _province_palette() -> list[str]:
    """Saturated province fills that still read under a full field of markers."""
    return [
        "#f9c784",
        "#8ecae6",
        "#a7d489",
        "#d9a7e0",
        "#f6d55c",
        "#7fd1bd",
        "#f5a3b7",
        "#a6b1f0",
    ]


def _print_province_palette() -> list[str]:
    """Province fills for the dark export.

    Deliberately cool and muted: the markers are warm and the city rings violet,
    so the provinces stay in a blue-green band, light enough to separate
    neighbours but never bright enough to compete with the data.
    """
    return [
        "#4a6fa5",
        "#3f8f7f",
        "#5b6bab",
        "#4f8f5a",
        "#3b7f9c",
        "#6b6fa8",
        "#4d8f8a",
        "#5a7fb0",
    ]


# Export-only colours, spaced in hue so no two layers read as the same thing:
# violet city rings, muted slate roads, warm markers.
PRINT_CITY_COLOR = "#c084fc"
PRINT_ROAD_COLOR = "#64748b"


@st.cache_data(show_spinner=False, ttl=3600)
def provinces_geojson() -> dict:
    """Provinces as GeoJSON, lightly simplified and pre-coloured for the browser."""
    provinces = load_provinces().reset_index(drop=True).copy()
    palette = _province_palette()
    provinces["_color"] = [palette[i % len(palette)] for i in range(len(provinces))]
    provinces["geometry"] = provinces.geometry.simplify(0.01, preserve_topology=True)
    return json.loads(provinces[["_color", "geometry"]].to_json())


@st.cache_data(show_spinner=False, ttl=3600)
def roads_geojson() -> dict:
    """Roads as GeoJSON. Simplified hard: the raw shapefile is far too heavy to inline."""
    roads = load_roads().copy()
    roads["geometry"] = roads.geometry.simplify(0.02, preserve_topology=True)
    return json.loads(roads[["geometry"]].to_json())


@st.cache_data(show_spinner=False, ttl=3600)
def cities_points() -> list[list]:
    """Cities as [lat, lon, label] triples, the label used for the hover tooltip."""
    cities = load_cities()

    def label(row: pd.Series) -> str:
        ancient = str(row.get("Ancient Toponym") or "").strip()
        modern = str(row.get("Modern Toponym") or "").strip()
        province = str(row.get("Province") or "").strip()
        name = ancient or modern or "Unnamed city"
        if ancient and modern and modern != ancient:
            name = f"{ancient} / {modern}"
        return f"{name}, {province}" if province else name

    points = []
    for _, row in cities.iterrows():
        point = row.geometry
        if point is None or point.is_empty:
            continue
        points.append([round(float(point.y), 5), round(float(point.x), 5), label(row)])
    return points


# --------------------------------------------------------------------------
# Leaflet map
# --------------------------------------------------------------------------


MAP_TEMPLATE = """
<link rel="stylesheet" href="__LEAFLET_CSS__"/>
<script src="__LEAFLET_JS__"></script>
<style>
  html, body { margin: 0; padding: 0; background: __MAP_BACKGROUND__; }
  #overviewMap {
    height: __MAP_HEIGHT__px;
    width: 100%;
    background: __MAP_BACKGROUND__;
    border-radius: 10px;
    font-family: "Helvetica Neue", Arial, Helvetica, sans-serif;
  }
  /* Dark chrome throughout: white Leaflet panels on a dark_matter basemap glare. */
  .leaflet-bar a, .leaflet-bar a:hover {
    background: #111827;
    color: #e5e7eb;
    border-bottom-color: rgba(148, 163, 184, 0.25);
  }
  .leaflet-bar { border: 1px solid rgba(148, 163, 184, 0.25); }
  .leaflet-control-attribution {
    background: rgba(17, 24, 39, 0.82) !important;
    color: #94a3b8;
  }
  .leaflet-control-attribution a { color: #cbd5e1; }
  .leaflet-tooltip {
    background: rgba(17, 24, 39, 0.94);
    border: 1px solid rgba(148, 163, 184, 0.3);
    color: #e5e7eb;
    box-shadow: none;
  }
  .leaflet-tooltip-top:before { border-top-color: rgba(148, 163, 184, 0.5); }
  .leaflet-popup-content-wrapper, .leaflet-popup-tip {
    background: #111827;
    color: #e5e7eb;
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.55);
  }
  .leaflet-popup-close-button { color: #94a3b8 !important; }
  .overview-map-layers, .overview-map-legend {
    background: rgba(17, 24, 39, 0.9);
    border: 1px solid rgba(148, 163, 184, 0.28);
    border-radius: 8px;
    box-shadow: 0 1px 6px rgba(0, 0, 0, 0.5);
    color: #e5e7eb;
    font-size: 0.82rem;
  }
  .overview-map-layers { padding: 0.55rem 0.65rem; display: grid; gap: 0.35rem; line-height: 1.23rem; }
  .overview-map-layers label {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    cursor: pointer;
    white-space: nowrap;
  }
  .overview-map-layers input { accent-color: __KNOWN_COLOR__; }
  .overview-map-layers input[disabled] + .overview-map-layer-swatch { opacity: 0.4; }
  .overview-map-layers label:has(input[disabled]) { color: #6b7280; cursor: default; }
  .overview-map-layers hr {
    border: 0;
    border-top: 1px solid rgba(148, 163, 184, 0.22);
    margin: 0.1rem 0;
  }
  .overview-map-layer-swatch {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    display: block;
    flex: 0 0 12px;
  }
  .overview-map-layer-swatch.all { background: __KNOWN_COLOR__; }
  .overview-map-layer-swatch.search { background: __SEARCH_COLOR__; }
  .overview-map-layer-swatch.provinces { background: #f9c784; border-radius: 2px; }
  .overview-map-layer-swatch.roads { background: __ROAD_COLOR__; border-radius: 2px; height: 3px; flex-basis: 12px; }
  .overview-map-layer-swatch.cities { background: transparent; border: 2px solid __CITY_COLOR__; }
  .overview-map-layer-letter {
    width: 12px;
    flex: 0 0 12px;
    display: flex;
    align-items: center;
    font-size: 0.68rem;
    line-height: 0.68rem;
    color: #94a3b8;
  }
  .overview-map-reset button {
    width: 30px;
    height: 30px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #111827;
    border: 0;
    cursor: pointer;
    color: #e5e7eb;
  }
  .overview-map-legend { padding: 0.65rem 0.75rem; line-height: 1.35; }
  .overview-map-legend-title { margin-bottom: 0.4rem; font-weight: 600; }
  .overview-map-legend-series {
    display: grid;
    grid-template-columns: 12px auto;
    gap: 0.2rem 0.4rem;
    align-items: center;
    font-size: 0.75rem;
    color: #cbd5e1;
    margin-bottom: 0.45rem;
  }
  .overview-map-legend-row { display: flex; align-items: center; gap: 0.5rem; height: 26px; }
  .overview-map-legend-dot {
    border-radius: 50%;
    background: rgba(10, 138, 159, 0.45);
    border: 1px solid __KNOWN_COLOR__;
    display: block;
    flex: none;
    margin: 0 auto;
  }
  .overview-map-legend-slot { width: 26px; display: flex; justify-content: center; }
  .leaflet-container { font-family: inherit; background: __MAP_BACKGROUND__; }
  .overview-popup h4 { margin: 0 0 0.3rem 0; font-size: 0.85rem; color: __SEARCH_COLOR__; }
  .overview-popup ul { margin: 0; padding-left: 1rem; font-size: 0.75rem; line-height: 1.45; }
  .overview-popup .more { margin-top: 0.3rem; color: #94a3b8; font-size: 0.72rem; }
</style>
<div id="overviewMap"></div>
<script>
(function () {
  var KNOWN = __KNOWN__;
  var RESULTS = __RESULTS__;
  var HAS_SEARCH = __HAS_SEARCH__;
  var HAS_KNOWN = KNOWN.length > 0;
  var FIT_BOUNDS = __FIT_BOUNDS__;
  var PROVINCES = __PROVINCES__;
  var ROADS = __ROADS__;
  var CITIES = __CITIES__;
  var LEGEND_STOPS = __LEGEND_STOPS__;
  var TILE_LABELLED = "__TILE_LABELLED__";
  var TILE_PLAIN = "__TILE_PLAIN__";
  var ATTRIBUTION = "__ATTRIBUTION__";
  var CENTER = __CENTER__;
  var ZOOM = __ZOOM__;

  var escapeHtml = function (value) {
    return String(value).replace(/[&<>"']/g, function (ch) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch];
    });
  };
  var formatInteger = function (value) { return Number(value).toLocaleString("en-US"); };
  // Size is continuous in the count, as in the tutorial - just on a log scale,
  // because these counts span 1 to several thousand rather than tens.
  var radiusFor = function (count) {
    if (count <= 0) return __EMPTY_RADIUS__;
    return __RADIUS_BASE__ + __RADIUS_SCALE__ * Math.log(count + 1) / Math.LN10;
  };

  var map = L.map("overviewMap", { preferCanvas: true, worldCopyJump: true }).setView(CENTER, ZOOM);
  // Leaflet binds mouse events to each canvas element, so only the TOPMOST
  // interactive canvas hit-tests: a second canvas over the markers swallows
  // every hover below it. Everything hoverable therefore shares one canvas,
  // where stacking and hit priority both follow draw order, and the two
  // non-interactive layers get panes that let events fall straight through.
  var provincesPane = map.createPane("provincesPane");
  provincesPane.style.zIndex = "400";
  provincesPane.style.pointerEvents = "none";
  map.createPane("markersPane").style.zIndex = "410";
  var roadsPane = map.createPane("roadsPane");
  roadsPane.style.zIndex = "415";
  roadsPane.style.pointerEvents = "none";

  var baseLayer = L.tileLayer(TILE_LABELLED, {
    maxZoom: 19,
    subdomains: "abcd",
    attribution: ATTRIBUTION,
  }).addTo(map);
  var setPlaceLabels = function (visible) {
    baseLayer.setUrl(visible ? TILE_LABELLED : TILE_PLAIN);
  };

  // Radii are calibrated for the default zoom and scale with the zoom level, so
  // the empire is neither a solid mass zoomed out nor pinpricks zoomed in.
  var scaled = [];
  var zoomScale = function () {
    return Math.min(2.2, Math.max(0.62, Math.pow(2, (map.getZoom() - ZOOM) * 0.3)));
  };
  var applyZoomScale = function () {
    var factor = zoomScale();
    for (var i = 0; i < scaled.length; i += 1) {
      scaled[i].marker.setRadius(scaled[i].base * factor);
    }
  };
  map.on("zoomend", applyZoomScale);

  var markerRenderer = L.canvas({ pane: "markersPane", padding: 0.5, tolerance: 5 });
  var allLayer = L.layerGroup();
  var searchLayer = L.layerGroup();

  KNOWN.forEach(function (row) {
    var lat = row[0], lon = row[1], count = row[2], name = row[3] || "?";
    var hasInscriptions = count > 0;
    var base = radiusFor(count);
    var marker = L.circleMarker([lat, lon], {
      renderer: markerRenderer,
      radius: base,
      color: hasInscriptions ? "__KNOWN_COLOR__" : "__EMPTY_COLOR__",
      weight: 0.6,
      opacity: hasInscriptions ? 0.55 : 0.35,
      fillColor: hasInscriptions ? "__KNOWN_COLOR__" : "__EMPTY_COLOR__",
      fillOpacity: hasInscriptions ? 0.2 : 0.12,
      fill: true,
    });
    marker.bindTooltip(
      hasInscriptions
        ? escapeHtml(name) + " · " + formatInteger(count) + (count === 1 ? " inscription" : " inscriptions")
        : escapeHtml(name) + " · no inscriptions in this corpus",
      { direction: "top", sticky: true }
    );
    marker.addTo(allLayer);
    scaled.push({ marker: marker, base: base });
  });

  RESULTS.forEach(function (row) {
    var lat = row[0], lon = row[1], count = row[2], name = row[3] || "?", items = row[4] || [];
    var base = radiusFor(count);
    var marker = L.circleMarker([lat, lon], {
      renderer: markerRenderer,
      radius: base,
      color: "__SEARCH_COLOR__",
      weight: 1,
      opacity: 1,
      fillColor: "__SEARCH_COLOR__",
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
    marker.addTo(searchLayer);
    scaled.push({ marker: marker, base: base });
  });

  var provincesLayer = PROVINCES
    ? L.geoJSON(PROVINCES, {
        pane: "provincesPane",
        style: function (feature) {
          return {
            fillColor: (feature.properties && feature.properties._color) || "#f0f0f0",
            color: "rgba(226, 232, 240, 0.35)",
            weight: 0.6,
            fillOpacity: 0.3,
          };
        },
      })
    : L.layerGroup();
  var roadsLayer = ROADS
    ? L.geoJSON(ROADS, {
        pane: "roadsPane",
        style: function () { return { color: "__ROAD_COLOR__", weight: 1, opacity: 0.85 }; },
      })
    : L.layerGroup();
  var citiesLayer = L.layerGroup(
    CITIES.map(function (row) {
      var ring = L.circleMarker([row[0], row[1]], {
        renderer: markerRenderer,
        radius: 2.2,
        color: "__CITY_COLOR__",
        weight: 1.1,
        opacity: 0.95,
        fill: false,
      });
      ring.bindTooltip(escapeHtml(row[2] || "Unnamed city"), { direction: "top", sticky: true });
      scaled.push({ marker: ring, base: 2.2 });
      return ring;
    })
  );

  var layersByKey = {
    all: allLayer,
    search: searchLayer,
    provinces: provincesLayer,
    roads: roadsLayer,
    cities: citiesLayer,
  };
  var visibility = { all: HAS_KNOWN, search: HAS_SEARCH, provinces: false, roads: false, cities: false };
  var setLayerVisible = function (key, visible) {
    var layer = layersByKey[key];
    if (!layer) return;
    var active = Boolean(
      visible && (key !== "search" || HAS_SEARCH) && (key !== "all" || HAS_KNOWN)
    );
    if (active) {
      if (!map.hasLayer(layer)) layer.addTo(map);
    } else if (map.hasLayer(layer)) {
      map.removeLayer(layer);
    }
  };
  // Order matters inside the shared canvas: later layers draw on top and win
  // the hover, so places sit under cities, which sit under search results.
  var MARKER_ORDER = ["all", "cities", "search"];
  var restackAbove = function (key) {
    var from = MARKER_ORDER.indexOf(key);
    if (from < 0) return;
    // Re-adding puts a layer back at the end of the draw list. Only the layers
    // that belong above the one just toggled need it, which keeps the 22,000
    // known places out of the work whenever anything else is switched.
    MARKER_ORDER.slice(from + 1).forEach(function (above) {
      var layer = layersByKey[above];
      if (map.hasLayer(layer)) {
        map.removeLayer(layer);
        layer.addTo(map);
      }
    });
  };
  ["provinces", "roads"].concat(MARKER_ORDER).forEach(function (key) {
    setLayerVisible(key, visibility[key]);
  });

  var layerControl = L.control({ position: "topright" });
  layerControl.onAdd = function () {
    var node = L.DomUtil.create("div", "overview-map-layers");
    node.innerHTML =
      '<label><input type="checkbox" data-map-layer="all"' + (HAS_KNOWN ? " checked" : " disabled") +
      '> <span class="overview-map-layer-swatch all"></span>Known places</label>' +
      '<label><input type="checkbox" data-map-layer="search"' + (HAS_SEARCH ? " checked" : " disabled") +
      '> <span class="overview-map-layer-swatch search"></span>Search results</label>' +
      '<label><input type="checkbox" data-map-label-toggle checked> <span class="overview-map-layer-letter">Aa</span>Place names</label>' +
      "<hr>" +
      '<label><input type="checkbox" data-map-layer="provinces"> <span class="overview-map-layer-swatch provinces"></span>Roman provinces</label>' +
      '<label><input type="checkbox" data-map-layer="roads"> <span class="overview-map-layer-swatch roads"></span>Roads</label>' +
      '<label><input type="checkbox" data-map-layer="cities"> <span class="overview-map-layer-swatch cities"></span>Cities</label>';
    L.DomEvent.disableClickPropagation(node);
    L.DomEvent.disableScrollPropagation(node);
    node.querySelectorAll("input[data-map-layer]").forEach(function (input) {
      input.addEventListener("change", function () {
        var key = input.dataset.mapLayer;
        visibility[key] = input.checked;
        setLayerVisible(key, input.checked);
        if (input.checked) restackAbove(key);
      });
    });
    var labelToggle = node.querySelector("input[data-map-label-toggle]");
    if (labelToggle) {
      labelToggle.addEventListener("change", function () { setPlaceLabels(labelToggle.checked); });
    }
    return node;
  };
  layerControl.addTo(map);

  var resetControl = L.control({ position: "topleft" });
  resetControl.onAdd = function () {
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
  resetControl.addTo(map);

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

  if (FIT_BOUNDS) {
    map.fitBounds(FIT_BOUNDS, { padding: [24, 24] });
  }
  applyZoomScale();
})();
</script>
"""


def _json_for_script(value: object) -> str:
    """Serialise for embedding in a <script> block."""
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def build_map_html(
    known_places: pd.DataFrame,
    result_places: pd.DataFrame,
    has_search: bool,
    fit_bounds: list[list[float]] | None = None,
) -> str:
    """Render the EDCS-style Leaflet map as a standalone HTML fragment."""
    known_rows = [
        [float(row.latitude), float(row.longitude), int(row.count), str(row.place)]
        for row in known_places.itertuples(index=False)
    ]
    result_rows = [
        [
            float(row.latitude),
            float(row.longitude),
            int(row.count),
            str(row.place),
            list(getattr(row, "items", []) or []),
        ]
        for row in result_places.itertuples(index=False)
    ]

    replacements = {
        "__LEAFLET_CSS__": LEAFLET_CSS,
        "__LEAFLET_JS__": LEAFLET_JS,
        "__MAP_HEIGHT__": str(MAP_HEIGHT),
        "__KNOWN__": _json_for_script(known_rows),
        "__RESULTS__": _json_for_script(result_rows),
        "__HAS_SEARCH__": "true" if has_search else "false",
        "__FIT_BOUNDS__": _json_for_script(fit_bounds) if fit_bounds else "null",
        "__LEGEND_STOPS__": _json_for_script(LEGEND_STOPS),
        "__PROVINCES__": _json_for_script(provinces_geojson()),
        "__ROADS__": _json_for_script(roads_geojson()),
        "__CITIES__": _json_for_script(cities_points()),
        "__TILE_LABELLED__": CARTO_LABELLED,
        "__TILE_PLAIN__": CARTO_PLAIN,
        "__ATTRIBUTION__": CARTO_ATTRIBUTION.replace('"', '\\"'),
        "__CENTER__": _json_for_script(list(MAP_CENTER)),
        "__ZOOM__": str(MAP_ZOOM),
        "__KNOWN_COLOR__": KNOWN_COLOR,
        "__SEARCH_COLOR__": SEARCH_COLOR,
        "__CITY_COLOR__": CITY_COLOR,
        "__ROAD_COLOR__": ROAD_COLOR,
        "__EMPTY_COLOR__": EMPTY_COLOR,
        "__MAP_BACKGROUND__": MAP_BACKGROUND,
        "__RADIUS_BASE__": str(RADIUS_BASE),
        "__RADIUS_SCALE__": str(RADIUS_SCALE),
        "__EMPTY_RADIUS__": str(EMPTY_RADIUS),
    }
    document = MAP_TEMPLATE
    for token, value in replacements.items():
        document = document.replace(token, value)
    return document


def _normalize_search_mode(label: str | None) -> str:
    legacy_map = {
        "Raw inscription text": "Raw inscriptions",
        "Interpretive cleaned text": "Interpretive Cleaned Inscriptions",
        "Conservative cleaned text": "Conservative Cleaned Inscriptions",
    }
    normalized = legacy_map.get(label or "", label or "")
    valid_modes = {
        "Raw inscriptions",
        "Interpretive Cleaned Inscriptions",
        "Conservative Cleaned Inscriptions",
    }
    return normalized if normalized in valid_modes else "Raw inscriptions"


@st.cache_data(show_spinner=False, ttl=3600)
def build_png_bytes(
    result_places: pd.DataFrame,
    heading: str,
    subtitle: str,
    footer_lines: tuple[str, ...],
    marker_color: str = SEARCH_COLOR,
    marker_edge: str = PRINT_SEARCH_EDGE,
    marker_label: str = "matching inscriptions",
) -> bytes:
    """Publication-style PNG using the same symbology as the web map.

    `heading`, `subtitle` and `footer_lines` are built by the caller because a
    query can now be a keyword, a set of filters, or both. The subtitle names
    every active filter directly under the title.
    """
    subtitle_lines = textwrap.wrap(subtitle, width=110) if subtitle.strip() else []
    provinces = load_provinces().reset_index(drop=True).copy()
    roads = load_roads()
    cities = load_cities()

    provinces_proj = provinces.to_crs(epsg=3857)
    roads_proj = roads.to_crs(epsg=3857)
    cities_proj = cities.to_crs(epsg=3857)

    province_colors = _print_province_palette()
    provinces_proj["_color"] = [
        province_colors[i % len(province_colors)] for i in range(len(provinces_proj))
    ]

    # The export carries the same palette as the interactive map rather than
    # sitting on white paper.
    fig, ax = plt.subplots(figsize=(11.8, 8.6), dpi=180)
    fig.patch.set_facecolor(MAP_BACKGROUND)
    ax.set_facecolor(MAP_BACKGROUND)
    # The footer grows a line per active filter, so the map area has to yield
    # room for it rather than being overdrawn.
    footer_bottom = 0.05
    map_bottom = footer_bottom + 0.021 * len(footer_lines) + 0.02
    map_top = 0.88 - 0.022 * len(subtitle_lines)
    fig.subplots_adjust(left=0.06, right=0.94, top=map_top, bottom=map_bottom)

    provinces_proj.plot(
        ax=ax,
        color=provinces_proj["_color"],
        edgecolor="#cbd5e1",
        linewidth=0.4,
        alpha=0.3,
        zorder=1,
    )
    roads_proj.plot(ax=ax, color=PRINT_ROAD_COLOR, linewidth=0.35, alpha=0.75, zorder=2)
    cities_proj.plot(
        ax=ax,
        color="none",
        markersize=4,
        edgecolor=PRINT_CITY_COLOR,
        linewidth=0.5,
        alpha=0.85,
        zorder=3,
    )

    if not result_places.empty:
        places_geo = gpd.GeoDataFrame(
            result_places.copy(),
            geometry=gpd.points_from_xy(result_places["longitude"], result_places["latitude"]),
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        # Sorted ascending by aggregate_places, so the largest circles are drawn
        # last and stay on top. Size is continuous in the count, as on the web map.
        sizes = [(radius_for_count(int(c)) * 1.9) ** 2 / 4 for c in places_geo["count"]]
        places_geo.plot(
            ax=ax,
            color=marker_color,
            markersize=sizes,
            alpha=0.92,
            edgecolor=marker_edge,
            linewidth=0.35,
            zorder=4,
        )

    minx, miny, maxx, maxy = provinces_proj.total_bounds
    x_pad = (maxx - minx) * 0.025
    y_pad = (maxy - miny) * 0.04
    ax.set_xlim(minx - x_pad, maxx + x_pad)
    ax.set_ylim(miny - y_pad, maxy + y_pad)
    ax.set_axis_off()
    ax.set_aspect("equal", adjustable="box")
    ax.set_anchor("C")

    fig.suptitle(heading, fontsize=17, fontfamily="serif", y=0.955, color="#e5e7eb")
    if subtitle_lines:
        fig.text(
            0.5,
            0.925,
            "\n".join(subtitle_lines),
            ha="center",
            va="top",
            fontsize=10,
            color="#9fb3c8",
        )

    legend_handles = [
        Line2D([0], [0], color=PRINT_ROAD_COLOR, lw=1.4, label="Roads"),
        Line2D(
            [0], [0], marker="o", color="none", markerfacecolor="none",
            markeredgecolor=PRINT_CITY_COLOR, markersize=5, label="Cities",
        ),
    ]
    legend_handles += [
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=marker_color,
            markeredgecolor=marker_edge,
            markersize=max(4.0, radius_for_count(stop) * 1.1),
            label=f"{stop:,} {marker_label}",
        )
        for stop in LEGEND_STOPS
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(0.965, 0.965),
        frameon=True,
        fontsize=9,
        borderaxespad=0.0,
        labelcolor="#e5e7eb",
    )
    legend.get_frame().set_facecolor("#111827")
    legend.get_frame().set_edgecolor("#94a3b8")
    legend.get_frame().set_alpha(0.92)

    fig.text(
        0.5,
        footer_bottom,
        "\n".join(footer_lines),
        ha="center",
        va="bottom",
        fontsize=8,
        color="#cbd5e1",
    )
    fig.text(
        0.5,
        0.018,
        PNG_CITATION,
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#94a3b8",
    )

    buffer = BytesIO()
    fig.savefig(buffer, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


FILTER_LABELS = {
    "material_en": "Material",
    "category_en": "Category",
    "language": "Language",
    "province": "Province",
    "place": "Place",
}

SEARCH_COLUMNS = {
    "Raw inscriptions": "inscription_text",
    "Interpretive Cleaned Inscriptions": "inscription_text_interpretive",
    "Conservative Cleaned Inscriptions": "inscription_text_conservative",
}


def _selection_summary(selections: dict[str, list[str]]) -> list[str]:
    """One human-readable line per filter that is actually set."""
    lines = []
    for column, label in FILTER_LABELS.items():
        chosen = selections.get(column) or []
        if chosen:
            lines.append(f"{label}: {', '.join(chosen)}")
    return lines


@st.cache_data(show_spinner=False, ttl=3600)
def run_query(
    term: str,
    search_mode: str,
    selections: tuple[tuple[str, tuple[str, ...]], ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filtered rows and their per-place aggregate, cached on the query itself.

    The search controls are no longer wrapped in `st.form` - a download button
    cannot live inside one - so every widget change reruns the script. Caching
    here keeps those reruns from re-filtering half a million rows.
    """
    filters = {column: list(values) for column, values in selections}
    search_column = SEARCH_COLUMNS[search_mode]
    filtered = apply_filters(load_all_inscriptions(), search_column, term, filters)
    if filtered.empty:
        return filtered, pd.DataFrame(columns=["place", "latitude", "longitude", "count", "items"])

    places = aggregate_places(filtered)
    examples = _place_result_items(filtered, search_column)
    places["items"] = [
        examples.get((round(lat, 5), round(lon, 5)), [])
        for lat, lon in zip(places["latitude"], places["longitude"], strict=True)
    ]
    return filtered, places


def _submit_search() -> None:
    """Copy the live widget values into the submitted state."""
    st.session_state["submitted_term"] = st.session_state.get("keyword_input", "").strip()
    st.session_state["submitted_mode"] = _normalize_search_mode(
        st.session_state.get("search_mode_input")
    )
    st.session_state["submitted_filters"] = {
        column: list(st.session_state.get(f"filter_{column}") or []) for column in FILTER_LABELS
    }


def main() -> None:
    st.set_page_config(
        page_title="EpigCorpus - EDCS Interactive Map",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    inject_professional_styles()

    st.markdown("<div class='main-title'>EpigCorpus</div>", unsafe_allow_html=True)
    st.markdown(
        "<p style='text-align:center; color:#9ca3af; margin-top:0; margin-bottom:1rem;'>"
        "EDCS inscription exploration across the Roman Empire"
        "</p>",
        unsafe_allow_html=True,
    )

    # Options come from the corpus, so loading has to happen before the controls.
    with st.spinner("Loading inscriptions..."):
        all_inscriptions = load_all_inscriptions()
        options = filter_options()
        forms = short_forms()

    defaults = {
        "submitted_term": "",
        "submitted_mode": "Raw inscriptions",
        "submitted_filters": {column: [] for column in FILTER_LABELS},
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)

    def multiselect(column: str, placeholder: str = "Any") -> None:
        # Explicit key, no `default`: a default that changes between runs churns
        # the widget identity and silently restores a filter just cleared.
        st.multiselect(
            FILTER_LABELS[column],
            options=options[column],
            key=f"filter_{column}",
            format_func=option_formatter(column, forms),
            placeholder=placeholder,
        )

    row_1 = st.columns(3)
    with row_1[0]:
        st.selectbox("Search in:", options=list(SEARCH_COLUMNS), key="search_mode_input")
    with row_1[1]:
        st.text_input(
            "Keyword",
            key="keyword_input",
            placeholder="Enter search term (optional)",
            on_change=_submit_search,
        )
    with row_1[2]:
        multiselect("language")

    row_2 = st.columns(3)
    with row_2[0]:
        multiselect("province")
    with row_2[1]:
        multiselect("place", f"Any ({len(options['place']):,} - type to search)")
    with row_2[2]:
        multiselect("category_en")

    row_3 = st.columns(3)
    with row_3[0]:
        multiselect("material_en")

    active_term = st.session_state["submitted_term"]
    active_mode = _normalize_search_mode(st.session_state["submitted_mode"])
    active_filters = st.session_state["submitted_filters"]

    # Any keyword or any selected filter counts as a query. With one running the
    # corpus-wide teal layer comes off, so only the results are on the map.
    query_active = bool(active_term) or any(active_filters.get(c) for c in FILTER_LABELS)
    summary_lines = _selection_summary(active_filters)

    filtered = all_inscriptions.iloc[:0]
    result_places = pd.DataFrame(columns=["place", "latitude", "longitude", "count", "items"])
    fit_bounds: list[list[float]] | None = None
    known_places = pd.DataFrame(columns=["place", "latitude", "longitude", "count"])

    if query_active:
        with st.spinner("Searching inscriptions..."):
            filtered, result_places = run_query(
                active_term,
                active_mode,
                tuple((column, tuple(active_filters.get(column) or [])) for column in FILTER_LABELS),
            )
        if not filtered.empty:
            fit_bounds = [
                [float(filtered["latitude"].min()), float(filtered["longitude"].min())],
                [float(filtered["latitude"].max()), float(filtered["longitude"].max())],
            ]
    else:
        known_places = load_known_places()

    # The PNG always reflects whatever the map is showing, so the button is live
    # whether or not a query is running.
    if query_active:
        png_places = result_places
        heading = (
            f'Inscriptions matching "{active_term}" in {active_mode}'
            if active_term
            else "Inscriptions matching the selected filters"
        )
        subtitle = " \u00b7 ".join(summary_lines)
        footer_lines = (
            f"Search term: {active_term}" if active_term else "Search term: (none)",
            f"Results: {len(filtered):,} across {len(png_places):,} places"
            f" | Search mode: {active_mode}",
            "Data source: Epigraphik-Datenbank Clauss / Slaby",
        )
        png_colors = (SEARCH_COLOR, PRINT_SEARCH_EDGE, "matching inscriptions")
        file_stem = active_term.replace(" ", "_").lower() or "filtered"
    else:
        png_places = known_places
        heading = "All known places in the EDCS corpus"
        subtitle = "No keyword or filters applied"
        footer_lines = (
            f"All known places: {len(known_places):,}",
            f"Inscriptions plotted: {int(known_places['count'].sum()):,}",
            "Data source: Epigraphik-Datenbank Clauss / Slaby",
        )
        png_colors = (KNOWN_COLOR, PRINT_KNOWN_EDGE, "inscriptions")
        file_stem = "all_known_places"

    with st.spinner("Rendering PNG export..."):
        png_bytes = build_png_bytes(
            png_places[["place", "latitude", "longitude", "count"]],
            heading,
            subtitle,
            footer_lines,
            *png_colors,
        )

    with row_3[1]:
        st.markdown("<div style='height: 1.72rem;'></div>", unsafe_allow_html=True)
        st.button("Search", use_container_width=True, on_click=_submit_search)
    with row_3[2]:
        st.markdown("<div style='height: 1.72rem;'></div>", unsafe_allow_html=True)
        st.download_button(
            label="Download as PNG",
            data=png_bytes,
            file_name=f"edcs_map_{file_stem}.png",
            mime="image/png",
            use_container_width=True,
        )

    if query_active:
        criteria = []
        if active_term:
            criteria.append(f'"{active_term}" in {active_mode}')
        criteria.extend(summary_lines)
        st.markdown(
            (
                "<div class='matches-summary'>"
                f"{len(filtered):,} inscriptions match &mdash; {' &middot; '.join(criteria)}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            (
                "<div class='matches-summary'>"
                f"All known places: {len(known_places):,}. "
                "Search by keyword, by filter, or by both."
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    has_search = query_active and not filtered.empty
    with st.spinner("Building map..."):
        map_html = build_map_html(known_places, result_places, has_search, fit_bounds)
    html(map_html, height=MAP_HEIGHT + 10, scrolling=False)

    if not query_active:
        st.info("Enter a keyword, choose filters, or both, then press Search.")
        return

    if filtered.empty:
        st.warning("Nothing matches that combination. Try removing a filter.")
        return

    # Results table with all columns from cleaned dataset for matched IDs
    st.markdown("<div class='results-title'>Results</div>", unsafe_allow_html=True)
    with st.spinner("Preparing detailed results table..."):
        full_data = load_full_cleaned_data()

        id_col = "edcs_id" if "edcs_id" in filtered.columns and "edcs_id" in full_data.columns else "record_id"
        matched_ids = filtered[id_col].dropna().astype(str).unique().tolist()

        full_data_keyed = full_data.copy()
        full_data_keyed[id_col] = full_data_keyed[id_col].astype(str)
        matched_table = full_data_keyed[full_data_keyed[id_col].isin(matched_ids)]

    tsv_bytes = matched_table.to_csv(sep="\t", index=False).encode("utf-8")
    dl_col_1, dl_col_2, dl_col_3 = st.columns([3, 1, 3])
    with dl_col_2:
        st.download_button(
            label="Download TSV",
            data=tsv_bytes,
            file_name="edcs_search_results.tsv",
            mime="text/tab-separated-values",
            use_container_width=True,
        )

    st.dataframe(matched_table, use_container_width=True, height=360)


if __name__ == "__main__":
    main()

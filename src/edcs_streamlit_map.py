"""Optimized EDCS Streamlit map with ultra-fast loading."""

from __future__ import annotations

from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import requests
import streamlit as st
from streamlit.components.v1 import html


BASE_RAW = "https://raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SUPPORT_DIR = DATA_DIR / "lat_epig_support"
CLEANED_JSONL_FILE = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"

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
]

# Roman Empire approximate bounds (tight filter)
ROMAN_BOUNDS = (25, 50, -10, 45)  # (min_lat, max_lat, min_lon, max_lon)


def download_if_missing(url: str, destination: Path) -> Path:
    if destination.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def download_shapefile_components(base_url: str, stem: str, local_dir: Path) -> Path:
    required = [".shp", ".shx", ".dbf", ".prj"]
    optional = [".sbn", ".sbx", ".cpg"]

    for ext in required:
        download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")
    for ext in optional:
        try:
            download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")
        except requests.HTTPError:
            pass
    return local_dir / f"{stem}.shp"


@st.cache_data(show_spinner=False, ttl=3600)
def load_provinces() -> gpd.GeoDataFrame:
    """Load and cache Roman provinces shapefile."""
    provinces_shp = download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/cultural_data/political_shading/roman_empire_ad_117/shape",
        "roman_empire_ad_117",
        SUPPORT_DIR / "roman_empire_ad_117",
    )
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=4326)
    return provinces


@st.cache_data(show_spinner=False, ttl=3600)
def load_roads() -> gpd.GeoDataFrame:
    """Load and cache Roman roads shapefile."""
    roads_shp = download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/ba_roads",
        "ba_roads",
        SUPPORT_DIR / "ba_roads",
    )
    roads = gpd.read_file(roads_shp).to_crs(epsg=4326)
    return roads


@st.cache_data(show_spinner=False, ttl=3600)
def load_cities() -> gpd.GeoDataFrame:
    """Load and cache cities dataset."""
    cities_csv = download_if_missing(
        f"{BASE_RAW}/cities/Hanson2016_Cities_OxREP.csv",
        SUPPORT_DIR / "Hanson2016_Cities_OxREP.csv",
    )
    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"], crs="EPSG:4326"),
    )
    return cities


@st.cache_data(show_spinner=False, ttl=3600)
def load_all_inscriptions() -> pd.DataFrame:
    """Load JSONL with only needed columns - minimal memory footprint."""
    if not CLEANED_JSONL_FILE.exists():
        raise FileNotFoundError(f"Missing: {CLEANED_JSONL_FILE}. Run main.py first.")

    # Load JSONL and keep only required columns
    data = pd.read_json(CLEANED_JSONL_FILE, lines=True)
    
    # Select only columns we need
    cols_to_keep = [c for c in REQUIRED_COLS if c in data.columns]
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


def filter_inscriptions(data: pd.DataFrame, search_column: str, term: str) -> pd.DataFrame:
    """Fast string filtering without GeoDataFrame overhead."""
    if not term.strip():
        return data.iloc[:0]  # Empty dataframe
    
    search_text = data[search_column].fillna("").astype(str)
    matches = search_text.str.contains(term, case=False, na=False, regex=False)
    return data[matches].reset_index(drop=True)


def _format_popup(row: pd.Series) -> str:
    """Create popup HTML for a single inscription."""
    def safe_str(val):
        if val is None or pd.isna(val):
            return "-"
        return str(val)[:500]

    raw = safe_str(row.get("inscription_text", ""))
    interp = safe_str(row.get("inscription_text_interpretive", ""))
    cons = safe_str(row.get("inscription_text_conservative", ""))
    
    return (
        f"<b>{row.get('edcs_id', row.get('record_id', 'Inscription'))}</b><br>"
        f"<b>Raw:</b> {raw}<br>"
        f"<b>Interp:</b> {interp}<br>"
        f"<b>Cons:</b> {cons}<br>"
        f"<b>Place:</b> {safe_str(row.get('place'))}<br>"
        f"<b>Province:</b> {safe_str(row.get('province'))}"
    )


def build_map_fast(
    inscriptions: pd.DataFrame,
    show_provinces: bool,
    show_roads: bool,
    show_cities: bool,
) -> folium.Map:
    """Build map with ultra-lightweight rendering."""
    m = folium.Map(
        location=[41.9, 12.5],
        zoom_start=4,
        tiles=None,
        prefer_canvas=True,
    )

    # White background
    folium.Rectangle(
        bounds=[[-90, -180], [90, 180]],
        color="#ffffff",
        weight=0,
        fill=True,
        fill_color="#ffffff",
        fill_opacity=1.0,
        interactive=False,
    ).add_to(m)

    if show_provinces:
        provinces = load_provinces()
        province_colors = [
            "#f8dfd0",
            "#d9ebf6",
            "#e7f3dc",
            "#f3e4f8",
            "#f9edc9",
            "#dcefe9",
            "#fde2ea",
            "#e5e9fb",
        ]
        provinces_colored = provinces.reset_index(drop=True).copy()
        provinces_colored["_color"] = [
            province_colors[i % len(province_colors)]
            for i in range(len(provinces_colored))
        ]
        folium.GeoJson(
            provinces_colored.to_json(),
            name="Roman Provinces",
            style_function=lambda f: {
                "fillColor": f["properties"].get("_color", "#f0f0f0"),
                "color": "#8c8c8c",
                "weight": 0.35,
                "fillOpacity": 0.58,
            },
        ).add_to(m)

    if show_roads:
        roads = load_roads()
        folium.GeoJson(
            roads.to_json(),
            name="Roads",
            style_function=lambda _: {"color": "#999999", "weight": 0.3, "opacity": 0.6},
        ).add_to(m)

    if show_cities:
        cities = load_cities()
        for _, city in cities.iterrows():
            folium.CircleMarker(
                location=[city.geometry.y, city.geometry.x],
                radius=2,
                fill=True,
                fillColor="#228B6B",
                fillOpacity=0.3,
                weight=0.5,
                color="#228B6B",
            ).add_to(m)

    # Ultra-fast marker rendering using simple CircleMarker
    if not inscriptions.empty:
        ins_fg = folium.FeatureGroup(name="Inscriptions", show=True)
        for _, row in inscriptions.iterrows():
            marker = folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=2,
                fill=True,
                fillColor="#e33d2e",
                fillOpacity=0.7,
                weight=0.5,
                color="#a01010",
                popup=folium.Popup(_format_popup(row), max_width=400),
            )
            marker.add_to(ins_fg)
        ins_fg.add_to(m)

    folium.LayerControl(collapsed=False, position="topright").add_to(m)

    if not inscriptions.empty:
        bounds = [
            [inscriptions["latitude"].min(), inscriptions["longitude"].min()],
            [inscriptions["latitude"].max(), inscriptions["longitude"].max()],
        ]
        m.fit_bounds(bounds)

    return m


def main() -> None:
    st.set_page_config(
        page_title="EpigCorpus - EDCS Interactive Map",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    with st.sidebar:
        st.title("📍 EpigCorpus")
        st.subheader("EDCS Inscriptions Map")
        
        search_mode = st.radio(
            "Search in:",
            options=["Raw Text", "Interpretive", "Conservative"],
            index=0,
        )
        term = st.text_input("Keyword", value="viator", placeholder="Enter search term")
        
        st.divider()
        st.subheader("Map Layers")
        show_provinces = st.checkbox("Provinces", value=True)
        show_roads = st.checkbox("Roads", value=True)
        show_cities = st.checkbox("Cities", value=True)

    if not term.strip():
        st.warning("⚠️ Enter a search term to display results")
        return

    # Load all data once (cached)
    with st.spinner("⏳ Loading inscriptions..."):
        all_inscriptions = load_all_inscriptions()

    # Map search mode to column
    search_map = {
        "Raw Text": "inscription_text",
        "Interpretive": "inscription_text_interpretive",
        "Conservative": "inscription_text_conservative",
    }
    search_col = search_map[search_mode]

    # Fast filtering
    with st.spinner("🔍 Searching..."):
        filtered = filter_inscriptions(all_inscriptions, search_col, term)

    # Display stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Matches", f"{len(filtered):,}")
    with col2:
        st.metric("Mode", search_mode)
    with col3:
        st.metric("Term", term)

    if filtered.empty:
        st.warning("No matches found. Try different keywords.")
        return

    # Build and render map
    with st.spinner("🗺️ Building map..."):
        map_obj = build_map_fast(filtered, show_provinces, show_roads, show_cities)

    html(map_obj._repr_html_(), height=700, scrolling=False)

    # Results table
    if len(filtered) <= 500:
        st.subheader("Results")
        display_cols = [c for c in ["record_id", "place", "province", "not_before", "not_after"] if c in filtered.columns]
        st.dataframe(filtered[display_cols], use_container_width=True, height=300)


if __name__ == "__main__":
    main()

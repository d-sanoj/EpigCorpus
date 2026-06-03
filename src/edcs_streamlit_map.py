"""Optimized EDCS Streamlit map with ultra-fast loading."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from matplotlib.lines import Line2D
from streamlit.components.v1 import html


BASE_RAW = "https://raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SUPPORT_DIR = DATA_DIR / "lat_epig_support"
CLEANED_JSONL_FILE = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"
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
]

# Roman Empire approximate bounds (tight filter)
ROMAN_BOUNDS = (25, 50, -10, 45)  # (min_lat, max_lat, min_lon, max_lon)


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
        </style>
        """,
        unsafe_allow_html=True,
    )


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


@st.cache_data(show_spinner=False, ttl=3600)
def load_full_cleaned_data() -> pd.DataFrame:
    """Load full cleaned dataset with all columns for detailed result table."""
    if not CLEANED_JSONL_FILE.exists():
        raise FileNotFoundError(f"Missing: {CLEANED_JSONL_FILE}. Run main.py first.")
    return pd.read_json(CLEANED_JSONL_FILE, lines=True)


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
        f"<b>Raw inscription text:</b> {raw}<br>"
        f"<b>Interpretive cleaned text:</b> {interp}<br>"
        f"<b>Conservative cleaned text:</b> {cons}<br>"
        f"<b>Place:</b> {safe_str(row.get('place'))}<br>"
        f"<b>Province:</b> {safe_str(row.get('province'))}"
    )


def _add_fixed_legend(map_obj: folium.Map) -> None:
    legend_html = """
    <div style="
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 9999;
        background: rgba(255, 255, 255, 0.94);
        border: 1px solid #cfcfcf;
        border-radius: 8px;
        padding: 10px 12px;
        min-width: 165px;
        font-size: 12px;
        line-height: 1.3;
        box-shadow: 0 1px 6px rgba(0, 0, 0, 0.12);
        color: #2f2f2f;
    ">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <span style="display:inline-block;flex:0 0 14px;width:14px;height:10px;background:#f8dfd0;border:1px solid #8c8c8c;"></span>
            Roman Provinces
        </div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <span style="display:inline-block;flex:0 0 14px;width:14px;height:0;border-top:2px solid #8b9099;"></span>
            Roads
        </div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <span style="display:inline-block;flex:0 0 8px;width:8px;height:8px;border-radius:50%;background:#228B6B;opacity:0.5;"></span>
            Cities
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
            <span style="display:inline-block;flex:0 0 8px;width:8px;height:8px;border-radius:50%;background:#e33d2e;border:1px solid #a01010;"></span>
            Inscriptions
        </div>
    </div>
    """
    map_obj.get_root().html.add_child(folium.Element(legend_html))


def _province_palette() -> list[str]:
    return [
        "#f8dfd0",
        "#d9ebf6",
        "#e7f3dc",
        "#f3e4f8",
        "#f9edc9",
        "#dcefe9",
        "#fde2ea",
        "#e5e9fb",
    ]


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
def build_png_bytes(inscriptions: pd.DataFrame, search_term: str, search_mode: str) -> bytes:
    provinces = load_provinces().reset_index(drop=True).copy()
    roads = load_roads()
    cities = load_cities()
    inscriptions_geo = gpd.GeoDataFrame(
        inscriptions.copy(),
        geometry=gpd.points_from_xy(inscriptions["longitude"], inscriptions["latitude"]),
        crs="EPSG:4326",
    )

    provinces_proj = provinces.to_crs(epsg=3857)
    roads_proj = roads.to_crs(epsg=3857)
    cities_proj = cities.to_crs(epsg=3857)
    inscriptions_proj = inscriptions_geo.to_crs(epsg=3857)

    province_colors = _province_palette()
    provinces_proj["_color"] = [
        province_colors[i % len(province_colors)] for i in range(len(provinces_proj))
    ]

    fig, ax = plt.subplots(figsize=(11.8, 8.6), dpi=180)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    fig.subplots_adjust(left=0.06, right=0.94, top=0.88, bottom=0.08)

    provinces_proj.plot(
        ax=ax,
        color=provinces_proj["_color"],
        edgecolor="#9a9a9a",
        linewidth=0.25,
        alpha=0.58,
        zorder=1,
    )
    roads_proj.plot(ax=ax, color="#8b9099", linewidth=0.22, alpha=0.85, zorder=2)
    cities_proj.plot(ax=ax, color="#228B6B", markersize=3, alpha=0.30, zorder=3)

    if not inscriptions_proj.empty:
        inscriptions_proj.plot(
            ax=ax,
            color="#e33d2e",
            markersize=10,
            alpha=0.9,
            edgecolor="#7f1212",
            linewidth=0.15,
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

    title = f'Inscriptions matching "{search_term}" in {search_mode}'
    fig.suptitle(title, fontsize=17, fontfamily="serif", y=0.94)

    legend_handles = [
        Line2D([0], [0], color="#8b9099", lw=1.2, label="Roads"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#228B6B", markersize=5, alpha=0.5, label="Cities"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#e33d2e", markeredgecolor="#7f1212", markersize=5, label="Inscriptions"),
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(0.965, 0.965),
        frameon=True,
        fontsize=10,
        borderaxespad=0.0,
    )
    legend.get_frame().set_facecolor("white")
    legend.get_frame().set_edgecolor("#c6c6c6")

    fig.text(
        0.5,
        0.068,
        "\n".join(
            [
                f"Search term: {search_term}",
                f"Results: {len(inscriptions):,} | Search mode: {search_mode}",
                "Data source: Epigraphik-Datenbank Clauss / Slaby",
            ]
        ),
        ha="center",
        va="bottom",
        fontsize=8,
        color="#3f3f3f",
    )
    fig.text(
        0.5,
        0.02,
        PNG_CITATION,
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#2f2f2f",
    )

    buffer = BytesIO()
    fig.savefig(buffer, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def build_map_fast(
    inscriptions: pd.DataFrame,
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

    provinces = load_provinces()
    province_colors = _province_palette()
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

    roads = load_roads()
    folium.GeoJson(
        roads.to_json(),
        name="Roads",
        style_function=lambda _: {"color": "#8b9099", "weight": 0.8, "opacity": 0.9},
    ).add_to(m)

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

    _add_fixed_legend(m)

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

    with st.form("search_form", clear_on_submit=False):
        form_col_1, form_col_2, form_col_3 = st.columns([1.5, 2.4, 0.9])
        with form_col_1:
            search_mode = st.selectbox(
                "Search in:",
                options=[
                    "Raw inscriptions",
                    "Interpretive Cleaned Inscriptions",
                    "Conservative Cleaned Inscriptions",
                ],
                index=0,
            )
        with form_col_2:
            term = st.text_input("Keyword", value="viator", placeholder="Enter search term")
        with form_col_3:
            st.markdown("<div style='height: 1.72rem;'></div>", unsafe_allow_html=True)
            search_submitted = st.form_submit_button("Search", use_container_width=True)

    if "submitted_term" not in st.session_state:
        st.session_state["submitted_term"] = ""
    if "submitted_mode" not in st.session_state:
        st.session_state["submitted_mode"] = "Raw inscriptions"
    else:
        st.session_state["submitted_mode"] = _normalize_search_mode(
            st.session_state["submitted_mode"]
        )

    if search_submitted:
        st.session_state["submitted_term"] = term.strip()
        st.session_state["submitted_mode"] = _normalize_search_mode(search_mode)

    active_term = st.session_state["submitted_term"]
    active_mode = _normalize_search_mode(st.session_state["submitted_mode"])

    if not active_term:
        st.warning("Enter a keyword and press Enter or click Search to display results.")
        return

    # Load all data once (cached)
    with st.spinner("Loading inscriptions..."):
        all_inscriptions = load_all_inscriptions()

    # Map search mode to column
    search_map = {
        "Raw inscriptions": "inscription_text",
        "Interpretive Cleaned Inscriptions": "inscription_text_interpretive",
        "Conservative Cleaned Inscriptions": "inscription_text_conservative",
    }
    search_col = search_map[active_mode]

    # Fast filtering
    with st.spinner("Searching inscriptions..."):
        filtered = filter_inscriptions(all_inscriptions, search_col, active_term)

    st.markdown(
        (
            "<div class='matches-summary'>"
            f"Matches for \"{active_term}\" in {active_mode}: {len(filtered):,}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    if filtered.empty:
        st.warning("No matches found. Try different keywords.")
        return

    # Build and render map
    with st.spinner("Building map..."):
        map_obj = build_map_fast(filtered)
        png_bytes = build_png_bytes(filtered, active_term, active_mode)

    map_btn_col_1, map_btn_col_2, map_btn_col_3 = st.columns([3, 1, 3])
    with map_btn_col_2:
        st.download_button(
            label="Download as PNG",
            data=png_bytes,
            file_name=f"edcs_map_{active_term.replace(' ', '_').lower()}.png",
            mime="image/png",
            use_container_width=True,
        )

    html(map_obj._repr_html_(), height=700, scrolling=False)

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

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
RAW_JSONL_FILE = DATA_DIR / "edcs_inscriptions.jsonl"
CLEANED_JSONL_FILE = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"

TEXT_FIELD_OPTIONS = [
    "inscription_text_interpretive",
    "inscription_text_conservative",
]


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


@st.cache_data(show_spinner=False)
def load_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    SUPPORT_DIR.mkdir(parents=True, exist_ok=True)

    roads_shp = download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/ba_roads",
        "ba_roads",
        SUPPORT_DIR / "ba_roads",
    )
    provinces_shp = download_shapefile_components(
        f"{BASE_RAW}/awmc.unc.edu/awmc/map_data/shapefiles/cultural_data/political_shading/roman_empire_ad_117/shape",
        "roman_empire_ad_117",
        SUPPORT_DIR / "roman_empire_ad_117",
    )
    cities_csv = download_if_missing(
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

    return provinces, roads, cities


@st.cache_data(show_spinner=False)
def load_inscriptions() -> pd.DataFrame:
    source_file = CLEANED_JSONL_FILE if CLEANED_JSONL_FILE.exists() else RAW_JSONL_FILE
    if not source_file.exists():
        raise FileNotFoundError(f"Missing dataset: {source_file}")

    source_data = pd.read_json(source_file, lines=True)

    # If only raw text exists, keep Streamlit usable by deriving both display fields.
    if "inscription_text_interpretive" not in source_data.columns and "inscription_text" in source_data.columns:
        source_data["inscription_text_interpretive"] = source_data["inscription_text"]
    if "inscription_text_conservative" not in source_data.columns and "inscription_text" in source_data.columns:
        source_data["inscription_text_conservative"] = source_data["inscription_text"]

    source_data = source_data.replace(r"^\s*$", pd.NA, regex=True)
    source_data["latitude"] = pd.to_numeric(source_data["latitude"], errors="coerce")
    source_data["longitude"] = pd.to_numeric(source_data["longitude"], errors="coerce")
    source_data = source_data.dropna(subset=["latitude", "longitude"])
    source_data = source_data[
        source_data["latitude"].between(-90, 90) & source_data["longitude"].between(-180, 180)
    ]

    return source_data


def prepare_filtered_data(
    source_data: pd.DataFrame,
    provinces: gpd.GeoDataFrame,
    text_column: str,
    term: str,
    limit: int,
) -> gpd.GeoDataFrame:
    if text_column not in source_data.columns:
        raise ValueError(f"Column '{text_column}' not found in dataset.")

    map_df = source_data.dropna(subset=[text_column]).copy()
    if term:
        map_df = map_df[map_df[text_column].astype(str).str.contains(term, case=False, na=False, regex=False)]

    inscriptions = gpd.GeoDataFrame(
        map_df,
        geometry=gpd.points_from_xy(map_df["longitude"], map_df["latitude"]),
        crs="EPSG:4326",
    )

    roman_union = provinces.geometry.union_all()
    inscriptions = inscriptions[inscriptions.geometry.within(roman_union)]

    if limit > 0:
        inscriptions = inscriptions.head(limit)

    return inscriptions


def _format_popup(record: pd.Series, text_column: str) -> str:
    def text(value: object) -> str:
        if value is None or value is pd.NA:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value)

    category_en = record.get("category_en", [])
    if isinstance(category_en, list):
        category_en = " | ".join(str(item) for item in category_en)

    belege = record.get("belege", [])
    if isinstance(belege, list):
        belege = " | ".join(str(item) for item in belege)

    popup_text = text(record.get(text_column, ""))
    popup_text = popup_text[:500] + ("..." if len(popup_text) > 500 else "")

    date_range = " - ".join(part for part in [text(record.get("not_before")), text(record.get("not_after"))] if part)

    return (
        f"<b>{text(record.get('edcs_id', record.get('record_id', 'Inscription')))}</b><br>"
        f"<span style='font-family:serif;'>{popup_text}</span><br><br>"
        f"<b>Place:</b> {text(record.get('place')) or '-'}<br>"
        f"<b>Province:</b> {text(record.get('province')) or '-'}<br>"
        f"<b>Dates:</b> {date_range or '-'}<br>"
        f"<b>Language:</b> {text(record.get('language')) or '-'}<br>"
        f"<b>Material:</b> {text(record.get('material_en', record.get('material'))) or '-'}<br>"
        f"<b>Categories:</b> {text(category_en) or '-'}<br>"
        f"<b>Belege:</b> {text(belege) or '-'}"
    )


def build_map(
    provinces: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    cities: gpd.GeoDataFrame,
    inscriptions: gpd.GeoDataFrame,
    text_column: str,
    show_provinces: bool,
    show_roads: bool,
    show_cities: bool,
    show_inscriptions: bool,
) -> folium.Map:
    m = folium.Map(
        location=[41.9, 12.5],
        zoom_start=4,
        tiles="https://stamen-tiles.a.ssl.fastly.net/terrain-background/{z}/{x}/{y}.png",
        attr=(
            "Map tiles by Stamen Design, under CC BY 3.0. Data by OpenStreetMap, under ODbL."
        ),
        prefer_canvas=True,
    )

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

    provinces_colored = provinces.reset_index(drop=True).copy()
    provinces_colored["_color"] = [province_colors[i % len(province_colors)] for i in range(len(provinces_colored))]

    if show_provinces:
        folium.GeoJson(
            provinces_colored.to_json(),
            name="Roman Provinces",
            style_function=lambda feat: {
                "fillColor": feat["properties"]["_color"],
                "color": "#c4c4c4",
                "weight": 0.5,
                "fillOpacity": 0.45,
            },
        ).add_to(m)

    if show_roads:
        folium.GeoJson(
            roads.to_json(),
            name="Roads",
            style_function=lambda _: {"color": "#666666", "weight": 1.4, "opacity": 0.9},
        ).add_to(m)

    if show_cities:
        cities_fg = folium.FeatureGroup(name="Cities", show=True)
        for _, city in cities.iterrows():
            folium.CircleMarker(
                location=[city.geometry.y, city.geometry.x],
                radius=1.8,
                color="#7a7a7a",
                weight=0.7,
                fill=True,
                fill_color="#7a7a7a",
                fill_opacity=0.45,
            ).add_to(cities_fg)
        cities_fg.add_to(m)

    if show_inscriptions:
        ins_fg = folium.FeatureGroup(name="Inscriptions", show=True)
        for _, row in inscriptions.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=3,
                color="#2f2f2f",
                weight=0.4,
                fill=True,
                fill_color="#cb2f2f",
                fill_opacity=0.86,
                popup=folium.Popup(_format_popup(row, text_column), max_width=420),
            ).add_to(ins_fg)
        ins_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    if not inscriptions.empty:
        bounds = [[inscriptions.geometry.y.min(), inscriptions.geometry.x.min()], [inscriptions.geometry.y.max(), inscriptions.geometry.x.max()]]
        m.fit_bounds(bounds)

    return m


def main() -> None:
    st.set_page_config(page_title="EDCS Streamlit Map", page_icon="🗺️", layout="wide")

    st.title("EDCS Interactive Map")
    st.caption("Roman Empire inscription explorer powered by Streamlit and Folium")

    source_data = load_inscriptions()
    provinces, roads, cities = load_layers()

    available_text_fields = [field for field in TEXT_FIELD_OPTIONS if field in source_data.columns]
    if not available_text_fields:
        st.error(
            "Neither inscription_text_interpretive nor inscription_text_conservative is available in the dataset."
        )
        return

    with st.sidebar:
        st.header("Filters")
        text_column = st.selectbox("Text field", available_text_fields, index=0)
        term = st.text_input("Search term", value="viator", placeholder="Type a term")
        limit = st.slider("Maximum records shown", min_value=50, max_value=5000, value=500, step=50)

        st.header("Layers")
        show_provinces = st.checkbox("Roman provinces", value=True)
        show_roads = st.checkbox("Roads", value=True)
        show_cities = st.checkbox("Cities", value=True)
        show_inscriptions = st.checkbox("Inscriptions", value=True)

    inscriptions = prepare_filtered_data(
        source_data=source_data,
        provinces=provinces,
        text_column=text_column,
        term=term.strip(),
        limit=limit,
    )

    stat_col_1, stat_col_2, stat_col_3 = st.columns(3)
    stat_col_1.metric("Matches", f"{len(inscriptions):,}")
    stat_col_2.metric("Text field", text_column)
    stat_col_3.metric("Search term", term if term else "(none)")

    if inscriptions.empty:
        st.warning("No inscriptions matched your filter. Try a broader term.")
        return

    map_object = build_map(
        provinces=provinces,
        roads=roads,
        cities=cities,
        inscriptions=inscriptions,
        text_column=text_column,
        show_provinces=show_provinces,
        show_roads=show_roads,
        show_cities=show_cities,
        show_inscriptions=show_inscriptions,
    )

    html(map_object._repr_html_(), height=760, scrolling=False)

    st.subheader("Matched Inscriptions")
    table_columns = [
        col
        for col in [
            "record_id",
            "edcs_id",
            "province",
            "place",
            "not_before",
            "not_after",
            text_column,
        ]
        if col in inscriptions.columns
    ]
    st.dataframe(inscriptions[table_columns], use_container_width=True, height=360)


if __name__ == "__main__":
    main()

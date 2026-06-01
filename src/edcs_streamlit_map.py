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
    source_file = CLEANED_JSONL_FILE
    if not source_file.exists():
        raise FileNotFoundError(
            f"Missing cleaned dataset: {source_file}. Run main.py first to generate cleaned outputs."
        )

    source_data = pd.read_json(source_file, lines=True)

    required_cols = ["inscription_text", "inscription_text_interpretive", "inscription_text_conservative"]
    missing_cols = [col for col in required_cols if col not in source_data.columns]
    if missing_cols:
        raise ValueError(f"Cleaned dataset is missing required columns: {', '.join(missing_cols)}")

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
    search_column: str,
    term: str,
) -> gpd.GeoDataFrame:
    map_df = source_data.copy()
    if search_column not in map_df.columns:
        raise ValueError(f"Search column '{search_column}' not found in dataset.")

    search_text = map_df[search_column].fillna("").astype(str)

    if term:
        matches = search_text.str.contains(term, case=False, na=False, regex=False)
        map_df = map_df[matches]

    inscriptions = gpd.GeoDataFrame(
        map_df,
        geometry=gpd.points_from_xy(map_df["longitude"], map_df["latitude"]),
        crs="EPSG:4326",
    )

    roman_union = provinces.geometry.union_all()
    inscriptions = inscriptions[inscriptions.geometry.within(roman_union)]

    return inscriptions


def _format_popup(record: pd.Series) -> str:
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

    raw_text = text(record.get("inscription_text", ""))
    interpretive_text = text(record.get("inscription_text_interpretive", ""))
    conservative_text = text(record.get("inscription_text_conservative", ""))

    raw_text = raw_text[:500] + ("..." if len(raw_text) > 500 else "")
    interpretive_text = interpretive_text[:500] + ("..." if len(interpretive_text) > 500 else "")
    conservative_text = conservative_text[:500] + ("..." if len(conservative_text) > 500 else "")

    date_range = " - ".join(part for part in [text(record.get("not_before")), text(record.get("not_after"))] if part)

    return (
        f"<b>{text(record.get('edcs_id', record.get('record_id', 'Inscription')))}</b><br>"
        f"<b>Raw inscription_text:</b><br><span style='font-family:serif;'>{raw_text or '-'}</span><br><br>"
        f"<b>Interpretive:</b><br><span style='font-family:serif;'>{interpretive_text or '-'}</span><br><br>"
        f"<b>Conservative:</b><br><span style='font-family:serif;'>{conservative_text or '-'}</span><br><br>"
        f"<b>Place:</b> {text(record.get('place')) or '-'}<br>"
        f"<b>Province:</b> {text(record.get('province')) or '-'}<br>"
        f"<b>Dates:</b> {date_range or '-'}<br>"
        f"<b>Language:</b> {text(record.get('language')) or '-'}<br>"
        f"<b>Material:</b> {text(record.get('material_en', record.get('material'))) or '-'}<br>"
        f"<b>Categories:</b> {text(category_en) or '-'}<br>"
        f"<b>Belege:</b> {text(belege) or '-'}"
    )


def _format_hover_text(record: pd.Series) -> str:
    def text(value: object) -> str:
        if value is None or value is pd.NA:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value)

    raw = text(record.get("inscription_text", "")).replace("\n", " ").strip()
    interp = text(record.get("inscription_text_interpretive", "")).replace("\n", " ").strip()
    cons = text(record.get("inscription_text_conservative", "")).replace("\n", " ").strip()

    if len(raw) > 80:
        raw = raw[:77] + "..."
    if len(interp) > 120:
        interp = interp[:117] + "..."
    if len(cons) > 120:
        cons = cons[:117] + "..."

    label = text(record.get("edcs_id", record.get("record_id", "Inscription")))
    return f"{label} | Raw: {raw or '-'} | I: {interp or '-'} | C: {cons or '-'}"


def build_map(
    provinces: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    cities: gpd.GeoDataFrame,
    inscriptions: gpd.GeoDataFrame,
    show_provinces: bool,
    show_roads: bool,
    show_cities: bool,
    show_inscriptions: bool,
) -> folium.Map:
    m = folium.Map(
        location=[41.9, 12.5],
        zoom_start=4,
        tiles=None,
        prefer_canvas=True,
    )

    # Keep a neutral, publication-like white backdrop.
    folium.Rectangle(
        bounds=[[-90, -180], [90, 180]],
        color="#ffffff",
        weight=0,
        fill=True,
        fill_color="#ffffff",
        fill_opacity=1.0,
        interactive=False,
    ).add_to(m)

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
    provinces_colored["_color"] = [province_colors[i % len(province_colors)] for i in range(len(provinces_colored))]

    if show_provinces:
        folium.GeoJson(
            provinces_colored.to_json(),
            name="Roman Provinces",
            style_function=lambda feat: {
                "fillColor": feat["properties"]["_color"],
                "color": "#000000",
                "weight": 0,
                "fillOpacity": 0.55,
            },
        ).add_to(m)

    if show_roads:
        folium.GeoJson(
            roads.to_json(),
            name="Roads",
            style_function=lambda _: {"color": "#4e5d72", "weight": 0.4, "opacity": 0.92},
        ).add_to(m)

    if show_cities:
        cities_fg = folium.FeatureGroup(name="Cities", show=True)
        for _, city in cities.iterrows():
            folium.Marker(
                location=[city.geometry.y, city.geometry.x],
                icon=folium.DivIcon(
                    icon_size=(20, 20),
                    icon_anchor=(10, 10),
                    html=(
                        "<div style='font-size:7.5px; line-height:7.5px; "
                        "color:rgba(34,107,107,0.35); font-weight:700;'>x</div>"
                    ),
                ),
            ).add_to(cities_fg)
        cities_fg.add_to(m)

    if show_inscriptions:
        ins_fg = folium.FeatureGroup(name="Inscriptions", show=True)
        for _, row in inscriptions.iterrows():
            marker = folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=folium.DivIcon(
                    icon_size=(20, 20),
                    icon_anchor=(10, 10),
                    html=(
                        "<div style='width:20px;height:20px;display:flex;align-items:center;justify-content:center;'>"
                        "<div style='width:5px;height:5px;border-radius:50%;"
                        "background:#e33d2e;opacity:0.9;pointer-events:none;"
                        "border:0.01px solid #5a1212;box-sizing:border-box;'></div>"
                        "</div>"
                    ),
                ),
            )
            marker.add_child(folium.Popup(_format_popup(row), max_width=420, auto_pan=True))
            marker.add_to(ins_fg)
        ins_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    if not inscriptions.empty:
        bounds = [[inscriptions.geometry.y.min(), inscriptions.geometry.x.min()], [inscriptions.geometry.y.max(), inscriptions.geometry.x.max()]]
        m.fit_bounds(bounds)

        # Lock zoom-out at the initial fitted extent so users cannot zoom out farther.
        lock_extent_script = f"""
        <script>
        (function() {{
            var map = {m.get_name()};
            function lockExtent() {{
                var fittedBounds = map.getBounds();
                var minZoom = map.getZoom();

                map.setMaxBounds(fittedBounds);
                map.options.maxBoundsViscosity = 1.0;
                map.setMinZoom(minZoom);

                map.on('zoomend', function() {{
                    if (map.getZoom() < minZoom) {{
                        map.setZoom(minZoom);
                    }}
                }});

                map.on('dragend', function() {{
                    map.panInsideBounds(fittedBounds, {{ animate: false }});
                }});
            }}
            map.whenReady(lockExtent);
            setTimeout(lockExtent, 0);
        }})();
        </script>
        """
        m.get_root().html.add_child(folium.Element(lock_extent_script))

    return m


def main() -> None:
    st.set_page_config(page_title="EDCS Streamlit Map", page_icon="🗺️", layout="wide")

    source_data = load_inscriptions()
    provinces, roads, cities = load_layers()

    required_text_fields = ["inscription_text_interpretive", "inscription_text_conservative"]
    missing_text_fields = [field for field in required_text_fields if field not in source_data.columns]
    if missing_text_fields:
        st.error(
            "Required cleaned columns are missing in cleaned dataset: " + ", ".join(missing_text_fields)
        )
        return

    with st.sidebar:
        st.title("EDCS Interactive Map")
        st.header("Filters")
        search_mode = st.selectbox(
            "Search in",
            options=["raw", "interpretive", "conservative"],
            index=0,
        )
        term = st.text_input("Search term", value="viator", placeholder="Type a term")

        st.header("Layers")
        show_provinces = st.checkbox("Roman provinces", value=True)
        show_roads = st.checkbox("Roads", value=True)
        show_cities = st.checkbox("Cities", value=True)
        show_inscriptions = st.checkbox("Inscriptions", value=True)

    search_column_map = {
        "raw": "inscription_text",
        "interpretive": "inscription_text_interpretive",
        "conservative": "inscription_text_conservative",
    }
    search_column = search_column_map[search_mode]

    inscriptions = prepare_filtered_data(
        source_data=source_data,
        provinces=provinces,
        search_column=search_column,
        term=term.strip(),
    )

    stat_col_1, stat_col_2, stat_col_3 = st.columns(3)
    stat_col_1.metric("Matches", f"{len(inscriptions):,}")
    stat_col_2.metric("Search field", search_column)
    stat_col_3.metric("Search term", term if term else "(none)")

    if inscriptions.empty:
        st.warning("No inscriptions matched your filter. Try a broader term.")
        return

    map_object = build_map(
        provinces=provinces,
        roads=roads,
        cities=cities,
        inscriptions=inscriptions,
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
            "inscription_text",
            "inscription_text_interpretive",
            "inscription_text_conservative",
        ]
        if col in inscriptions.columns
    ]
    st.dataframe(inscriptions[table_columns], use_container_width=True, height=360)


if __name__ == "__main__":
    main()

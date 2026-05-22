from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import requests


BASE_RAW = "https://raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main"
SUPPORT_DIR = Path("data/lat_epig_support")


def _download_if_missing(url: str, destination: Path) -> Path:
    if destination.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def _download_shapefile_components(base_url: str, stem: str, local_dir: Path) -> Path:
    required = [".shp", ".shx", ".dbf", ".prj"]
    optional = [".sbn", ".sbx", ".cpg"]

    for ext in required:
        _download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")
    for ext in optional:
        try:
            _download_if_missing(f"{base_url}/{stem}{ext}", local_dir / f"{stem}{ext}")
        except requests.HTTPError:
            pass

    return local_dir / f"{stem}.shp"


def build_interactive_map(source_data: pd.DataFrame | None = None, term: str = "viator") -> folium.Map:
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

    if source_data is None:
        source_data = pd.read_json("data/edcs_inscriptions.jsonl", lines=True)

    text_col = None
    for candidate in ["inscription_text_interpretive", "inscription_text_conservative", "inscription_text"]:
        if candidate in source_data.columns:
            text_col = candidate
            break
    if text_col is None:
        raise ValueError("No inscription text column found.")

    map_df = source_data.copy()
    map_df = map_df.replace(r"^\s*$", pd.NA, regex=True)
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude", text_col])
    map_df = map_df[map_df["latitude"].between(-90, 90) & map_df["longitude"].between(-180, 180)]
    map_df = map_df[map_df[text_col].str.contains(rf"\b{term}\b", case=False, na=False)]

    if map_df.empty:
        raise ValueError(f"No records with term '{term}' and valid coordinates were found.")

    inscriptions = gpd.GeoDataFrame(
        map_df,
        geometry=gpd.points_from_xy(map_df["longitude"], map_df["latitude"]),
        crs="EPSG:4326",
    )
    roads = gpd.read_file(roads_shp).to_crs(epsg=4326)
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=4326)

    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"]),
        crs="EPSG:4326",
    )

    roman_union = provinces.geometry.unary_union
    inscriptions = inscriptions[inscriptions.geometry.within(roman_union)]
    roads = roads.clip(roman_union)
    cities = cities[cities.geometry.within(roman_union)]

    if inscriptions.empty:
        raise ValueError(f"No '{term}' inscriptions fall within Roman Empire AD 117 extent.")

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
    provinces = provinces.reset_index(drop=True).copy()
    provinces["_color"] = [province_colors[i % len(province_colors)] for i in range(len(provinces))]

    m = folium.Map(location=[41.9, 12.5], zoom_start=4, tiles="CartoDB positron", prefer_canvas=True)

    folium.GeoJson(
        provinces.to_json(),
        name="Roman Provinces (AD 117)",
        style_function=lambda feat: {
            "fillColor": feat["properties"]["_color"],
            "color": "#c4c4c4",
            "weight": 0.5,
            "fillOpacity": 0.45,
        },
    ).add_to(m)

    folium.GeoJson(
        roads.to_json(),
        name="Roads",
        style_function=lambda _: {"color": "#666666", "weight": 1.8, "opacity": 0.95},
    ).add_to(m)

    cities_fg = folium.FeatureGroup(name="Cities", show=True)
    for _, row in cities.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=1.8,
            color="#7a7a7a",
            weight=0.7,
            fill=True,
            fill_color="#7a7a7a",
            fill_opacity=0.45,
        ).add_to(cities_fg)
    cities_fg.add_to(m)

    ins_fg = folium.FeatureGroup(name="Inscriptions", show=True)
    for _, row in inscriptions.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=2.8,
            color="#2f2f2f",
            weight=0.4,
            fill=True,
            fill_color="#cb2f2f",
            fill_opacity=0.85,
            popup=str(row.get("edcs_id", "Inscription")),
        ).add_to(ins_fg)
    ins_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    title_html = """
    <div style="position: fixed; top: 10px; left: 50%; transform: translateX(-50%); z-index: 9999;
                font-size: 30px; font-family: serif; color: #111; background: rgba(233,233,233,0.85);
                padding: 4px 10px; border-radius: 6px;">
    Inscriptions containing the term 'viator' (passer-by)
    </div>
    """
    legend_html = """
    <div style="position: fixed; top: 80px; right: 15px; z-index: 9999;
                background: rgba(245,245,245,0.95); border: 2px solid #b5b5b5; border-radius: 6px;
                padding: 10px 12px; font-size: 18px; font-family: serif; color: #111;">
    <div><span style="display:inline-block; width:28px; height:0; border-top:3px solid #666; margin-right:8px; vertical-align:middle;"></span>Roads</div>
    <div style="margin-top:6px;"><span style="display:inline-block; width:28px; text-align:center; margin-right:8px; color:#777;">+</span>Cities</div>
    <div style="margin-top:6px;"><span style="display:inline-block; width:9px; height:9px; border:1px solid #222; border-radius:50%; background:#cb2f2f; margin-right:8px; vertical-align:middle;"></span>Inscriptions</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))
    m.get_root().html.add_child(folium.Element(legend_html))

    bounds = provinces.total_bounds
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    print(
        f"Interactive map loaded with {len(inscriptions):,} '{term}' inscriptions, "
        f"{len(cities):,} cities, and {len(roads):,} road segments."
    )

    return m

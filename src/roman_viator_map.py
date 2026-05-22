from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import requests


BASE_RAW = "https://raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SUPPORT_DIR = DATA_DIR / "lat_epig_support"
OUTPUT_DIR = ROOT_DIR / "output_maps"
OUTPUT_FILE = OUTPUT_DIR / "viator_roman_empire_map.png"


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


def load_inscriptions() -> pd.DataFrame:
    jsonl_file = DATA_DIR / "edcs_inscriptions.jsonl"
    if not jsonl_file.exists():
        raise FileNotFoundError(f"Missing dataset: {jsonl_file}")

    source_data = pd.read_json(jsonl_file, lines=True)

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

    map_df = map_df[map_df[text_col].str.contains(r"\bviator\b", case=False, na=False)]
    if map_df.empty:
        raise ValueError("No records with term 'viator' and valid coordinates were found.")

    return map_df


def build_map() -> Path:
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

    map_df = load_inscriptions()

    inscriptions = gpd.GeoDataFrame(
        map_df,
        geometry=gpd.points_from_xy(map_df["longitude"], map_df["latitude"]),
        crs="EPSG:4326",
    ).to_crs(epsg=3857)

    roads = gpd.read_file(roads_shp).to_crs(epsg=3857)
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=3857)

    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"], crs="EPSG:4326"),
    ).to_crs(epsg=3857)

    roman_union = provinces.geometry.union_all()
    inscriptions = inscriptions[inscriptions.geometry.within(roman_union)]
    roads = roads.clip(roman_union)
    cities = cities[cities.geometry.within(roman_union)]

    if inscriptions.empty:
        raise ValueError("No 'viator' inscriptions fall within the Roman Empire AD 117 extent.")

    province_palette = ListedColormap(
        [
            "#f6dce0",
            "#dff0e2",
            "#f6f2df",
            "#e7def6",
            "#e8f4da",
            "#f2e6d8",
            "#e4d6ef",
            "#f3dfec",
        ]
    )
    provinces = provinces.copy()
    provinces["_province_id"] = np.arange(len(provinces))

    fig, ax = plt.subplots(figsize=(16, 10), dpi=120)
    fig.patch.set_facecolor("#e9e9e9")
    ax.set_facecolor("#e9e9e9")

    provinces.plot(
        ax=ax,
        column="_province_id",
        cmap=province_palette,
        linewidth=0.35,
        edgecolor="#b9b9b9",
        alpha=0.55,
        zorder=1,
    )
    roads.plot(
        ax=ax,
        linewidth=0.55,
        alpha=0.9,
        color="#6b6b6b",
        zorder=2,
    )
    cities.plot(
        ax=ax,
        marker="+",
        markersize=14,
        linewidth=0.35,
        alpha=0.45,
        color="#7a7a7a",
        zorder=3,
    )
    inscriptions.plot(
        ax=ax,
        marker="o",
        markersize=5,
        alpha=0.8,
        color="#cb2f2f",
        edgecolor="#2f2f2f",
        linewidth=0.15,
        zorder=4,
    )

    legend_elements = [
        Line2D([0], [0], color="#6b6b6b", lw=1.4, label="Roads"),
        Line2D([0], [0], marker="+", linestyle="None", color="#7a7a7a", markersize=8, label="Cities"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="None",
            markerfacecolor="#cb2f2f",
            markeredgecolor="#2f2f2f",
            markersize=4,
            label="Inscriptions",
        ),
    ]
    ax.legend(handles=legend_elements, loc="upper right", frameon=True, facecolor="#efefef", edgecolor="#a9a9a9")

    ax.set_title("Inscriptions containing the term 'viator' (passer-by)", fontsize=18, fontfamily="serif", pad=10)

    xmin, ymin, xmax, ymax = provinces.total_bounds
    ax.set_xlim(xmin - 300000, xmax + 300000)
    ax.set_ylim(ymin - 550000, ymax + 250000)

    ax.annotate(
        "N",
        xy=(0.02, 0.11),
        xytext=(0.02, 0.045),
        xycoords="axes fraction",
        arrowprops={"arrowstyle": "-|>", "color": "black", "lw": 1.2},
        ha="center",
        va="center",
        fontsize=10,
        fontfamily="serif",
    )

    scale_len_m = 1_000_000
    sx = xmin + (xmax - xmin) * 0.45
    sy = ymin + (ymax - ymin) * 0.03
    ax.plot([sx, sx + scale_len_m], [sy, sy], color="black", linewidth=3)
    ax.text(sx + scale_len_m / 2, sy + 60000, "1000 km", ha="center", va="bottom", fontsize=16, fontfamily="serif")

    citation = (
        "Ballsun-Stanton B., Hermankova P., Laurence R. \"Lat Epig\" (version 2.0). GitHub.\n"
        "https://github.com/mqAncientHistory/Lat-Epig/    https://doi.org/10.5281/zenodo.5211341"
    )
    fig.text(0.5, 0.01, citation, ha="center", va="bottom", fontsize=8, fontfamily="serif")

    ax.axis("off")
    plt.tight_layout()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_FILE, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return OUTPUT_FILE


if __name__ == "__main__":
    out = build_map()
    print(f"Saved map to: {out}")

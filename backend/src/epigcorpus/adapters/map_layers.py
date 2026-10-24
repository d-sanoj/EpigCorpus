"""Vendored historical map layers: Roman provinces, roads and cities.

Provenance and licence terms: data/reference/map_layers/README.md.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

from epigcorpus.config.paths import MAP_LAYERS_DIR


def map_layer_path(*parts: str) -> Path:
    """Resolve a vendored base-map file, failing with an actionable message."""
    path = MAP_LAYERS_DIR.joinpath(*parts)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing base-map layer: {path}\n"
            "These files are vendored in the repository under data/reference/map_layers/. "
            "Restore them with: git checkout -- data/reference/map_layers"
        )
    return path


def load_provinces() -> gpd.GeoDataFrame:
    """Load and cache the Roman provinces shapefile (AWMC, AD 117)."""
    provinces_shp = map_layer_path("roman_empire_ad_117", "roman_empire_ad_117.shp")
    provinces = gpd.read_file(provinces_shp).to_crs(epsg=4326)
    return provinces


def load_roads() -> gpd.GeoDataFrame:
    """Load and cache the Roman roads shapefile (AWMC, Barrington Atlas)."""
    roads_shp = map_layer_path("ba_roads", "ba_roads.shp")
    roads = gpd.read_file(roads_shp).to_crs(epsg=4326)
    return roads


def load_cities() -> gpd.GeoDataFrame:
    """Load and cache the cities dataset (Hanson 2016, OXREP)."""
    cities_csv = map_layer_path("Hanson2016_Cities_OxREP.csv")
    cities_df = pd.read_csv(cities_csv, encoding="iso-8859-1")
    cities = gpd.GeoDataFrame(
        cities_df,
        geometry=gpd.points_from_xy(cities_df["Longitude (X)"], cities_df["Latitude (Y)"], crs="EPSG:4326"),
    )
    return cities


def load_land() -> gpd.GeoDataFrame:
    """Land outlines for the map's background (Natural Earth 1:50m, the whole world)."""
    return gpd.read_file(map_layer_path("natural_earth_land_50m.geojson")).to_crs(epsg=4326)

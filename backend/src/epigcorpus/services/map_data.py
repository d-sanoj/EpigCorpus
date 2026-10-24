"""Map data use cases: the historical layers as browser-ready GeoJSON and points.

Each function takes the (possibly cached) layer loader, so the UI's cached
loaders stay shared with the PNG export.
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.affinity import translate
from shapely.geometry import Polygon, box


def _province_palette() -> list[str]:
    """Muted province fills: the map's base, kept quiet so every marker on it reads.

    Eight hues spread round the wheel at low saturation and similar lightness;
    the bright marker colours (teal places, yellow cities, orange results)
    stay clear of them.
    """
    return [
        "#5b4b8a",  # violet
        "#3f6b8f",  # steel blue
        "#4f7a4a",  # moss
        "#8a7440",  # ochre
        "#8a4f4f",  # brick
        "#3f7f7a",  # slate teal
        "#7a4f73",  # plum
        "#55608f",  # indigo
    ]


def provinces_geojson(load_provinces: Callable[[], Any]) -> dict:
    """Provinces as GeoJSON, lightly simplified and pre-coloured for the browser."""
    provinces = load_provinces().reset_index(drop=True).copy()
    palette = _province_palette()
    provinces["_color"] = [palette[i % len(palette)] for i in range(len(provinces))]
    provinces["geometry"] = provinces.geometry.simplify(0.01, preserve_topology=True)
    return json.loads(provinces[["_color", "geometry"]].to_json())


# How far outside the shading a province label may still take its colour.
NEAREST_DEGREES = 0.75


def province_labels(inscriptions: pd.DataFrame, load_provinces: Callable[[], Any]) -> list[dict]:
    """One label per EDCS province, placed among its places, coloured like the shading.

    The AWMC layer has no names, and its polygons are not one per province
    (one shape covers Syria, Galatia and Pontus), so the names come from the
    corpus instead: each EDCS province is anchored at the median latitude and
    longitude of its distinct places, and takes the fill colour of the
    shaded polygon under that point (provinces_geojson's colours), or of the
    nearest one within NEAREST_DEGREES, or none. Ordered by inscription count, so
    a client hiding overlaps keeps the largest provinces.
    """
    frame = inscriptions.dropna(subset=["province", "latitude", "longitude"])
    frame = frame[frame["province"].astype(str).str.strip() != ""]
    if frame.empty:
        return []
    counts = frame.groupby("province").size()
    places = frame.drop_duplicates(["province", "place", "latitude", "longitude"])
    anchors = places.groupby("province").agg(
        latitude=("latitude", "median"),
        longitude=("longitude", "median"),
        places=("place", "size"),
    )
    anchors["inscriptions"] = counts
    anchors = anchors.reset_index().sort_values(
        ["inscriptions", "province"], ascending=[False, True]
    )

    provinces = load_provinces().reset_index(drop=True)
    palette = _province_palette()
    colors = gpd.GeoDataFrame(
        {"fill": [palette[i % len(palette)] for i in range(len(provinces))]},
        geometry=provinces.geometry,
        crs=provinces.crs,
    )
    points = gpd.GeoDataFrame(
        anchors,
        geometry=gpd.points_from_xy(anchors["longitude"], anchors["latitude"]),
        crs="EPSG:4326",
    ).to_crs(colors.crs)
    joined = gpd.sjoin(points, colors, how="left", predicate="within")
    # A point on a shared border can match two polygons; keep the first.
    joined = joined[~joined.index.duplicated(keep="first")]
    # An island province's median can land in the sea between its islands
    # (Achaia, in the Aegean): take the nearest shading within NEAREST_DEGREES.
    missing = joined["fill"].isna()
    if missing.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)  # distances in degrees, on purpose
            nearest = gpd.sjoin_nearest(
                points[missing.to_numpy()], colors, how="left", max_distance=NEAREST_DEGREES
            )
        nearest = nearest[~nearest.index.duplicated(keep="first")]
        joined.loc[nearest.index, "fill"] = nearest["fill"]

    labels = []
    for row in joined.itertuples(index=False):
        color = row.fill if isinstance(row.fill, str) else None
        labels.append(
            {
                "name": str(row.province),
                "latitude": round(float(row.latitude), 5),
                "longitude": round(float(row.longitude), 5),
                "inscriptions": int(row.inscriptions),
                "places": int(row.places),
                "color": color,
            }
        )
    return labels


def land_geojson(load_land: Callable[[], Any]) -> dict:
    """Land outlines as GeoJSON: the map's fallback background.

    The web map draws CARTO's dark tiles; when those cannot be loaded it draws
    this instead. It is the whole world's land, so no cut edge shows at any
    zoom. Natural Earth draws a few polygons straight across the antimeridian
    (Afro-Eurasia itself, at the Bering Strait), and a flat map would draw
    each of those as a sliver across the whole world. They are split at ±180°;
    Antarctica, which wraps the pole, is left out.
    """
    land = load_land().explode(index_parts=False).reset_index(drop=True)
    parts = []
    for polygon in land.geometry:
        if not _crosses_antimeridian(polygon):
            parts.append(polygon)
        elif polygon.bounds[3] > -60:  # not Antarctica
            parts.extend(_split_at_antimeridian(polygon))
    frame = gpd.GeoDataFrame(geometry=parts, crs="EPSG:4326")
    frame["geometry"] = frame.geometry.simplify(0.02, preserve_topology=True)
    frame = frame[~frame.geometry.is_empty]
    return json.loads(frame[["geometry"]].to_json())


def _crosses_antimeridian(polygon) -> bool:
    xs = polygon.exterior.coords.xy[0]
    return any(abs(a - b) > 180 for a, b in zip(xs, xs[1:], strict=False))


def _split_at_antimeridian(polygon) -> list:
    """Unwrap a ring that jumps across ±180°, then cut it back into the world."""

    def unwrap(ring) -> list[tuple[float, float]]:
        coords = list(ring.coords)
        out = [coords[0]]
        shift = 0.0
        for (x0, _), (x1, y1) in zip(coords, coords[1:], strict=False):
            if x1 - x0 > 180:
                shift -= 360
            elif x0 - x1 > 180:
                shift += 360
            out.append((x1 + shift, y1))
        return out

    unwrapped = Polygon(unwrap(polygon.exterior), [unwrap(r) for r in polygon.interiors])
    unwrapped = unwrapped.buffer(0)
    pieces = []
    for offset in (-360, 0, 360):
        window = box(-180 - offset, -90, 180 - offset, 90)
        piece = unwrapped.intersection(window)
        if not piece.is_empty:
            pieces.append(translate(piece, xoff=offset))
    out = []
    for piece in pieces:
        out.extend(getattr(piece, "geoms", [piece]))
    return [p for p in out if p.geom_type == "Polygon" and not p.is_empty]


def roads_geojson(load_roads: Callable[[], Any]) -> dict:
    """Roads as GeoJSON. Simplified hard: the raw shapefile is far too heavy to inline."""
    roads = load_roads().copy()
    roads["geometry"] = roads.geometry.simplify(0.02, preserve_topology=True)
    return json.loads(roads[["geometry"]].to_json())


def cities_points(load_cities: Callable[[], Any]) -> list[list]:
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

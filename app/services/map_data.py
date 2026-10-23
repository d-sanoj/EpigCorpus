"""Map data use cases: the historical layers as browser-ready GeoJSON and points.

Each function takes the (possibly cached) layer loader, so the UI's cached
loaders stay shared with the PNG export.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pandas as pd


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


def provinces_geojson(load_provinces: Callable[[], Any]) -> dict:
    """Provinces as GeoJSON, lightly simplified and pre-coloured for the browser."""
    provinces = load_provinces().reset_index(drop=True).copy()
    palette = _province_palette()
    provinces["_color"] = [palette[i % len(palette)] for i in range(len(provinces))]
    provinces["geometry"] = provinces.geometry.simplify(0.01, preserve_topology=True)
    return json.loads(provinces[["_color", "geometry"]].to_json())


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

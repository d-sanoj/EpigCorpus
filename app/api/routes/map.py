"""Historical map layers. Static per process, so each is serialised once and served with an ETag."""

from __future__ import annotations

import hashlib
import json
import threading

from fastapi import APIRouter, Request, Response

from app.api.deps import Deps

router = APIRouter(prefix="/v1/map/layers", tags=["map"])

_cache: dict[tuple[int, str], tuple[bytes, str]] = {}
_lock = threading.Lock()


def _layer(request: Request, deps, name: str, produce, media_type: str) -> Response:
    key = (id(deps.catalog), name)
    with _lock:
        cached = _cache.get(key)
    if cached is None:
        body = json.dumps(produce(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        etag = '"' + hashlib.sha256(body).hexdigest()[:16] + '"'
        cached = (body, etag)
        with _lock:
            _cache[key] = cached
    body, etag = cached
    headers = {"ETag": etag, "Cache-Control": "public, max-age=3600"}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type=media_type, headers=headers)


@router.get("/provinces", operation_id="getProvincesLayer")
def provinces(request: Request, deps: Deps):
    produce = deps.catalog.provinces_geojson
    return _layer(request, deps, "provinces", produce, "application/geo+json")


@router.get("/roads", operation_id="getRoadsLayer")
def roads(request: Request, deps: Deps):
    return _layer(request, deps, "roads", deps.catalog.roads_geojson, "application/geo+json")


@router.get("/province-labels", operation_id="getProvinceLabels")
def province_labels(request: Request, deps: Deps):
    produce = deps.catalog.province_labels
    return _layer(request, deps, "province-labels", produce, "application/json")


@router.get("/cities", operation_id="getCitiesLayer")
def cities(request: Request, deps: Deps):
    def points():
        return [
            {"latitude": lat, "longitude": lon, "label": label}
            for lat, lon, label in deps.catalog.cities_points()
        ]

    return _layer(request, deps, "cities", points, "application/json")

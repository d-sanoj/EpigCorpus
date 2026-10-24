"""Historical map layers, serialised once per corpus snapshot and served with an ETag."""

from __future__ import annotations

import hashlib
import json
import threading

from fastapi import APIRouter, Request, Response

from epigcorpus.api.deps import CatalogDep

router = APIRouter(prefix="/map/layers", tags=["map"])

# (corpus fingerprint, layer) -> (body, etag). Only province labels depend on
# the corpus, but keying every layer by it keeps the rule simple; a reload
# drops the previous snapshot's entries.
_cache: dict[tuple[str, str], tuple[bytes, str]] = {}
_lock = threading.Lock()


def _layer(request: Request, catalog, name: str, produce, media_type: str) -> Response:
    key = (catalog.fingerprint(), name)
    with _lock:
        cached = _cache.get(key)
    if cached is None:
        body = json.dumps(produce(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        etag = '"' + hashlib.sha256(body).hexdigest()[:16] + '"'
        cached = (body, etag)
        with _lock:
            for stale in [k for k in _cache if k[0] != key[0]]:
                del _cache[stale]
            _cache[key] = cached
    body, etag = cached
    headers = {"ETag": etag, "Cache-Control": "public, max-age=3600"}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type=media_type, headers=headers)


@router.get("/provinces", operation_id="getProvincesLayer")
def provinces(request: Request, catalog: CatalogDep):
    produce = catalog.provinces_geojson
    return _layer(request, catalog, "provinces", produce, "application/geo+json")


@router.get("/roads", operation_id="getRoadsLayer")
def roads(request: Request, catalog: CatalogDep):
    return _layer(request, catalog, "roads", catalog.roads_geojson, "application/geo+json")


@router.get("/land", operation_id="getLandLayer")
def land(request: Request, catalog: CatalogDep):
    return _layer(request, catalog, "land", catalog.land_geojson, "application/geo+json")


@router.get("/province-labels", operation_id="getProvinceLabels")
def province_labels(request: Request, catalog: CatalogDep):
    produce = catalog.province_labels
    return _layer(request, catalog, "province-labels", produce, "application/json")


@router.get("/cities", operation_id="getCitiesLayer")
def cities(request: Request, catalog: CatalogDep):
    def points():
        return [
            {"latitude": lat, "longitude": lon, "label": label}
            for lat, lon, label in catalog.cities_points()
        ]

    return _layer(request, catalog, "cities", points, "application/json")


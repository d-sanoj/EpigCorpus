"""Places: a query's results per findspot, and every known place.

Each comes paged as JSON, for lists, and whole as GeoJSON, for the map.
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from epigcorpus.api import serialize
from epigcorpus.api.deps import (
    SEARCH_MODES,
    CatalogDep,
    Cursor,
    FilterParams,
    OptionalQueryText,
    PlacesLimit,
    SearchIn,
    SearchInParam,
)
from epigcorpus.api.pagination import paginate

router = APIRouter(tags=["places"])

GEOJSON = "application/geo+json"


def _result_summary(result) -> dict:
    return {
        "inscriptions": len(result.rows),
        "places": len(result.places),
        "title": result.title,
        "subtitle": result.subtitle,
    }


def _known_summary(known) -> dict:
    return {"places": len(known), "inscriptions": int(known["count"].sum())}


@router.get("/places", operation_id="listResultPlaces")
def list_result_places(
    request: Request,
    response: Response,
    catalog: CatalogDep,
    filters: FilterParams,
    q: OptionalQueryText = None,
    search_in: SearchInParam = SearchIn.raw,
    limit: PlacesLimit = 50,
    cursor: Cursor = None,
):
    result = catalog.run_query(q or "", SEARCH_MODES[search_in], filters.selections())
    places = result.places
    page = paginate(request, response, len(places), limit, cursor, catalog.fingerprint())
    return {
        "data": serialize.places(places.iloc[page.start : page.stop], with_examples=True),
        "page": page.info(limit),
        "summary": _result_summary(result),
    }


@router.get("/places.geojson", operation_id="getResultPlacesGeoJson")
def result_places_geojson(
    catalog: CatalogDep,
    filters: FilterParams,
    q: OptionalQueryText = None,
    search_in: SearchInParam = SearchIn.raw,
):
    result = catalog.run_query(q or "", SEARCH_MODES[search_in], filters.selections())
    body = serialize.place_collection(result.places, True, _result_summary(result))
    return JSONResponse(body, media_type=GEOJSON)


@router.get("/places/known", operation_id="listKnownPlaces")
def list_known_places(
    request: Request,
    response: Response,
    catalog: CatalogDep,
    limit: PlacesLimit = 50,
    cursor: Cursor = None,
):
    known = catalog.known_places()
    page = paginate(request, response, len(known), limit, cursor, catalog.fingerprint())
    return {
        "data": serialize.places(known.iloc[page.start : page.stop], with_examples=False),
        "page": page.info(limit),
        "summary": _known_summary(known),
    }


@router.get("/places/known.geojson", operation_id="getKnownPlacesGeoJson")
def known_places_geojson(catalog: CatalogDep):
    known = catalog.known_places()
    body = serialize.place_collection(known, False, _known_summary(known))
    return JSONResponse(body, media_type=GEOJSON)

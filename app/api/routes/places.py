"""Places: a query's results per findspot, and every known place."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from app.api import serialize
from app.api.deps import (
    SEARCH_MODES,
    Cursor,
    Deps,
    FilterParams,
    PlacesLimit,
    OptionalQueryText,
    SearchIn,
    SearchInParam,
)
from app.api.pagination import paginate

router = APIRouter(prefix="/v1/places", tags=["places"])


@router.get("", operation_id="listResultPlaces")
def list_result_places(
    request: Request,
    response: Response,
    deps: Deps,
    filters: FilterParams,
    q: OptionalQueryText = None,
    search_in: SearchInParam = SearchIn.raw,
    limit: PlacesLimit = 50,
    cursor: Cursor = None,
):
    result = deps.catalog.run_query(q or "", SEARCH_MODES[search_in], filters.selections())
    places = result.places
    page = paginate(request, response, len(places), limit, cursor, deps.catalog.fingerprint())
    return {
        "data": serialize.places(places.iloc[page.start : page.stop], with_examples=True),
        "page": page.info(limit),
        "summary": {
            "inscriptions": len(result.rows),
            "places": len(places),
            "title": result.title,
            "subtitle": result.subtitle,
        },
    }


@router.get("/known", operation_id="listKnownPlaces")
def list_known_places(
    request: Request, response: Response, deps: Deps, limit: PlacesLimit = 50, cursor: Cursor = None
):
    known = deps.catalog.known_places()
    page = paginate(request, response, len(known), limit, cursor, deps.catalog.fingerprint())
    return {
        "data": serialize.places(known.iloc[page.start : page.stop], with_examples=False),
        "page": page.info(limit),
        "summary": {
            "places": len(known),
            "inscriptions": int(known["count"].sum()),
        },
    }

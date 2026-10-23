"""EDCS code tables and the filter values present in the corpus."""

from __future__ import annotations

from enum import StrEnum

from fastapi import APIRouter

from app.api.deps import Deps
from app.api.problems import Problem

router = APIRouter(tags=["lookups"])


class Table(StrEnum):
    materials = "materials"
    categories = "categories"
    provinces = "provinces"
    languages = "languages"
    places = "places"
    sources = "sources"


# Corpus column -> the API's filter name.
FACET_NAMES = {
    "material_en": "material",
    "province": "province",
    "place": "place",
    "category_en": "category",
    "language": "language",
}


def _lookup(deps) -> dict:
    lookup = deps.catalog.lookup()
    if lookup is None:
        raise Problem(
            503,
            "lookup-unavailable",
            "Lookup table not available",
            "app/data/reference/edcs_lookup.json does not exist; run a harvest.",
        )
    return lookup


@router.get("/v1/lookups", operation_id="getLookups")
def get_lookups(deps: Deps):
    return _lookup(deps)


@router.get("/v1/lookups/{table}", operation_id="getLookupTable")
def get_lookup_table(deps: Deps, table: Table):
    return {"table": table.value, "entries": _lookup(deps).get(table.value) or {}}


@router.get("/v1/facets", operation_id="getFacets")
def get_facets(deps: Deps):
    options = deps.catalog.filter_options()
    forms = deps.catalog.short_forms()
    return {
        "values": {FACET_NAMES[column]: values for column, values in options.items()},
        "short_forms": {FACET_NAMES[column]: mapping for column, mapping in forms.items()},
    }

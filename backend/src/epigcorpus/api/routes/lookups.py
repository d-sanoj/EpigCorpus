"""EDCS code tables and the filter values present in the corpus."""

from __future__ import annotations

from enum import StrEnum

from fastapi import APIRouter

from epigcorpus.api.deps import CatalogDep
from epigcorpus.api.problems import Problem

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


def _lookup(catalog) -> dict:
    lookup = catalog.lookup()
    if lookup is None:
        raise Problem(
            503,
            "lookup-unavailable",
            "Lookup table not available",
            "data/reference/edcs_lookup.json does not exist; run a harvest.",
        )
    return lookup


@router.get("/lookups", operation_id="getLookups")
def get_lookups(catalog: CatalogDep):
    return _lookup(catalog)


@router.get("/lookups/{table}", operation_id="getLookupTable")
def get_lookup_table(catalog: CatalogDep, table: Table):
    return {"table": table.value, "entries": _lookup(catalog).get(table.value) or {}}


@router.get("/facets", operation_id="getFacets")
def get_facets(catalog: CatalogDep):
    options = catalog.filter_options()
    forms = catalog.short_forms()
    return {
        "values": {FACET_NAMES[column]: values for column, values in options.items()},
        "short_forms": {FACET_NAMES[column]: mapping for column, mapping in forms.items()},
    }

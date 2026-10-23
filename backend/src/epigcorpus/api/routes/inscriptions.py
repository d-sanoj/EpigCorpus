"""Inscriptions: list, search, get one, and the cleaned variants of one."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Path, Request, Response

from epigcorpus.api import serialize
from epigcorpus.api.deps import (
    SEARCH_MODES,
    Cursor,
    Deps,
    FilterParams,
    Limit,
    QueryText,
    SearchIn,
    SearchInParam,
)
from epigcorpus.api.pagination import paginate
from epigcorpus.api.problems import not_found

router = APIRouter(prefix="/v1/inscriptions", tags=["inscriptions"])

RecordId = Annotated[str, Path(pattern=r"^EDCS-[0-9]{8}-[0-9]+$")]


class VariantName(StrEnum):
    raw = "raw"
    conservative = "conservative"
    interpretive = "interpretive"


VARIANT_COLUMNS = {
    "raw": "inscription_text",
    "conservative": "inscription_text_conservative",
    "interpretive": "inscription_text_interpretive",
}


def _page(request, response, deps, rows, limit, cursor) -> dict:
    page = paginate(request, response, len(rows), limit, cursor, deps.catalog.fingerprint())
    return {
        "data": serialize.summaries(rows.iloc[page.start : page.stop]),
        "page": page.info(limit),
    }


@router.get("", operation_id="listInscriptions")
def list_inscriptions(
    request: Request,
    response: Response,
    deps: Deps,
    filters: FilterParams,
    limit: Limit = 50,
    cursor: Cursor = None,
):
    rows = deps.catalog.rows("", SEARCH_MODES[SearchIn.raw], filters.selections())
    return _page(request, response, deps, rows, limit, cursor)


@router.get("/search", operation_id="searchInscriptions")
def search_inscriptions(
    request: Request,
    response: Response,
    deps: Deps,
    q: QueryText,
    filters: FilterParams,
    search_in: SearchInParam = SearchIn.raw,
    limit: Limit = 50,
    cursor: Cursor = None,
):
    rows = deps.catalog.rows(q, SEARCH_MODES[search_in], filters.selections())
    return _page(request, response, deps, rows, limit, cursor)


def _row(deps, record_id: str):
    row = deps.catalog.inscription(record_id)
    if row is None:
        raise not_found(f"No inscription {record_id} in the corpus.")
    return row


@router.get("/{record_id}", operation_id="getInscription")
def get_inscription(deps: Deps, record_id: RecordId):
    return serialize.record(_row(deps, record_id), serialize.FULL_FIELDS)


@router.get("/{record_id}/variants", operation_id="getInscriptionVariants", tags=["variants"])
def get_variants(deps: Deps, record_id: RecordId):
    row = _row(deps, record_id)
    out = {"record_id": record_id}
    for name, column in VARIANT_COLUMNS.items():
        out[name] = serialize.value(column, row.get(column))
    out["is_unreadable"] = serialize.value("is_unreadable", row.get("is_unreadable"))
    out["is_forged"] = serialize.value("is_forged", row.get("is_forged"))
    return out


@router.get(
    "/{record_id}/variants/{variant}", operation_id="getInscriptionVariant", tags=["variants"]
)
def get_variant(deps: Deps, record_id: RecordId, variant: VariantName):
    row = _row(deps, record_id)
    column = VARIANT_COLUMNS[variant.value]
    text = serialize.value(column, row.get(column))
    return {"record_id": record_id, "variant": variant.value, "text": text}

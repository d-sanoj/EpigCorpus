"""Downloads: the corpus files, a query's results as TSV, and the map as PNG."""

from __future__ import annotations

import gzip
from collections.abc import Iterator
from pathlib import Path

from fastapi import APIRouter, Request, Response
from fastapi.responses import FileResponse, StreamingResponse

from app.api.deps import (
    SEARCH_MODES,
    Deps,
    FilterParams,
    OptionalQueryText,
    SearchIn,
    SearchInParam,
)
from app.api.problems import Problem, invalid
from app.services import search

router = APIRouter(prefix="/v1/exports", tags=["exports"])

JSONL = "application/x-ndjson"
TSV = "text/tab-separated-values"

# Export name -> (download file name, media type)
FILES = {
    "corpus.jsonl": ("edcs_inscriptions.jsonl", JSONL),
    "corpus.tsv": ("edcs_inscriptions.tsv", TSV),
    "cleaned.jsonl": ("edcs_inscriptions_cleaned.jsonl", JSONL),
}


def _attachment(filename: str) -> dict[str, str]:
    return {"Content-Disposition": f'attachment; filename="{filename}"'}


def _accepts_gzip(request: Request) -> bool:
    accepted = request.headers.get("accept-encoding", "")
    return any(part.split(";")[0].strip() == "gzip" for part in accepted.split(","))


def _gunzip(path: Path) -> Iterator[bytes]:
    with gzip.open(path, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            yield chunk


def _serve_file(request: Request, deps, name: str) -> Response:
    filename, media_type = FILES[name]
    path = deps.exports.export_file(name)
    if path is None:
        raise Problem(
            503,
            "export-unavailable",
            "Export not available",
            f"There is no {filename} or {filename}.gz on the server; run the pipeline first.",
        )
    headers = _attachment(filename)
    if path.suffix != ".gz":
        return FileResponse(path, media_type=media_type, headers=headers)
    if _accepts_gzip(request):
        # The stored snapshot is already gzip: send it as is.
        headers["Content-Encoding"] = "gzip"
        headers["Vary"] = "Accept-Encoding"
        return FileResponse(path, media_type=media_type, headers=headers)
    return StreamingResponse(_gunzip(path), media_type=media_type, headers=headers)


@router.get("/corpus.jsonl", operation_id="exportCorpusJsonl")
def corpus_jsonl(request: Request, deps: Deps):
    return _serve_file(request, deps, "corpus.jsonl")


@router.get("/corpus.tsv", operation_id="exportCorpusTsv")
def corpus_tsv(request: Request, deps: Deps):
    return _serve_file(request, deps, "corpus.tsv")


@router.get("/cleaned.jsonl", operation_id="exportCleanedJsonl")
def cleaned_jsonl(request: Request, deps: Deps):
    return _serve_file(request, deps, "cleaned.jsonl")


@router.get("/search.tsv", operation_id="exportSearchTsv")
def search_tsv(
    deps: Deps,
    filters: FilterParams,
    q: OptionalQueryText = None,
    search_in: SearchInParam = SearchIn.raw,
):
    if not q and not filters.any():
        raise invalid("q", "give a keyword, a filter, or both")
    rows = deps.catalog.rows(q or "", SEARCH_MODES[search_in], filters.selections())
    table = deps.catalog.results_table(rows)
    return Response(
        search.results_tsv(table),
        media_type=TSV,
        headers=_attachment("edcs_search_results.tsv"),
    )


@router.get("/map.png", operation_id="exportMapPng")
def map_png(
    deps: Deps,
    filters: FilterParams,
    q: OptionalQueryText = None,
    search_in: SearchInParam = SearchIn.raw,
):
    selections = filters.selections()
    term = q or ""
    png = deps.catalog.export_png(term, SEARCH_MODES[search_in], selections)
    return Response(png.content, media_type="image/png", headers=_attachment(png.filename))

"""Health and the contract itself."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse

from epigcorpus.api.deps import CatalogDep, Deps

router = APIRouter(tags=["meta"])


@router.get("/health", operation_id="getHealth")
def health(catalog: CatalogDep):
    """`ready` turns true once the corpus is loaded; until then reads wait for it."""
    return {"status": "ok", "ready": catalog.is_loaded()}


@router.get("/openapi.yaml", operation_id="getOpenApi", include_in_schema=False)
def openapi_document(deps: Deps):
    return FileResponse(deps.spec_path, media_type="application/yaml")

"""The API's composition root: real adapters into services, services into the app.

Run it from backend/ with ``uv run uvicorn epigcorpus.api.server:app`` (see
backend/README.md). The corpus is loaded in a background thread at startup;
requests that need it wait until it is in memory, and /api/v1/health reports
when it is.
"""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from functools import partial

from epigcorpus.adapters import corpus_files, edcs_client, harvest_store, map_layers
from epigcorpus.adapters.console import log
from epigcorpus.api.app import create_app
from epigcorpus.api.deps import ApiDeps
from epigcorpus.config.paths import API_SPEC, UI_STATIC_DIR
from epigcorpus.services.catalog import Catalog, CatalogSlot
from epigcorpus.services.cleaning import run_cleaning
from epigcorpus.services.pipeline import PipelineRuns, PipelineSteps
from epigcorpus.services.scrape import run_harvest


def build_deps() -> ApiDeps:
    catalogs = CatalogSlot(lambda: Catalog(corpus_files, map_layers))
    steps = PipelineSteps(
        harvest=partial(run_harvest, client=edcs_client, store=harvest_store, log=log),
        clean=lambda: len(run_cleaning(corpus_files)),
        reload=catalogs.reload,
    )
    return ApiDeps(
        catalogs=catalogs,
        exports=corpus_files,
        runs=PipelineRuns(steps),
        spec_path=API_SPEC,
        static_dir=UI_STATIC_DIR if UI_STATIC_DIR.is_dir() else None,
    )


@asynccontextmanager
async def _warm_on_startup(app):
    threading.Thread(target=app.state.deps.catalogs.current.warm, daemon=True).start()
    yield


app = create_app(build_deps(), lifespan=_warm_on_startup)

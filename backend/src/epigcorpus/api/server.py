"""The API's composition root: real adapters into services, services into the app.

Run it with ``uvicorn epigcorpus.api.server:app`` from backend/ (see backend/README.md).
The corpus is loaded in a background thread at startup; requests that need it
wait until it is in memory, and /v1/health reports when it is.
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
from epigcorpus.services.catalog import Catalog
from epigcorpus.services.jobs import ScrapeJobs
from epigcorpus.services.scrape import run_harvest


def build_deps() -> ApiDeps:
    harvest = partial(run_harvest, client=edcs_client, store=harvest_store, log=log)
    return ApiDeps(
        catalog=Catalog(corpus_files, map_layers),
        exports=corpus_files,
        jobs=ScrapeJobs(harvest),
        spec_path=API_SPEC,
        static_dir=UI_STATIC_DIR,
    )


@asynccontextmanager
async def _warm_on_startup(app):
    threading.Thread(target=app.state.deps.catalog.warm, daemon=True).start()
    yield


app = create_app(build_deps(), lifespan=_warm_on_startup)

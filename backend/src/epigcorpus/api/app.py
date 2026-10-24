"""Build the FastAPI application from already-wired dependencies.

The contract is the hand-written backend/src/epigcorpus/api/openapi.yaml, served at
/api/v1/openapi.yaml. FastAPI's generated schema is switched off so there is
only one description of the API.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from epigcorpus.api import problems
from epigcorpus.api.deps import ApiDeps
from epigcorpus.api.routes import (
    exports,
    inscriptions,
    lookups,
    map,
    meta,
    pipeline_runs,
    places,
)
from epigcorpus.api.spa import SinglePageApp

API_PREFIX = "/api/v1"


def create_app(deps: ApiDeps, lifespan=None) -> FastAPI:
    app = FastAPI(
        title="EpigCorpus API",
        version="1.0.0",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.deps = deps
    problems.install(app)
    # The map layers and place collections are JSON of up to a few MB.
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    for module in (meta, inscriptions, places, map, exports, lookups, pipeline_runs):
        app.include_router(module.router, prefix=API_PREFIX)
    if deps.static_dir is not None:
        # The built web UI (frontend/dist), a client of the routes above.
        # Mounted last so every API route wins.
        app.mount("/", SinglePageApp(directory=deps.static_dir), name="ui")
    return app

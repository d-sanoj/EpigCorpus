"""Build the FastAPI application from already-wired dependencies.

The contract is the hand-written backend/src/epigcorpus/api/openapi.yaml, served at
/v1/openapi.yaml. FastAPI's generated schema is switched off so there is
only one description of the API.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from epigcorpus.api import problems
from epigcorpus.api.deps import ApiDeps
from epigcorpus.api.routes import exports, inscriptions, jobs, lookups, map, meta, places


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
    for module in (meta, inscriptions, places, map, exports, lookups, jobs):
        app.include_router(module.router)
    if deps.static_dir is not None:
        # The web UI: plain files, a client of the routes above. Mounted last so
        # every /v1 route wins.
        app.mount("/", StaticFiles(directory=deps.static_dir, html=True), name="ui")
    return app

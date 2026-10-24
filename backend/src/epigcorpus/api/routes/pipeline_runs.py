"""Pipeline runs: harvest, clean and reload in the background; poll; cancel."""

from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from epigcorpus.api.deps import Deps
from epigcorpus.api.problems import Problem, not_found
from epigcorpus.config.settings import DEFAULT_WORKERS
from epigcorpus.services.pipeline import ACTIVE, RunConflict, RunFinished

PATH = "/pipeline-runs"
router = APIRouter(prefix=PATH, tags=["pipeline-runs"])


class PipelineRunRequest(BaseModel):
    """Which stages to run; the harvest options keep the old scraper's defaults."""

    model_config = ConfigDict(extra="forbid")

    harvest: bool = True
    clean: bool = True
    limit: int | None = Field(default=None, ge=1)
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=32)
    restart: bool = False
    refresh_index: bool = False

    @model_validator(mode="after")
    def _some_stage(self):
        if not (self.harvest or self.clean):
            raise ValueError("at least one of harvest and clean must be true")
        return self


def _url(run_id: str) -> str:
    return f"/api/v1{PATH}/{run_id}"


def _view(run: dict) -> dict:
    return {
        **run,
        "links": {
            "self": _url(run["id"]),
            "cancel": f"{_url(run['id'])}/cancel" if run["status"] in ACTIVE else None,
        },
    }


@router.post("", operation_id="startPipelineRun", status_code=202)
def start_pipeline_run(response: Response, deps: Deps, body: PipelineRunRequest | None = None):
    request = (body or PipelineRunRequest()).model_dump()
    try:
        run = deps.runs.start(request)
    except RunConflict as conflict:
        raise Problem(
            409,
            "pipeline-run-active",
            "A pipeline run is already in progress",
            f"Run {conflict.running.id} is {conflict.running.status}; "
            "cancel it or wait for it to finish.",
            running_run=_url(conflict.running.id),
        ) from None
    response.headers["Location"] = _url(run["id"])
    return _view(run)


@router.get("", operation_id="listPipelineRuns")
def list_pipeline_runs(deps: Deps):
    return {"data": [_view(run) for run in deps.runs.list()]}


@router.get("/{run_id}", operation_id="getPipelineRun")
def get_pipeline_run(deps: Deps, run_id: str):
    run = deps.runs.get(run_id)
    if run is None:
        raise not_found(f"No pipeline run {run_id}.")
    return _view(run)


@router.post("/{run_id}/cancel", operation_id="cancelPipelineRun", status_code=202)
def cancel_pipeline_run(deps: Deps, run_id: str):
    try:
        run = deps.runs.cancel(run_id)
    except RunFinished as finished:
        raise Problem(
            409,
            "pipeline-run-finished",
            "The run has already finished",
            f"Run {finished.run.id} ended with status {finished.run.status}.",
        ) from None
    if run is None:
        raise not_found(f"No pipeline run {run_id}.")
    return _view(run)

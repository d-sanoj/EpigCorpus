"""Scrape jobs: start a harvest in the background, poll it, cancel it."""

from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel, ConfigDict, Field

from app.api.deps import Deps
from app.api.problems import Problem, not_found
from app.config.settings import DEFAULT_WORKERS
from app.services.jobs import ACTIVE, JobConflict, JobFinished

router = APIRouter(prefix="/v1/scrape-jobs", tags=["scrape-jobs"])


class ScrapeJobRequest(BaseModel):
    """Mirrors the scraper's command line, with the same defaults."""

    model_config = ConfigDict(extra="forbid")

    limit: int | None = Field(default=None, ge=1)
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=32)
    restart: bool = False
    refresh_index: bool = False


def _url(job_id: str) -> str:
    return f"/v1/scrape-jobs/{job_id}"


def _view(job: dict) -> dict:
    return {
        **job,
        "links": {
            "self": _url(job["id"]),
            "cancel": f"{_url(job['id'])}/cancel" if job["status"] in ACTIVE else None,
        },
    }


@router.post("", operation_id="startScrapeJob", status_code=202)
def start_scrape_job(response: Response, deps: Deps, body: ScrapeJobRequest | None = None):
    request = (body or ScrapeJobRequest()).model_dump()
    try:
        job = deps.jobs.start(request)
    except JobConflict as conflict:
        raise Problem(
            409,
            "scrape-job-running",
            "A harvest is already running",
            f"Job {conflict.running.id} is {conflict.running.status}; "
            "cancel it or wait for it to finish.",
            running_job=_url(conflict.running.id),
        ) from None
    response.headers["Location"] = _url(job["id"])
    return _view(job)


@router.get("/{job_id}", operation_id="getScrapeJob")
def get_scrape_job(deps: Deps, job_id: str):
    job = deps.jobs.get(job_id)
    if job is None:
        raise not_found(f"No scrape job {job_id}.")
    return _view(job)


@router.post("/{job_id}/cancel", operation_id="cancelScrapeJob", status_code=202)
def cancel_scrape_job(deps: Deps, job_id: str):
    try:
        job = deps.jobs.cancel(job_id)
    except JobFinished as finished:
        raise Problem(
            409,
            "scrape-job-finished",
            "The job has already finished",
            f"Job {finished.job.id} ended with status {finished.job.status}.",
        ) from None
    if job is None:
        raise not_found(f"No scrape job {job_id}.")
    return _view(job)

"""Scrape jobs: run harvests in the background, one at a time, and report on them.

The registry lives in memory, so a server restart forgets finished jobs. A
harvest that was running when the server stopped resumes from its checkpoint
the next time a job starts, as it would on the command line.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from app.services.scrape import INTERRUPTED, HarvestError

# run(limit, workers, restart, refresh_index, should_stop, on_progress) -> outcome.
# The composition root binds run_harvest to the real client, store and log.
HarvestRunner = Callable[..., str]

QUEUED, RUNNING, CANCELLING = "queued", "running", "cancelling"
SUCCEEDED, CANCELLED, FAILED = "succeeded", "cancelled", "failed"
ACTIVE = {QUEUED, RUNNING, CANCELLING}


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class ScrapeJob:
    id: str
    request: dict
    status: str = QUEUED
    created_at: str = field(default_factory=_now)
    started_at: str | None = None
    finished_at: str | None = None
    progress: dict | None = None
    outcome: str | None = None
    error: dict | None = None

    def snapshot(self) -> dict:
        return asdict(self)


class JobConflict(Exception):
    """A harvest is already queued or running."""

    def __init__(self, running: ScrapeJob):
        super().__init__(running.id)
        self.running = running


class JobFinished(Exception):
    """The job can no longer be cancelled."""

    def __init__(self, job: ScrapeJob):
        super().__init__(job.id)
        self.job = job


class ScrapeJobs:
    def __init__(self, run: HarvestRunner):
        self._run = run
        self._jobs: dict[str, ScrapeJob] = {}
        self._stop: dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._threads: dict[str, threading.Thread] = {}

    def start(self, request: dict) -> dict:
        with self._lock:
            for job in self._jobs.values():
                if job.status in ACTIVE:
                    raise JobConflict(job)
            job = ScrapeJob(id=str(uuid.uuid4()), request=request)
            self._jobs[job.id] = job
            self._stop[job.id] = threading.Event()
            snapshot = job.snapshot()
        thread = threading.Thread(target=self._execute, args=(job.id,), daemon=True)
        self._threads[job.id] = thread
        thread.start()
        return snapshot

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.snapshot() if job else None

    def cancel(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if job.status not in ACTIVE:
                raise JobFinished(job)
            job.status = CANCELLING
            self._stop[job_id].set()
            return job.snapshot()

    def wait(self, job_id: str, timeout: float | None = None) -> None:
        """Block until the job's thread ends (used by tests and shutdown)."""
        thread = self._threads.get(job_id)
        if thread is not None:
            thread.join(timeout)

    def _update(self, job_id: str, **changes) -> None:
        with self._lock:
            job = self._jobs[job_id]
            for key, value in changes.items():
                setattr(job, key, value)

    def _execute(self, job_id: str) -> None:
        stop = self._stop[job_id]
        with self._lock:
            job = self._jobs[job_id]
            if not stop.is_set():
                job.status = RUNNING
            job.started_at = _now()
            request = dict(job.request)

        def on_progress(update: dict) -> None:
            self._update(job_id, progress=update)

        try:
            outcome = self._run(
                request["limit"],
                request["workers"],
                request["restart"],
                request["refresh_index"],
                should_stop=stop.is_set,
                on_progress=on_progress,
            )
        except HarvestError as exc:
            self._update(job_id, status=FAILED, finished_at=_now(), error=_problem(
                "harvest-unavailable", "The harvest could not start", str(exc)
            ))
            return
        except Exception as exc:  # noqa: BLE001 - recorded on the job, never lost
            self._update(job_id, status=FAILED, finished_at=_now(), error=_problem(
                "harvest-failed", "The harvest failed", f"{type(exc).__name__}: {exc}"
            ))
            return

        if outcome == INTERRUPTED or stop.is_set():
            self._update(job_id, status=CANCELLED, finished_at=_now(), outcome=None)
        else:
            self._update(job_id, status=SUCCEEDED, finished_at=_now(), outcome=outcome)


def _problem(slug: str, title: str, detail: str) -> dict:
    return {
        "type": f"https://epigcorpus.example/problems/{slug}",
        "title": title,
        "status": 500,
        "detail": detail,
    }

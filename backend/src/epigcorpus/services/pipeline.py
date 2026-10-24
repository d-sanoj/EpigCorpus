"""Pipeline runs: harvest, clean and reload the catalog in the background.

A run is the old command line's "scrape, then clean" plus a step it never had:
swapping the freshly cleaned corpus into the running server. Its stages are

- ``harvest``: fetch monuments from the EDCS into the raw corpus (resumable);
- ``clean``: derive the conservative and interpretive variants and save the
  cleaned corpus;
- ``reload``: load the new cleaned corpus into a fresh catalog and swap it in,
  so every later request reads the new data. Cursors issued before the swap
  stop being valid, as they are bound to the corpus fingerprint.

Either of the first two can be left out; ``reload`` follows ``clean``.

One run at a time: there is one checkpoint and one corpus. The registry lives
in memory, so a server restart forgets finished runs; a harvest that was
running resumes from its checkpoint when the next run starts.

Cancelling stops the harvest at its next chunk (the same path as Ctrl+C on the
old command line) and skips the stages that have not started. Cleaning cannot
be interrupted; once it has written the cleaned corpus the reload still runs,
so the server never serves an older corpus than the one on disk.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from epigcorpus.services.isolation import ChildProcessDied, IsolatedWorkFailed
from epigcorpus.services.scrape import INTERRUPTED, HarvestError

# harvest(limit, workers, restart, refresh_index, *, should_stop, on_progress) -> outcome.
# The composition root binds run_harvest to the real client, store and log.
HarvestRunner = Callable[..., str]

QUEUED, RUNNING, CANCELLING = "queued", "running", "cancelling"
SUCCEEDED, CANCELLED, FAILED = "succeeded", "cancelled", "failed"
ACTIVE = {QUEUED, RUNNING, CANCELLING}

PENDING, SKIPPED = "pending", "skipped"

HARVEST, CLEAN, RELOAD = "harvest", "clean", "reload"
STAGES = (HARVEST, CLEAN, RELOAD)

# How many finished runs the registry keeps for GET /pipeline-runs.
HISTORY = 20


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class PipelineSteps:
    """The work behind each stage, wired to real adapters by the composition root."""

    harvest: HarvestRunner
    # Cleans the newest raw corpus and saves it; returns the rows written.
    clean: Callable[[], int]
    # Loads the cleaned corpus into a new catalog and swaps it in; returns
    # the new corpus fingerprint.
    reload: Callable[[], str]


@dataclass
class Stage:
    name: str
    status: str = PENDING
    started_at: str | None = None
    finished_at: str | None = None
    # harvest: its outcome (complete, partial, ...); clean: rows written;
    # reload: the new corpus fingerprint.
    result: str | int | None = None


@dataclass
class PipelineRun:
    id: str
    request: dict
    stages: list[Stage]
    status: str = QUEUED
    created_at: str = field(default_factory=_now)
    started_at: str | None = None
    finished_at: str | None = None
    progress: dict | None = None
    error: dict | None = None

    def snapshot(self) -> dict:
        return asdict(self)

    def stage(self, name: str) -> Stage | None:
        return next((stage for stage in self.stages if stage.name == name), None)


def planned_stages(request: dict) -> list[str]:
    stages = []
    if request.get("harvest", True):
        stages.append(HARVEST)
    if request.get("clean", True):
        stages.extend((CLEAN, RELOAD))
    return stages


class RunConflict(Exception):
    """A run is already queued or running."""

    def __init__(self, running: PipelineRun):
        super().__init__(running.id)
        self.running = running


class RunFinished(Exception):
    """The run can no longer be cancelled."""

    def __init__(self, run: PipelineRun):
        super().__init__(run.id)
        self.run = run


class PipelineRuns:
    def __init__(self, steps: PipelineSteps):
        self._steps = steps
        self._runs: dict[str, PipelineRun] = {}
        self._stop: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def start(self, request: dict) -> dict:
        stages = planned_stages(request)
        if not stages:
            raise ValueError("A run needs at least one of harvest and clean.")
        with self._lock:
            for run in self._runs.values():
                if run.status in ACTIVE:
                    raise RunConflict(run)
            run = PipelineRun(
                id=str(uuid.uuid4()), request=request, stages=[Stage(name) for name in stages]
            )
            self._runs[run.id] = run
            self._stop[run.id] = threading.Event()
            self._forget_old_runs()
            snapshot = run.snapshot()
        thread = threading.Thread(target=self._execute, args=(run.id,), daemon=True)
        self._threads[run.id] = thread
        thread.start()
        return snapshot

    def get(self, run_id: str) -> dict | None:
        with self._lock:
            run = self._runs.get(run_id)
            return run.snapshot() if run else None

    def list(self) -> list[dict]:
        """Every run the registry still holds, newest first."""
        with self._lock:
            return [run.snapshot() for run in reversed(self._runs.values())]

    def cancel(self, run_id: str) -> dict | None:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                return None
            if run.status not in ACTIVE:
                raise RunFinished(run)
            run.status = CANCELLING
            self._stop[run_id].set()
            return run.snapshot()

    def wait(self, run_id: str, timeout: float | None = None) -> None:
        """Block until the run's thread ends (used by tests and shutdown)."""
        thread = self._threads.get(run_id)
        if thread is not None:
            thread.join(timeout)

    # ─── internals ────────────────────────────────────────────────────────────

    def _forget_old_runs(self) -> None:
        finished = [run_id for run_id, run in self._runs.items() if run.status not in ACTIVE]
        for run_id in finished[: max(0, len(self._runs) - HISTORY)]:
            del self._runs[run_id]
            self._stop.pop(run_id, None)
            self._threads.pop(run_id, None)

    def _set_stage(self, run_id: str, name: str, **changes) -> None:
        with self._lock:
            stage = self._runs[run_id].stage(name)
            for key, value in changes.items():
                setattr(stage, key, value)

    def _finish(self, run_id: str, status: str, error: dict | None = None) -> None:
        with self._lock:
            run = self._runs[run_id]
            for stage in run.stages:
                if stage.status == PENDING:
                    stage.status = SKIPPED
            run.status = status
            run.error = error
            run.finished_at = _now()

    def _execute(self, run_id: str) -> None:
        stop = self._stop[run_id]
        with self._lock:
            run = self._runs[run_id]
            if not stop.is_set():
                run.status = RUNNING
            run.started_at = _now()
            request = dict(run.request)
            stages = [stage.name for stage in run.stages]

        def on_progress(update: dict) -> None:
            with self._lock:
                self._runs[run_id].progress = update

        cleaned = False
        for name in stages:
            # Once the cleaned corpus is on disk, the reload must follow it.
            if stop.is_set() and not (name == RELOAD and cleaned):
                self._finish(run_id, CANCELLED)
                return
            self._set_stage(run_id, name, status=RUNNING, started_at=_now())
            try:
                if name == HARVEST:
                    result = self._steps.harvest(
                        request["limit"],
                        request["workers"],
                        request["restart"],
                        request["refresh_index"],
                        should_stop=stop.is_set,
                        on_progress=on_progress,
                    )
                elif name == CLEAN:
                    result = self._steps.clean()
                    cleaned = True
                else:
                    result = self._steps.reload()
            except HarvestError as exc:
                self._fail(run_id, name, "harvest-unavailable", "The harvest could not start", exc)
                return
            except Exception as exc:  # noqa: BLE001 - recorded on the run, never lost
                self._fail(run_id, name, f"{name}-failed", f"The {name} stage failed", exc)
                return

            if name == HARVEST and result == INTERRUPTED:
                self._set_stage(run_id, name, status=CANCELLED, finished_at=_now())
                self._finish(run_id, CANCELLED)
                return
            self._set_stage(run_id, name, status=SUCCEEDED, finished_at=_now(), result=result)

        # Every stage ran to its end, so the run succeeded even if a cancel
        # arrived too late to stop anything.
        self._finish(run_id, SUCCEEDED)

    def _fail(self, run_id: str, stage: str, slug: str, title: str, exc: Exception) -> None:
        explained = (HarvestError, ChildProcessDied, IsolatedWorkFailed)
        detail = str(exc) if isinstance(exc, explained) else f"{type(exc).__name__}: {exc}"
        self._set_stage(run_id, stage, status=FAILED, finished_at=_now())
        self._finish(run_id, FAILED, error={
            "type": f"https://epigcorpus.example/problems/{slug}",
            "title": title,
            "status": 500,
            "detail": detail,
        })

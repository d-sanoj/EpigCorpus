/* Run the pipeline (harvest, clean, reload) and follow it.
 *
 * A run is started with POST /pipeline-runs and polled until it ends. When a
 * run's reload stage succeeds the server answers from the new corpus, and
 * the app reloads what it shows.
 */
import { useEffect, useRef, useState, type FormEvent } from "react";
import { getJson, postJson, type PipelineRun, type PipelineRunRequest } from "../../api/client";
import { formatInteger } from "../../lib/format";

const POLL_MS = 2000;
const ACTIVE = new Set(["queued", "running", "cancelling"]);

const STATUS_STYLE: Record<string, string> = {
  pending: "text-slate-500",
  skipped: "text-slate-500",
  queued: "text-slate-300",
  running: "text-sky-300",
  cancelling: "text-amber-300",
  succeeded: "text-teal-300",
  cancelled: "text-amber-300",
  failed: "text-red-400",
};

interface Props {
  onCorpusReloaded: () => void;
}

export function PipelinePanel({ onCorpusReloaded }: Props) {
  const [request, setRequest] = useState<Required<Omit<PipelineRunRequest, "limit">> & {
    limit: string;
  }>({ harvest: true, clean: true, limit: "", workers: 16, restart: false, refresh_index: false });
  const [run, setRun] = useState<PipelineRun | null>(null);
  const [error, setError] = useState<string | null>(null);
  const reported = useRef<string | null>(null);

  // The latest run, so a page opened mid-run shows it.
  useEffect(() => {
    getJson<{ data: PipelineRun[] }>("/pipeline-runs").then(
      (body) => {
        const latest = body.data[0] ?? null;
        if (latest) reported.current = latest.id; // its reload, if any, is already served
        setRun(latest);
      },
      () => {},
    );
  }, []);

  const active = run !== null && ACTIVE.has(run.status);
  useEffect(() => {
    if (!run || !ACTIVE.has(run.status)) return;
    const timer = window.setTimeout(() => {
      getJson<PipelineRun>(`/pipeline-runs/${run.id}`).then(setRun, (reason: Error) =>
        setError(reason.message),
      );
    }, POLL_MS);
    return () => window.clearTimeout(timer);
  }, [run]);

  useEffect(() => {
    if (!run || reported.current === run.id) return;
    const reload = run.stages.find((stage) => stage.name === "reload");
    if (reload?.status === "succeeded") {
      reported.current = run.id;
      onCorpusReloaded();
    }
  }, [run, onCorpusReloaded]);

  function start(event: FormEvent) {
    event.preventDefault();
    if (
      request.restart &&
      !window.confirm("Restart deletes the harvested corpus and starts over. Continue?")
    )
      return;
    setError(null);
    const body: PipelineRunRequest = {
      ...request,
      limit: request.limit ? Number(request.limit) : null,
    };
    postJson<PipelineRun>("/pipeline-runs", body).then(setRun, (reason: Error) =>
      setError(reason.message),
    );
  }

  function cancel() {
    if (!run) return;
    postJson<PipelineRun>(`/pipeline-runs/${run.id}/cancel`).then(setRun, (reason: Error) =>
      setError(reason.message),
    );
  }

  const set = <K extends keyof typeof request>(key: K, value: (typeof request)[K]) =>
    setRequest((current) => ({ ...current, [key]: value }));
  const progress = run?.progress;

  return (
    <section aria-labelledby="pipeline-title" className="flex flex-col gap-3">
      <h2 id="pipeline-title" className="text-xs tracking-wide text-slate-500 uppercase">
        Pipeline
      </h2>
      <form onSubmit={start} className="flex flex-col gap-2 text-sm">
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            className="accent-orange-500"
            checked={request.harvest}
            onChange={(e) => set("harvest", e.target.checked)}
          />
          Harvest from the EDCS
        </label>
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            className="accent-orange-500"
            checked={request.clean}
            onChange={(e) => set("clean", e.target.checked)}
          />
          Clean and reload the corpus
        </label>
        {request.harvest && (
          <div className="ml-6 flex flex-col gap-2">
            <div className="grid grid-cols-2 gap-2">
              <label className="flex flex-col gap-1 text-xs text-slate-400">
                Limit
                <input
                  type="number"
                  min={1}
                  placeholder="all"
                  className="field min-h-8 py-1"
                  value={request.limit}
                  onChange={(e) => set("limit", e.target.value)}
                />
              </label>
              <label className="flex flex-col gap-1 text-xs text-slate-400">
                Workers
                <input
                  type="number"
                  min={1}
                  max={32}
                  className="field min-h-8 py-1"
                  value={request.workers}
                  onChange={(e) => set("workers", Number(e.target.value))}
                />
              </label>
            </div>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                className="accent-orange-500"
                checked={request.refresh_index}
                onChange={(e) => set("refresh_index", e.target.checked)}
              />
              Refresh the corpus index
            </label>
            <label className="flex items-center gap-2 text-xs text-red-300">
              <input
                type="checkbox"
                className="accent-red-500"
                checked={request.restart}
                onChange={(e) => set("restart", e.target.checked)}
              />
              Restart from scratch (deletes the harvest)
            </label>
          </div>
        )}
        <button
          type="submit"
          className="button"
          disabled={active || !(request.harvest || request.clean)}
        >
          Run pipeline
        </button>
      </form>

      {error && <p className="text-sm text-red-400">{error}</p>}

      {run && (
        <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3 text-sm" data-run={run.id}>
          <div className="flex items-center justify-between gap-2">
            <span>
              Run <span className={STATUS_STYLE[run.status]}>{run.status}</span>
            </span>
            {run.links.cancel && run.status !== "cancelling" && (
              <button type="button" className="button w-auto min-h-7 px-3 text-xs" onClick={cancel}>
                Cancel
              </button>
            )}
          </div>
          <ol className="mt-2 flex flex-col gap-1">
            {run.stages.map((stage) => (
              <li key={stage.name} className="flex justify-between gap-2">
                <span className="capitalize">{stage.name}</span>
                <span className={STATUS_STYLE[stage.status]}>
                  {stage.status}
                  {stage.result !== null && stage.name !== "reload" && (
                    <span className="text-slate-400">
                      {" "}
                      ·{" "}
                      {typeof stage.result === "number"
                        ? `${formatInteger(stage.result)} rows`
                        : String(stage.result).replace("_", " ")}
                    </span>
                  )}
                </span>
              </li>
            ))}
          </ol>
          {progress && progress.total !== undefined && progress.cursor !== undefined && (
            <div className="mt-2">
              <div className="h-1.5 overflow-hidden rounded bg-slate-800">
                <div
                  className="h-full bg-orange-500"
                  style={{ width: `${Math.min(100, (100 * progress.cursor) / Math.max(1, progress.total))}%` }}
                />
              </div>
              <div className="mt-1 text-xs text-slate-400">
                {formatInteger(progress.cursor)} / {formatInteger(progress.total)} monuments
                {progress.failed ? ` · ${formatInteger(progress.failed)} failed` : ""}
              </div>
            </div>
          )}
          {run.error && <p className="mt-2 text-xs text-red-400">{run.error.detail ?? run.error.title}</p>}
        </div>
      )}
    </section>
  );
}

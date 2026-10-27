/* Pipeline runs (harvest, clean, reload): start one, follow it, cancel it.
 *
 * Lives at the app's top level, so a run is followed whether or not its panel
 * is open. When a run's reload stage succeeds the server answers from the new
 * corpus, and `onCorpusReloaded` lets the app fetch what it shows again. */
import { useCallback, useEffect, useRef, useState } from "react";
import {
  ApiError,
  getJson,
  postJson,
  STATIC_MODE,
  type PipelineRun,
  type PipelineRunRequest,
} from "../../api/client";

const POLL_MS = 2000;
export const ACTIVE = new Set(["queued", "running", "cancelling"]);

export interface Pipeline {
  run: PipelineRun | null;
  error: string | null;
  active: boolean;
  start: (request: PipelineRunRequest) => void;
  cancel: () => void;
}

export function usePipeline(onCorpusReloaded: () => void): Pipeline {
  const [run, setRun] = useState<PipelineRun | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Bumped after a failed poll, so polling carries on until the server answers.
  const [retry, setRetry] = useState(0);
  const reported = useRef<string | null>(null);

  // The latest run, so a page opened mid-run shows it.
  useEffect(() => {
    if (STATIC_MODE) return;
    getJson<{ data: PipelineRun[] }>("/pipeline-runs").then(
      (body) => {
        const latest = body.data[0] ?? null;
        if (latest) reported.current = latest.id; // its reload, if any, is already served
        setRun(latest);
      },
      () => {},
    );
  }, []);

  useEffect(() => {
    if (STATIC_MODE || !run || !ACTIVE.has(run.status)) return;
    const timer = window.setTimeout(() => {
      getJson<PipelineRun>(`/pipeline-runs/${run.id}`).then(
        (next) => {
          setError(null);
          setRun(next);
        },
        (reason: Error) => {
          if (reason instanceof ApiError && reason.status === 404) {
            // The registry lives in memory: a restarted server forgets runs.
            setError(
              "The server restarted and no longer knows this run. If it was cleaning, it " +
                "probably ran out of memory: give Docker at least 4 GB, then run again.",
            );
            setRun(null);
            return;
          }
          // fetch() rejects with a bare "Load failed" / "Failed to fetch" when
          // the server does not answer; keep polling until it does.
          setError("Lost contact with the server; still trying…");
          setRetry((count) => count + 1);
        },
      );
    }, POLL_MS);
    return () => window.clearTimeout(timer);
  }, [run, retry]);

  useEffect(() => {
    if (!run || reported.current === run.id) return;
    const reload = run.stages.find((stage) => stage.name === "reload");
    if (reload?.status === "succeeded") {
      reported.current = run.id;
      onCorpusReloaded();
    }
  }, [run, onCorpusReloaded]);

  const start = useCallback((request: PipelineRunRequest) => {
    setError(null);
    postJson<PipelineRun>("/pipeline-runs", request).then(setRun, (reason: Error) =>
      setError(reason.message),
    );
  }, []);

  const cancel = useCallback(() => {
    if (!run) return;
    postJson<PipelineRun>(`/pipeline-runs/${run.id}/cancel`).then(setRun, (reason: Error) =>
      setError(reason.message),
    );
  }, [run]);

  return { run, error, active: run !== null && ACTIVE.has(run.status), start, cancel };
}

/* The pipeline panel: a sheet sliding in from the right.
 *
 * What to run (harvest, clean and reload), the harvest's options behind an
 * "Advanced" disclosure, and the current run as a list of stages with the
 * harvest's progress. */
import {
  Check,
  ChevronDown,
  CircleAlert,
  CircleDashed,
  Loader2,
  Minus,
  Play,
  Square,
  X,
} from "lucide-react";
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { type PipelineRunRequest } from "../../api/client";
import { Button, cx, IconButton } from "../../components/ui";
import { formatInteger } from "../../lib/format";
import { type Pipeline } from "./usePipeline";

interface Props {
  open: boolean;
  onClose: () => void;
  pipeline: Pipeline;
}

const STAGE_TEXT: Record<string, string> = {
  harvest: "Harvest from the EDCS",
  clean: "Clean the inscriptions",
  reload: "Load the new corpus",
};

const STATUS_LABEL: Record<string, string> = {
  queued: "Queued",
  running: "Running",
  cancelling: "Stopping",
  succeeded: "Finished",
  cancelled: "Cancelled",
  failed: "Failed",
};

function StageIcon({ status }: { status: string }) {
  const box = "flex size-6 flex-none items-center justify-center rounded-full";
  if (status === "succeeded")
    return (
      <span className={cx(box, "bg-known/15 text-known")}>
        <Check className="size-3.5" strokeWidth={2.5} />
      </span>
    );
  if (status === "running")
    return (
      <span className={cx(box, "bg-accent-soft text-accent")}>
        <Loader2 className="size-3.5 animate-spin" />
      </span>
    );
  if (status === "failed")
    return (
      <span className={cx(box, "bg-red-500/10 text-red-600 dark:text-red-400")}>
        <CircleAlert className="size-3.5" />
      </span>
    );
  if (status === "skipped" || status === "cancelled")
    return (
      <span className={cx(box, "bg-surface-3 text-muted")}>
        <Minus className="size-3.5" />
      </span>
    );
  return (
    <span className={cx(box, "bg-surface-2 text-muted")}>
      <CircleDashed className="size-3.5" />
    </span>
  );
}

function Toggle({
  checked,
  onChange,
  title,
  hint,
  danger,
}: {
  checked: boolean;
  onChange: (value: boolean) => void;
  title: string;
  hint: ReactNode;
  danger?: boolean;
}) {
  return (
    <label className="flex cursor-pointer items-start gap-3 rounded-lg p-2.5 transition-colors hover:bg-surface-2">
      <input
        type="checkbox"
        className="peer sr-only"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        aria-label={title}
      />
      <span
        aria-hidden="true"
        className={cx(
          "relative mt-0.5 h-[18px] w-8 flex-none rounded-full bg-line-strong transition-colors",
          "peer-focus-visible:ring-2 peer-focus-visible:ring-accent-soft",
          "after:absolute after:top-[2px] after:left-[2px] after:size-[14px] after:rounded-full after:bg-surface after:shadow-sm after:transition-transform",
          "peer-checked:after:translate-x-[14px]",
          danger ? "peer-checked:bg-red-600" : "peer-checked:bg-ink",
        )}
      />
      <span className="min-w-0">
        <span className={cx("block text-[0.82rem] font-medium", danger ? "text-red-600 dark:text-red-400" : "text-fg")}>
          {title}
        </span>
        <span className="block text-[0.75rem] leading-relaxed text-muted">{hint}</span>
      </span>
    </label>
  );
}

export function PipelineSheet({ open, onClose, pipeline }: Props) {
  const { run, error, active, start, cancel } = pipeline;
  const [harvest, setHarvest] = useState(true);
  const [clean, setClean] = useState(true);
  const [limit, setLimit] = useState("");
  const [workers, setWorkers] = useState(16);
  const [refreshIndex, setRefreshIndex] = useState(false);
  const [restart, setRestart] = useState(false);
  const [advanced, setAdvanced] = useState(false);
  const panel = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => event.key === "Escape" && onClose();
    document.addEventListener("keydown", onKey);
    panel.current?.focus();
    return () => document.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  function submit(event: FormEvent) {
    event.preventDefault();
    if (restart && !window.confirm("Restart deletes the harvested corpus and starts over. Continue?"))
      return;
    const request: PipelineRunRequest = {
      harvest,
      clean,
      limit: limit ? Number(limit) : null,
      workers,
      restart,
      refresh_index: refreshIndex,
    };
    start(request);
  }

  const progress = run?.progress;
  const percent =
    progress && progress.total !== undefined && progress.cursor !== undefined
      ? Math.min(100, (100 * progress.cursor) / Math.max(1, progress.total))
      : null;

  return (
    <>
      <div
        aria-hidden="true"
        onClick={onClose}
        className={cx(
          "fixed inset-0 z-[1500] bg-black/20 backdrop-blur-[2px] transition-opacity duration-300 dark:bg-black/50",
          open ? "opacity-100" : "pointer-events-none opacity-0",
        )}
      />
      <div
        ref={panel}
        role="dialog"
        aria-modal="true"
        aria-labelledby="pipeline-title"
        tabIndex={-1}
        hidden={!open}
        className={cx(
          "fixed inset-y-0 right-0 z-[1600] flex w-full max-w-md flex-col border-l border-line bg-surface shadow-2xl outline-none",
          "animate-[sheet-in_300ms_cubic-bezier(0.32,0.72,0,1)]",
        )}
      >
        <header className="flex items-start justify-between gap-4 border-b border-line px-6 pt-5 pb-4">
          <div>
            <h2 id="pipeline-title" className="text-base font-semibold tracking-tight text-fg">
              Data pipeline
            </h2>
            <p className="mt-1 text-[0.8rem] leading-relaxed text-muted">
              Fetch new inscriptions from the EDCS, clean them, and load them into the map. Runs
              in the background; you can close this panel.
            </p>
          </div>
          <IconButton label="Close" size="sm" onClick={onClose}>
            <X className="size-4" />
          </IconButton>
        </header>

        <div className="flex-1 overflow-y-auto px-4 py-4">
          <form onSubmit={submit} className="flex flex-col gap-1" aria-label="Pipeline">
            <Toggle
              checked={harvest}
              onChange={setHarvest}
              title="Harvest from the EDCS"
              hint="Resumes where the last harvest stopped. A full harvest takes hours."
            />
            <Toggle
              checked={clean}
              onChange={setClean}
              title="Clean and reload the corpus"
              hint="Builds the conservative and interpretive texts, then swaps the new corpus into the map."
            />

            <button
              type="button"
              aria-expanded={advanced}
              onClick={() => setAdvanced((value) => !value)}
              disabled={!harvest}
              className="mt-1 flex items-center gap-1.5 self-start rounded-md px-2.5 py-1.5 text-[0.78rem] font-medium text-muted transition-colors hover:text-fg disabled:opacity-40"
            >
              <ChevronDown className={cx("size-3.5 transition-transform", advanced && harvest && "rotate-180")} />
              Harvest options
            </button>
            {advanced && harvest && (
              <div className="ml-2.5 flex flex-col gap-1 border-l border-line pl-3">
                <div className="grid grid-cols-2 gap-3 p-2.5">
                  <label className="flex flex-col gap-1.5 text-[0.75rem] font-medium text-fg-2">
                    Limit
                    <input
                      type="number"
                      min={1}
                      placeholder="All monuments"
                      className="field h-8"
                      value={limit}
                      onChange={(event) => setLimit(event.target.value)}
                    />
                  </label>
                  <label className="flex flex-col gap-1.5 text-[0.75rem] font-medium text-fg-2">
                    Workers
                    <input
                      type="number"
                      min={1}
                      max={32}
                      className="field h-8"
                      value={workers}
                      onChange={(event) => setWorkers(Number(event.target.value))}
                    />
                  </label>
                </div>
                <Toggle
                  checked={refreshIndex}
                  onChange={setRefreshIndex}
                  title="Refresh the corpus index"
                  hint="Fetch the list of monuments again, to pick up new ones."
                />
                <Toggle
                  checked={restart}
                  onChange={setRestart}
                  title="Restart from scratch"
                  hint="Deletes the harvested corpus first. Cannot be undone."
                  danger
                />
              </div>
            )}

            <Button
              type="submit"
              variant="primary"
              size="lg"
              className="mt-3"
              disabled={active || !(harvest || clean)}
            >
              <Play className="size-3.5" aria-hidden="true" />
              Run pipeline
            </Button>
          </form>

          {error && (
            <p className="mt-4 rounded-lg border border-red-500/30 bg-red-500/5 px-3 py-2.5 text-[0.8rem] text-red-700 dark:text-red-300">
              {error}
            </p>
          )}

          {run && (
            <section aria-label="Latest run" data-run={run.id} className="mt-6 rounded-xl border border-line">
              <div className="flex items-center justify-between gap-3 border-b border-line px-4 py-3">
                <div className="flex items-center gap-2 text-[0.82rem] font-semibold text-fg">
                  Latest run
                  <span
                    className={cx(
                      "rounded-full px-2 py-0.5 text-[0.7rem] font-semibold",
                      run.status === "succeeded" && "bg-known/15 text-known",
                      run.status === "failed" && "bg-red-500/10 text-red-600 dark:text-red-400",
                      active && "bg-accent-soft text-accent",
                      run.status === "cancelled" && "bg-surface-3 text-muted",
                    )}
                  >
                    {STATUS_LABEL[run.status] ?? run.status}
                  </span>
                </div>
                {run.links.cancel && run.status !== "cancelling" && (
                  <Button size="sm" variant="secondary" onClick={cancel}>
                    <Square className="size-3" aria-hidden="true" />
                    Stop
                  </Button>
                )}
              </div>
              <ol className="flex flex-col gap-3 px-4 py-4">
                {run.stages.map((stage) => (
                  <li key={stage.name} className="flex items-center gap-3">
                    <StageIcon status={stage.status} />
                    <span className="flex-1 text-[0.82rem] text-fg">{STAGE_TEXT[stage.name] ?? stage.name}</span>
                    <span className="text-[0.75rem] text-muted tabular-nums">
                      {stage.result !== null && stage.name !== "reload"
                        ? typeof stage.result === "number"
                          ? `${formatInteger(stage.result)} rows`
                          : String(stage.result).replace("_", " ")
                        : stage.status}
                    </span>
                  </li>
                ))}
              </ol>
              {percent !== null && progress && (
                <div className="border-t border-line px-4 py-3">
                  <div className="h-1.5 overflow-hidden rounded-full bg-surface-3">
                    <div
                      className="h-full rounded-full bg-accent transition-[width] duration-500"
                      style={{ width: `${percent}%` }}
                    />
                  </div>
                  <div className="mt-2 flex justify-between text-[0.72rem] text-muted tabular-nums">
                    <span>
                      {formatInteger(progress.cursor ?? 0)} of {formatInteger(progress.total ?? 0)} monuments
                    </span>
                    {progress.failed ? <span>{formatInteger(progress.failed)} failed</span> : null}
                  </div>
                </div>
              )}
              {run.error && (
                <p className="border-t border-line px-4 py-3 text-[0.78rem] text-red-600 dark:text-red-400">
                  {run.error.detail ?? run.error.title}
                </p>
              )}
            </section>
          )}
        </div>
      </div>
    </>
  );
}

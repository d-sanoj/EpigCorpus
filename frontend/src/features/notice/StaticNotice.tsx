/* What the hosted build says about itself: a snapshot of the corpus, as fresh
 * as the date it was scraped through, with the pipeline left out and a clone
 * one command away.
 *
 * Only the static build renders it (App.tsx): the API-served app has a server
 * behind it, so there is nothing to disclose. The date is the corpus's own
 * `retrieved_at` stamps, via the manifest the app loads anyway. It has no
 * close button: the date belongs on the page, not behind a dismissal.
 */
import { Check, ChevronDown, Copy, Info } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { loadManifest } from "../../api/static/artifacts";
import { Button, cx, IconButton } from "../../components/ui";

/** The repository this page is built from: where the clone starts. */
const REPO_URL = "https://github.com/d-sanoj/EpigCorpus";

/** The whole app, from nothing: Docker builds the API and the UI. */
const COMMANDS = `git clone ${REPO_URL}.git
cd EpigCorpus
docker compose up --build`;

/** "2026-08-19" as "19 August 2026", in UTC: the day the scrape recorded. */
function longDate(iso: string): string {
  const date = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return iso;
  return new Intl.DateTimeFormat("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  }).format(date);
}

export function StaticNotice() {
  const [dataAsOf, setDataAsOf] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  useEffect(() => {
    // The same manifest the corpus itself is loaded through: one fetch.
    let current = true;
    loadManifest()
      .then((manifest) => current && setDataAsOf(manifest.data_as_of ?? null))
      .catch(() => {});
    return () => {
      current = false;
    };
  }, []);

  useEffect(() => () => window.clearTimeout(timer.current), []);

  async function copy() {
    try {
      await navigator.clipboard.writeText(COMMANDS);
      setCopied(true);
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setCopied(false), 2000);
    } catch {
      // No clipboard (an insecure origin, a denied permission): the commands
      // stay on screen to be selected by hand.
    }
  }

  return (
    <section
      aria-label="About this snapshot"
      className="flex-none border-b border-line bg-surface-2 px-3 py-2 text-[0.78rem] text-fg-2 sm:px-4"
    >
      {/* Centred, not top-aligned: the button is taller than a line of text,
          and top-aligning it leaves it hanging below the line it sits beside. */}
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-2">
        <Info className="size-4 flex-none text-accent" aria-hidden="true" />
        <p className="min-w-0 flex-1 basis-72 leading-relaxed">
          <span className="font-semibold text-fg">Static snapshot.</span>{" "}
          {dataAsOf ? (
            <>
              The inscriptions were scraped through{" "}
              <span className="font-medium text-fg">{longDate(dataAsOf)}</span>.
            </>
          ) : (
            <>The inscriptions are the cleaned corpus committed in the repository.</>
          )}{" "}
          Search, filters, the results table and the map run in your browser from the files this
          build shipped, so nothing harvests or cleans on demand here.
        </p>
        <Button
          size="sm"
          variant="ghost"
          className="ml-auto text-[0.78rem]"
          onClick={() => setOpen((value) => !value)}
          aria-expanded={open}
          aria-controls="static-notice-local"
        >
          Run it locally
          <ChevronDown
            className={cx("size-3.5 transition-transform", open && "rotate-180")}
            aria-hidden="true"
          />
        </Button>
      </div>

      {open && (
        <div id="static-notice-local" className="mt-2 pl-[26px]">
          <p className="text-muted">
            A clone has what this page leaves out — the pipeline (harvest from the EDCS, clean,
            reload), the full-column TSV and the 4K PNG. Run it with Docker:
          </p>
          <div className="relative mt-2 max-w-[40rem]">
            <pre className="overflow-x-auto rounded-lg border border-line bg-surface px-3 py-2 pr-11 font-mono text-[0.72rem] leading-relaxed text-fg-2">
              {COMMANDS}
            </pre>
            <IconButton
              label={copied ? "Copied" : "Copy the commands"}
              size="sm"
              className="absolute top-1 right-1"
              onClick={copy}
            >
              {copied ? (
                <Check className="size-4 text-known" aria-hidden="true" />
              ) : (
                <Copy className="size-4" aria-hidden="true" />
              )}
            </IconButton>
          </div>
          <p className="mt-1.5 text-muted">
            Then open{" "}
            <span className="font-mono text-[0.72rem] text-fg-2">http://localhost:8000</span>. No
            Docker? The{" "}
            <a
              href={`${REPO_URL}#run-it`}
              target="_blank"
              rel="noreferrer"
              className="font-medium text-accent underline underline-offset-2 hover:no-underline"
            >
              README
            </a>{" "}
            has the uv and Node route.
          </p>
        </div>
      )}
    </section>
  );
}

/* EpigCorpus: search the EDCS corpus, see it on the map, read the matches.
 *
 * Layout: a slim top bar; the search panel on the left (collapsible); the map
 * filling the rest, with its summary, controls, legend and the results
 * drawer floating over it. The pipeline opens as a sheet from the right.
 *
 * Behaviour:
 * - a query is a keyword, any filter, or both; nothing runs until Search is
 *   pressed (or Enter in the keyword field);
 * - with no query the map shows every known place; with one, the results on
 *   top, and the view fits them;
 * - the submitted query is kept in the address bar, so it can be shared.
 */
import {
  AlertTriangle,
  BookOpen,
  DatabaseZap,
  Loader2,
  Monitor,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
  Sun,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { API, getJson, type Facets, type KnownPlaces, type ResultPlaces } from "./api/client";
import { fromSearch, isQuery, queryParams, toSearch, type Query } from "./api/query";
import { Button, cx, IconButton, Segmented } from "./components/ui";
import type { MapPlace } from "./features/map/epigMap";
import { MapView } from "./features/map/MapView";
import { PipelineSheet } from "./features/pipeline/PipelineSheet";
import { usePipeline } from "./features/pipeline/usePipeline";
import { ResultsDrawer } from "./features/results/ResultsDrawer";
import { SearchPanel } from "./features/search/SearchPanel";
import { formatInteger, plural } from "./lib/format";
import { useTheme, type ThemeChoice } from "./lib/theme";

type Feature = {
  geometry: { coordinates: number[] };
  properties: { place: string; count: number; examples?: string[] };
};

export function toMapPlaces(features: Feature[]): MapPlace[] {
  return features.map(({ geometry, properties }) => ({
    place: properties.place,
    longitude: geometry.coordinates[0],
    latitude: geometry.coordinates[1],
    count: properties.count,
    examples: properties.examples,
  }));
}

interface Shown {
  query: Query;
  title: string;
  detail: string;
  results: MapPlace[] | null;
  inscriptions: number;
  places: number;
  state: "loading" | "ready" | "empty" | "error";
}

const THEMES: { value: ThemeChoice; label: ReactNode; title: string }[] = [
  { value: "light", label: <Sun className="size-3.5" aria-label="Light" />, title: "Light" },
  { value: "dark", label: <Moon className="size-3.5" aria-label="Dark" />, title: "Dark" },
  { value: "system", label: <Monitor className="size-3.5" aria-label="System" />, title: "Match the system" },
];

function Logo() {
  return (
    <img src="/epigcorpus-logo.png" className="size-7 flex-none" alt="" aria-hidden="true" />
  );
}

export function App() {
  const [theme, themeChoice, setThemeChoice] = useTheme();
  const initial = useRef(fromSearch(window.location.search));
  const [draft, setDraft] = useState<Query>(initial.current);
  const [facets, setFacets] = useState<Facets | null>(null);
  const [known, setKnown] = useState<MapPlace[] | null>(null);
  const [corpus, setCorpus] = useState<{ places: number; inscriptions: number } | null>(null);
  const [shown, setShown] = useState<Shown>({
    query: initial.current,
    title: "Loading the corpus…",
    detail: "",
    results: null,
    inscriptions: 0,
    places: 0,
    state: "loading",
  });
  const [busy, setBusy] = useState(true);
  const [panelOpen, setPanelOpen] = useState(() => window.innerWidth >= 768);
  const [pipelineOpen, setPipelineOpen] = useState(false);
  const run = useRef(0);
  const corpusLoad = useRef<Promise<[unknown, KnownPlaces]> | null>(null);

  const loadCorpus = useCallback(() => {
    // Facets and known places: the filters' values and the no-query map.
    // Fetched again after a pipeline run reloads the corpus.
    const facetsLoad = getJson<Facets>("/facets").then(setFacets);
    const knownLoad = getJson<KnownPlaces>("/places/known.geojson").then((body) => {
      setKnown(toMapPlaces(body.features));
      setCorpus(body.summary);
      return body;
    });
    const both = Promise.all([facetsLoad, knownLoad]);
    both.catch(() => {});
    corpusLoad.current = both;
    return both;
  }, []);

  const submit = useCallback(async (query: Query) => {
    const mine = ++run.current;
    const search = toSearch(query);
    if (window.location.search !== search) {
      window.history.replaceState(null, "", window.location.pathname + search);
    }
    setBusy(true);
    try {
      if (!isQuery(query)) {
        const [, body] = await corpusLoad.current!;
        if (mine !== run.current) return;
        setShown({
          query,
          title: "All known places",
          detail: `${plural(body.summary.places, "place", "places")} · ${plural(body.summary.inscriptions, "inscription", "inscriptions")}`,
          results: null,
          inscriptions: 0,
          places: 0,
          state: "ready",
        });
        return;
      }
      setShown((current) => ({ ...current, query, state: "loading" }));
      const body = await getJson<ResultPlaces>(`/places.geojson?${queryParams(query)}`);
      if (mine !== run.current) return;
      const { summary } = body;
      setShown({
        query,
        title: summary.title,
        detail: summary.inscriptions
          ? `${plural(summary.inscriptions, "inscription", "inscriptions")} · ${plural(summary.places, "place", "places")} · ${summary.subtitle}`
          : "Nothing matches that combination. Try removing a filter.",
        results: toMapPlaces(body.features),
        inscriptions: summary.inscriptions,
        places: summary.places,
        state: summary.inscriptions ? "ready" : "empty",
      });
    } catch (error) {
      if (mine !== run.current) return;
      setShown((current) => ({ ...current, detail: (error as Error).message, state: "error" }));
    } finally {
      if (mine === run.current) setBusy(false);
    }
  }, []);

  useEffect(() => {
    loadCorpus();
    submit(initial.current);
  }, [loadCorpus, submit]);

  const shownQuery = useRef(shown.query);
  shownQuery.current = shown.query;
  const onCorpusReloaded = useCallback(() => {
    loadCorpus();
    submit(shownQuery.current);
  }, [loadCorpus, submit]);
  const pipeline = usePipeline(onCorpusReloaded);

  const hasResults = shown.results !== null && shown.inscriptions > 0;
  // 4K: 3840 x 2798, the same picture as the standard export, sharper.
  const pngHref = `${API}/exports/map.png?${queryParams(shown.query, { resolution: "4k" })}`;

  return (
    <div className="flex h-full flex-col bg-bg text-fg">
      {/* ── top bar ── */}
      <header className="flex h-14 flex-none items-center gap-3 border-b border-line bg-surface px-3 sm:px-4">
        <IconButton
          label={panelOpen ? "Hide search panel" : "Show search panel"}
          onClick={() => setPanelOpen((open) => !open)}
          aria-expanded={panelOpen}
          aria-controls="search-panel"
        >
          {panelOpen ? <PanelLeftClose className="size-[18px]" /> : <PanelLeftOpen className="size-[18px]" />}
        </IconButton>
        <div className="flex min-w-0 items-center gap-2.5">
          <Logo />
          <div className="min-w-0 leading-tight">
            <h1 className="text-[0.95rem] font-semibold tracking-tight">EpigCorpus</h1>
            <p className="hidden truncate text-[0.72rem] text-muted sm:block">
              Latin inscriptions of the Roman world
            </p>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-2">
          {corpus && (
            <div className="mr-2 hidden items-center gap-4 text-[0.75rem] text-muted lg:flex">
              <span>
                <span className="font-semibold text-fg tabular-nums">{formatInteger(corpus.inscriptions)}</span>{" "}
                inscriptions
              </span>
              <span className="h-3.5 w-px bg-line-strong" />
              <span>
                <span className="font-semibold text-fg tabular-nums">{formatInteger(corpus.places)}</span>{" "}
                places
              </span>
            </div>
          )}
          <Button
            size="sm"
            variant="secondary"
            onClick={() => setPipelineOpen(true)}
            aria-haspopup="dialog"
            className="relative"
          >
            <DatabaseZap className="size-3.5" aria-hidden="true" />
            <span className="hidden sm:inline">Pipeline</span>
            {pipeline.active && (
              <span className="absolute -top-1 -right-1 flex size-2.5">
                <span className="absolute inline-flex size-full animate-ping rounded-full bg-accent opacity-60" />
                <span className="relative inline-flex size-2.5 rounded-full bg-accent" />
              </span>
            )}
          </Button>
          <Segmented label="Theme" value={themeChoice} options={THEMES} onChange={setThemeChoice} size="sm" />
          <a
            href={`${API}/openapi.yaml`}
            className="hidden size-9 items-center justify-center rounded-lg text-fg-2 transition-colors hover:bg-surface-2 hover:text-fg sm:inline-flex"
            title="API reference (OpenAPI)"
            aria-label="API reference"
          >
            <BookOpen className="size-[17px]" />
          </a>
        </div>
      </header>

      <div className="relative flex min-h-0 flex-1">
        {/* ── search panel ── */}
        <aside
          id="search-panel"
          hidden={!panelOpen}
          className={cx(
            "z-[1200] w-full flex-none border-r border-line bg-surface sm:w-[22rem]",
            "absolute inset-y-0 left-0 shadow-2xl md:static md:shadow-none",
          )}
        >
          <SearchPanel
            draft={draft}
            onDraftChange={setDraft}
            onSubmit={() => {
              submit({ ...draft, q: draft.q.trim() });
              if (window.innerWidth < 768) setPanelOpen(false);
            }}
            facets={facets}
            busy={busy}
          />
        </aside>

        {/* ── map ── */}
        <main className="relative min-w-0 flex-1">
          <MapView
            known={known}
            results={shown.results}
            theme={theme}
            pngHref={pngHref}
            bottomBar={hasResults}
          >
            {/* What the map shows, top left. */}
            <div className="pointer-events-none absolute top-3 left-3 z-[1000] max-w-[min(34rem,calc(100%-5rem))]">
              <div className="float-card pointer-events-auto px-4 py-3" aria-live="polite">
                <div className="flex items-center gap-2 text-[0.9rem] font-semibold tracking-tight text-fg">
                  {shown.state === "loading" && <Loader2 className="size-4 animate-spin text-muted" aria-hidden="true" />}
                  {shown.state === "empty" && <AlertTriangle className="size-4 text-amber-600" aria-hidden="true" />}
                  <span className="truncate">{shown.state === "loading" && isQuery(shown.query) ? "Searching…" : shown.title}</span>
                </div>
                {shown.detail && (
                  <p
                    id="summary"
                    className={cx(
                      "mt-0.5 text-[0.78rem] leading-relaxed",
                      shown.state === "error" ? "text-red-600 dark:text-red-400" : "text-muted",
                    )}
                  >
                    {shown.detail}
                  </p>
                )}
              </div>
            </div>

            {hasResults && (
              <ResultsDrawer query={shown.query} inscriptions={shown.inscriptions} places={shown.places} />
            )}
          </MapView>
        </main>
      </div>

      <PipelineSheet open={pipelineOpen} onClose={() => setPipelineOpen(false)} pipeline={pipeline} />
    </div>
  );
}

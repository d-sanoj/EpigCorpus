/* EpigCorpus: search the EDCS corpus, see it on the map, read the matches.
 *
 * Layout: the sidebar (search, filters, downloads, pipeline) beside the map,
 * with the results table under the map once a query has matches.
 *
 * Behaviour carried over from the earlier UI:
 * - a query is a keyword, any filter, or both; nothing runs until Search is
 *   pressed (or Enter in the keyword box);
 * - with no query the map shows every known place in teal; with one, the
 *   results in orange, and the view fits them;
 * - "Map as PNG" always reflects what the map shows;
 * - the submitted query is kept in the address bar, so it can be shared.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import {
  getJson,
  type Facets,
  type KnownPlaces,
  type ResultPlaces,
} from "./api/client";
import { fromSearch, isQuery, queryParams, toSearch, type Query } from "./api/query";
import type { MapPlace } from "./features/map/epigMap";
import { MapView } from "./features/map/MapView";
import { PipelinePanel } from "./features/pipeline/PipelinePanel";
import { ResultsTable } from "./features/results/ResultsTable";
import { Sidebar } from "./features/search/Sidebar";
import { formatInteger } from "./lib/format";

type Feature = { geometry: { coordinates: number[] }; properties: { place: string; count: number; examples?: string[] } };

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
  summary: string;
  results: MapPlace[] | null;
  inscriptions: number;
}

type Notice = { text: string; kind: "info" | "warning" | "error" } | null;

export function App() {
  const initial = useRef(fromSearch(window.location.search));
  const [draft, setDraft] = useState<Query>(initial.current);
  const [facets, setFacets] = useState<Facets | null>(null);
  const [known, setKnown] = useState<MapPlace[] | null>(null);
  const [knownSummary, setKnownSummary] = useState<{ places: number } | null>(null);
  const [shown, setShown] = useState<Shown>({
    query: initial.current,
    summary: "Loading inscriptions…",
    results: null,
    inscriptions: 0,
  });
  const [busy, setBusy] = useState(true);
  const [notice, setNotice] = useState<Notice>(null);
  const run = useRef(0);

  const loadCorpus = useCallback(() => {
    // Facets and known places: the filters' values and the no-query map.
    // Fetched again after a pipeline run reloads the corpus.
    const facetsLoad = getJson<Facets>("/facets").then(setFacets);
    const knownLoad = getJson<KnownPlaces>("/places/known.geojson").then((body) => {
      setKnown(toMapPlaces(body.features));
      setKnownSummary(body.summary);
      return body;
    });
    return Promise.all([facetsLoad, knownLoad]);
  }, []);

  const submit = useCallback(
    async (query: Query, corpus: Promise<unknown>) => {
      const mine = ++run.current;
      const search = toSearch(query);
      if (window.location.search !== search) {
        window.history.replaceState(null, "", window.location.pathname + search);
      }
      setBusy(true);
      setNotice(null);
      try {
        if (!isQuery(query)) {
          const [, body] = (await corpus) as [unknown, KnownPlaces];
          if (mine !== run.current) return;
          setShown({
            query,
            summary: `All known places: ${formatInteger(body.summary.places)}. Search by keyword, by filter, or by both.`,
            results: null,
            inscriptions: 0,
          });
          setNotice({ text: "Enter a keyword, choose filters, or both, then press Search.", kind: "info" });
          return;
        }
        setShown((current) => ({ ...current, summary: "Searching inscriptions…" }));
        const body = await getJson<ResultPlaces>(`/places.geojson?${queryParams(query)}`);
        if (mine !== run.current) return;
        const { summary } = body;
        setShown({
          query,
          summary: `${formatInteger(summary.inscriptions)} inscriptions match. ${summary.subtitle}.`,
          results: toMapPlaces(body.features),
          inscriptions: summary.inscriptions,
        });
        if (!summary.inscriptions) {
          setNotice({ text: "Nothing matches that combination. Try removing a filter.", kind: "warning" });
        }
      } catch (error) {
        if (mine !== run.current) return;
        setShown((current) => ({ ...current, summary: "" }));
        setNotice({ text: (error as Error).message, kind: "error" });
      } finally {
        if (mine === run.current) setBusy(false);
      }
    },
    [],
  );

  const corpus = useRef<Promise<unknown> | null>(null);
  useEffect(() => {
    corpus.current = loadCorpus();
    corpus.current.catch(() => {});
    submit(initial.current, corpus.current);
  }, [loadCorpus, submit]);

  const onCorpusReloaded = useCallback(() => {
    corpus.current = loadCorpus();
    corpus.current.catch(() => {});
    submit(shown.query, corpus.current);
  }, [loadCorpus, submit, shown.query]);

  const hasResults = shown.results !== null && shown.inscriptions > 0;
  const noticeStyle = {
    info: "border-teal-400/40 bg-teal-400/10 text-teal-50",
    warning: "border-amber-500/45 bg-amber-500/10 text-amber-100",
    error: "border-red-400/45 bg-red-400/10 text-red-200",
  };

  return (
    <div className="flex min-h-dvh flex-col md:h-dvh md:flex-row md:overflow-hidden">
      <aside className="flex w-full shrink-0 flex-col gap-5 border-slate-800 bg-slate-950 p-4 md:w-[21rem] md:overflow-y-auto md:border-r">
        <header>
          <h1 className="text-2xl font-bold">EpigCorpus</h1>
          <p className="text-sm text-slate-400">EDCS inscription exploration across the Roman Empire</p>
        </header>
        <Sidebar
          draft={draft}
          onDraftChange={setDraft}
          onSubmit={() => submit({ ...draft, q: draft.q.trim() }, corpus.current!)}
          facets={facets}
          busy={busy}
          shown={shown.query}
          hasResults={hasResults}
        />
        <div className="border-t border-slate-800 pt-4">
          <PipelinePanel onCorpusReloaded={onCorpusReloaded} />
        </div>
        <footer className="mt-auto text-xs text-slate-500">
          Data: Epigraphik-Datenbank Clauss / Slaby (EDCS). Map layers: AWMC, Hanson 2016 (OXREP),
          Natural Earth. <a className="underline" href="/api/v1/openapi.yaml">API</a>
          {knownSummary && <> · {formatInteger(knownSummary.places)} places</>}
        </footer>
      </aside>

      {/* On a phone the page scrolls: the map follows the sidebar at a fixed
          height. From md up everything fits the window side by side. */}
      <main className="order-first flex min-w-0 flex-col gap-3 p-3 md:order-none md:min-h-0 md:flex-1">
        <div id="summary" aria-live="polite" className="text-center text-sm text-slate-300">
          {shown.summary}
        </div>
        {notice && (
          <div role="status" className={`rounded-lg border px-4 py-2.5 text-sm ${noticeStyle[notice.kind]}`}>
            {notice.text}
          </div>
        )}
        <div className={`h-[65vh] md:h-auto md:min-h-80 ${hasResults ? "md:flex-[3]" : "md:flex-1"}`}>
          <MapView known={known} results={shown.results} />
        </div>
        {hasResults && (
          <div className="h-[70vh] md:h-auto md:min-h-56 md:flex-[2]">
            <ResultsTable query={shown.query} total={shown.inscriptions} />
          </div>
        )}
      </main>
    </div>
  );
}

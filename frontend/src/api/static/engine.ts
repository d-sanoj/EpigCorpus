/* The /api/v1 paths the app calls, answered from the static build's files.
 *
 * The components keep asking for the same paths - /facets, /places.geojson,
 * /inscriptions/search, the map layers - so nothing in the UI has to know
 * whether a server or this engine answers. What is precomputed is fetched
 * (facets, known places, the map layers); what depends on the query is
 * computed here: filters, the keyword scan, the per-place aggregation, the
 * phrasing and the paging.
 *
 * Two API routes are deliberately not here, and the UI does without them on a
 * serverless build: /pipeline-runs (nothing can harvest) and /exports/map.png
 * (the map exports itself instead).
 */
import { DATA_BASE, fetchJson, loadCorpus, loadManifest } from "./artifacts";
import type { Mode } from "./container";
import { describeQuery, FILTER_ORDER, type Selections } from "./phrasing";
import { aggregatePlaces, matchRows, summary, TSV_COLUMNS, tsvLine, tsvValues } from "./query";
import { textStore } from "./textStore";

interface ParsedQuery {
  q: string;
  mode: Mode;
  selections: Selections;
  limit: number;
  cursor: string | null;
  /** What a cursor is bound to, so it cannot page a different query. */
  canonical: string;
}

// The filter-only pages (/inscriptions) carry no search_in; the mode is the one
// the query that produced them was run in.
let lastMode: Mode = "raw";

function parseQuery(path: string): ParsedQuery {
  const [, search] = path.split("?");
  const params = new URLSearchParams(search ?? "");
  const selections = Object.fromEntries(
    FILTER_ORDER.map((name) => [name, params.getAll(name)]),
  ) as Selections;
  const requested = params.get("search_in");
  const mode: Mode =
    requested === "raw" || requested === "conservative" || requested === "interpretive"
      ? requested
      : lastMode;
  const q = (params.get("q") ?? "").trim();
  const limit = Number(params.get("limit") ?? 50);
  return {
    q,
    mode,
    selections,
    limit: Number.isFinite(limit) && limit > 0 ? limit : 50,
    cursor: params.get("cursor"),
    canonical: JSON.stringify({ q, mode, selections }),
  };
}

async function ensureText(mode: Mode): Promise<void> {
  const manifest = await loadManifest();
  const corpus = await loadCorpus();
  await textStore().load(
    mode,
    DATA_BASE + manifest.files.text[mode],
    manifest.text_bytes[mode],
    corpus.textOffsets[mode],
  );
}

async function hitsFor(query: ParsedQuery): Promise<Uint8Array | null> {
  if (!query.q) return null;
  await ensureText(query.mode);
  return textStore().search(query.q);
}

/** Places as GeoJSON, with the summary and popup examples the map shows. */
async function resultPlaces(query: ParsedQuery) {
  lastMode = query.mode;
  const corpus = await loadCorpus();
  const rows = matchRows(corpus, query.selections, await hitsFor(query));
  const places = aggregatePlaces(corpus, rows);

  const examples = new Map<number, string[]>();
  if (rows.length) {
    await ensureText(query.mode);
    const found = await textStore().examples(rows, corpus.coord, corpus.edcs);
    found.coordIds.forEach((id, position) => examples.set(id, found.items[position]));
  }

  const { title, subtitle } = describeQuery(query.q, query.mode, query.selections);
  return {
    type: "FeatureCollection",
    summary: { inscriptions: rows.length, places: places.length, title, subtitle },
    features: places.map((place) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [place.longitude, place.latitude] },
      properties: {
        place: place.place,
        count: place.count,
        examples: examples.get(place.coordinate) ?? [],
      },
    })),
  };
}

/** Matched inscriptions, one page at a time, as the API pages them. */
async function inscriptionsPage(query: ParsedQuery) {
  lastMode = query.mode;
  const corpus = await loadCorpus();
  const rows = matchRows(corpus, query.selections, await hitsFor(query));
  const total = rows.length;

  const start = query.cursor ? decodeCursor(query.cursor, query.canonical) : 0;
  const stop = Math.min(start + query.limit, total);
  const page = rows.subarray(start, stop);

  let texts: string[] = [];
  if (page.length) {
    await ensureText(query.mode);
    texts = await textStore().texts(page);
  }

  return {
    data: Array.from(page, (row, position) =>
      summary(corpus, row, query.mode, texts[position] ?? null),
    ),
    page: {
      limit: query.limit,
      total,
      next_cursor: stop < total ? encodeCursor(stop, query.canonical) : null,
    },
  };
}

function encodeCursor(offset: number, canonical: string): string {
  const bytes = new TextEncoder().encode(JSON.stringify({ o: offset, f: canonical }));
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function decodeCursor(cursor: string, canonical: string): number {
  let parsed: { o?: unknown; f?: unknown };
  try {
    const padded = cursor.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(padded + "=".repeat((4 - (padded.length % 4)) % 4));
    const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error("Malformed cursor.");
  }
  if (parsed.f !== canonical || typeof parsed.o !== "number" || parsed.o < 0) {
    throw new Error("That cursor belongs to another query. Start from the first page.");
  }
  return parsed.o;
}

/** The TSV download, built here rather than streamed by a server.
 *
 * The API's own export carries every column of the cleaned corpus and every
 * row of the monuments a query matched; this build only holds the search
 * frame, so the download carries the columns the app itself shows.
 */
export async function staticTsv(
  params: string,
): Promise<{ blob: Blob; filename: string }> {
  const query = parseQuery(`/exports/search.tsv?${params}`);
  const corpus = await loadCorpus();
  const rows = matchRows(corpus, query.selections, await hitsFor(query));
  await ensureText(query.mode);

  const parts: BlobPart[] = [`${tsvLine([...TSV_COLUMNS])}\n`];
  const batch = 5000;
  for (let start = 0; start < rows.length; start += batch) {
    const page = rows.subarray(start, Math.min(start + batch, rows.length));
    const texts = await textStore().texts(page);
    const lines: string[] = [];
    page.forEach((row, position) => {
      lines.push(
        tsvLine(tsvValues(summary(corpus, row, query.mode, texts[position] ?? null))),
      );
    });
    parts.push(`${lines.join("\n")}\n`);
  }
  return {
    blob: new Blob(parts, { type: "text/tab-separated-values" }),
    filename: "edcs_search_results.tsv",
  };
}

/** Everything the components ask `getJson` for, answered locally. */
export async function staticGet<T>(path: string): Promise<T> {
  const [route] = path.split("?");

  if (route === "/map/layers/" || route.startsWith("/map/layers/")) {
    const name = route.slice("/map/layers/".length);
    const manifest = await loadManifest();
    const file = manifest.files.map[name];
    if (!file) throw new Error(`The static build has no "${name}" map layer.`);
    return fetchJson<T>(file);
  }

  switch (route) {
    case "/facets": {
      const manifest = await loadManifest();
      return fetchJson<T>(manifest.files.facets);
    }
    case "/places/known.geojson": {
      const manifest = await loadManifest();
      return fetchJson<T>(manifest.files.known_places);
    }
    case "/places.geojson":
      return resultPlaces(parseQuery(path)) as Promise<T>;
    case "/inscriptions":
    case "/inscriptions/search":
      return inscriptionsPage(parseQuery(path)) as Promise<T>;
    default:
      throw new Error(`${route} needs the API server, and this page has none.`);
  }
}

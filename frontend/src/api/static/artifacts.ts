/* The static build's files, fetched once each.
 *
 * Everything is fetched with `no-cache`: the browser revalidates against the
 * server instead of trusting a copy a redeploy has replaced. Names are fixed,
 * so the manifest carries the corpus fingerprint and each text blob's length,
 * and the loader checks both before anything is used - artifacts from two
 * builds can never be mixed.
 */
import { CONTAINER_FORMAT, decodeContainer, type Corpus, type Mode } from "./container";

export const DATA_BASE = `${import.meta.env.BASE_URL}data/`;

/** The spec the API-reference icon links to. Its name is fixed, like the rest. */
export const OPENAPI_URL = `${DATA_BASE}openapi.yaml`;

const NO_CACHE: RequestInit = { cache: "no-cache" };

export interface Manifest {
  format: string;
  fingerprint: string;
  rows: number;
  modes: Mode[];
  /** The newest `retrieved_at` in the corpus, `YYYY-MM-DD`; null if unknown. */
  data_as_of: string | null;
  text_bytes: Record<Mode, number>;
  files: {
    corpus: string;
    facets: string;
    known_places: string;
    openapi: string;
    text: Record<Mode, string>;
    map: Record<string, string>;
  };
}

async function fetchOrThrow(name: string): Promise<Response> {
  const response = await fetch(DATA_BASE + name, NO_CACHE);
  if (!response.ok) {
    throw new Error(`Could not load ${name} (${response.status} ${response.statusText}).`);
  }
  return response;
}

export async function fetchJson<T>(name: string): Promise<T> {
  return (await fetchOrThrow(name)).json() as Promise<T>;
}

/** A gzipped artifact, decompressed as it arrives. */
export async function fetchGzipUrl(url: string): Promise<ArrayBuffer> {
  const response = await fetch(url, NO_CACHE);
  if (!response.ok) {
    throw new Error(`Could not load ${url} (${response.status} ${response.statusText}).`);
  }
  if (typeof DecompressionStream === "undefined" || !response.body) {
    throw new Error("This browser cannot decompress the corpus. Try a current browser.");
  }
  return new Response(response.body.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer();
}

export function fetchGzip(name: string): Promise<ArrayBuffer> {
  return fetchGzipUrl(DATA_BASE + name);
}

let manifest: Promise<Manifest> | null = null;

export function loadManifest(): Promise<Manifest> {
  manifest ??= fetchJson<Manifest>("manifest.json").then((body) => {
    if (body.format !== CONTAINER_FORMAT) {
      throw new Error(`This page reads ${CONTAINER_FORMAT}, not ${body.format}.`);
    }
    return body;
  });
  return manifest;
}

let corpus: Promise<Corpus> | null = null;

export function loadCorpus(): Promise<Corpus> {
  corpus ??= (async () => {
    const manifest = await loadManifest();
    const decoded = decodeContainer(await fetchGzip(manifest.files.corpus));
    if (decoded.rows !== manifest.rows || decoded.fingerprint !== manifest.fingerprint) {
      throw new Error("The site's own files are from two different builds. Reload the page.");
    }
    return decoded;
  })();
  return corpus;
}

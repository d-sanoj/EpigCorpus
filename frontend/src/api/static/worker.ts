/* One search mode's inscription texts: searched and sliced off the main thread.
 *
 * The keyword is a case-insensitive substring, as the server's is. An ASCII
 * keyword is scanned byte by byte, which is what almost every search is; a
 * keyword carrying anything else falls back to decoding each row and comparing
 * strings, because folding UTF-8 case byte-wise is not the same as folding
 * text. Both paths are bounded by the loaded blob: the worker holds one mode
 * at a time, which is the largest object the page keeps.
 */
import { fetchGzipUrl } from "./artifacts";
import type { Mode } from "./container";

export interface LoadRequest {
  kind: "load";
  id: number;
  mode: Mode;
  url: string;
  bytes: number;
  offsets: Uint32Array;
}

export interface SearchRequest {
  kind: "search";
  id: number;
  needle: string;
}

export interface ExamplesRequest {
  kind: "examples";
  id: number;
  rows: Uint32Array;
  coords: Uint32Array;
  edcs: Uint32Array;
}

export interface TextsRequest {
  kind: "texts";
  id: number;
  rows: Uint32Array;
}

export type Request = LoadRequest | SearchRequest | ExamplesRequest | TextsRequest;

/** A request before the store gives it an id: what callers pass to `send`. */
export type Message =
  | Omit<LoadRequest, "id">
  | Omit<SearchRequest, "id">
  | Omit<ExamplesRequest, "id">
  | Omit<TextsRequest, "id">;

export type Reply =
  | { id: number; ok: true; kind: "load"; bytes: number }
  | { id: number; ok: true; kind: "search"; hits: Uint8Array }
  | { id: number; ok: true; kind: "examples"; coordIds: Uint32Array; items: string[][] }
  | { id: number; ok: true; kind: "texts"; texts: string[] }
  | { id: number; ok: false; error: string };

/** Inscriptions listed inside a result popup before it is truncated. */
const EXAMPLE_LIMIT = 8;
/** A popup text longer than this is cut. */
const EXAMPLE_WIDTH = 160;

let blob: Uint8Array | null = null;
let offsets: Uint32Array | null = null;

const decoder = new TextDecoder();

/** Fold an ASCII letter to lower case; every other byte is left alone. */
function fold(byte: number): number {
  return byte >= 65 && byte <= 90 ? byte + 32 : byte;
}

function slice(row: number): string {
  return decoder.decode(blob!.subarray(offsets![row], offsets![row + 1]));
}

function loadText(text: string): string {
  const collapsed = text.split(/\s+/).filter(Boolean).join(" ");
  return collapsed.length > EXAMPLE_WIDTH
    ? `${collapsed.slice(0, EXAMPLE_WIDTH - 3)}\u2026`
    : collapsed;
}

async function load(request: LoadRequest): Promise<Reply> {
  const buffer = await fetchGzipUrl(request.url);
  const loaded = new Uint8Array(buffer);
  if (loaded.length !== request.bytes) {
    throw new Error(
      `The ${request.mode} texts are ${loaded.length} bytes, not the ${request.bytes} the site ` +
        "expects. The files are from two different builds; reload the page.",
    );
  }
  blob = loaded;
  offsets = request.offsets;
  return { id: request.id, ok: true, kind: "load", bytes: loaded.length };
}

function search(request: SearchRequest): Reply {
  if (!blob || !offsets) throw new Error("The texts are not loaded.");
  const rows = offsets.length - 1;
  const hits = new Uint8Array(rows);
  if (!request.needle) return { id: request.id, ok: true, kind: "search", hits };

  if (/^[\x20-\x7e]*$/.test(request.needle)) {
    const needle = new TextEncoder().encode(request.needle.toLowerCase());
    const first = needle[0];
    const width = needle.length;
    for (let row = 0; row < rows; row++) {
      const end = offsets[row + 1] - width + 1;
      for (let at = offsets[row]; at < end; at++) {
        if (fold(blob[at]) !== first) continue;
        let position = 1;
        while (position < width && fold(blob[at + position]) === needle[position]) position++;
        if (position === width) {
          hits[row] = 1;
          break;
        }
      }
    }
    return { id: request.id, ok: true, kind: "search", hits };
  }

  // Anything outside ASCII: compare text, where case folding is Unicode's.
  const needle = request.needle.toLowerCase();
  for (let row = 0; row < rows; row++) {
    if (slice(row).toLowerCase().includes(needle)) hits[row] = 1;
  }
  return { id: request.id, ok: true, kind: "search", hits };
}

function examples(request: ExamplesRequest): Reply {
  if (!blob || !offsets) throw new Error("The texts are not loaded.");
  // Keyed by coordinate alone, as the server's `_place_result_items` keys it.
  const buckets = new Map<number, string[]>();
  for (const row of request.rows) {
    const coordinate = request.coords[row];
    let bucket = buckets.get(coordinate);
    if (bucket && bucket.length >= EXAMPLE_LIMIT) continue;
    if (!bucket) {
      bucket = [];
      buckets.set(coordinate, bucket);
    }
    const identifier = `EDCS-${String(request.edcs[row]).padStart(8, "0")}`;
    const text = loadText(slice(row));
    bucket.push(text ? `${identifier} \u00b7 ${text}` : identifier);
  }
  return {
    id: request.id,
    ok: true,
    kind: "examples",
    coordIds: Uint32Array.from(buckets.keys()),
    items: [...buckets.values()],
  };
}

function texts(request: TextsRequest): Reply {
  if (!blob || !offsets) throw new Error("The texts are not loaded.");
  const result: string[] = [];
  for (const row of request.rows) result.push(slice(row));
  return { id: request.id, ok: true, kind: "texts", texts: result };
}

// Inside a worker this is the worker's own scope; the DOM typings describe a
// window, where the second argument of postMessage means something else.
const post = self.postMessage as (message: unknown, transfer?: Transferable[]) => void;

self.addEventListener("message", (event: MessageEvent<Request>) => {
  const request = event.data;
  const answer = async (): Promise<Reply> => {
    switch (request.kind) {
      case "load":
        return load(request);
      case "search":
        return search(request);
      case "examples":
        return examples(request);
      case "texts":
        return texts(request);
    }
  };
  answer().then(
    (reply) => {
      // The results are large: hand them over rather than copy them.
      if (reply.ok && reply.kind === "search") {
        post(reply, [reply.hits.buffer as ArrayBuffer]);
      } else if (reply.ok && reply.kind === "examples") {
        post(reply, [reply.coordIds.buffer as ArrayBuffer]);
      } else {
        post(reply);
      }
    },
    (reason: Error) => post({ id: request.id, ok: false, error: reason.message }),
  );
});

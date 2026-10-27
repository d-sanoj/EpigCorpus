/* The text worker, from the main thread's side.
 *
 * One instance for the page, one mode loaded at a time. A search for a mode
 * whose texts are not loaded fetches them first (10-14 MB gzipped), which is
 * the wait behind the first search of each mode - and behind every search
 * after a reload, unless the browser still has the file cached.
 */
import type { Mode } from "./container";
import type { Message, Reply, Request } from "./worker";

export interface Examples {
  coordIds: Uint32Array;
  items: string[][];
}

export class TextStore {
  private worker: Worker | null = null;
  private nextId = 1;
  private pending = new Map<number, (reply: Reply) => void>();
  private loaded: { mode: Mode; bytes: number } | null = null;

  private ensureWorker(): Worker {
    if (!this.worker) {
      this.worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
      this.worker.addEventListener("message", (event: MessageEvent<Reply>) => {
        this.pending.get(event.data.id)?.(event.data);
      });
    }
    return this.worker;
  }

  private send<T extends Reply>(message: Message, transfer: Transferable[] = []): Promise<T> {
    const worker = this.ensureWorker();
    const id = this.nextId++;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, (reply) => {
        this.pending.delete(id);
        if (reply.ok) resolve(reply as T);
        else reject(new Error(reply.error));
      });
      worker.postMessage({ ...message, id } as Request, transfer);
    });
  }

  /** Load a mode's texts, unless that exact blob is already loaded. */
  async load(mode: Mode, url: string, bytes: number, offsets: Uint32Array): Promise<void> {
    if (this.loaded?.mode === mode && this.loaded.bytes === bytes) return;
    // The worker keeps the offsets: the copy is cheap, and the container's own
    // view must not be transferred away from the main thread.
    const copy = offsets.slice();
    const reply = await this.send<Extract<Reply, { kind: "load" }>>(
      { kind: "load", mode, url, bytes, offsets: copy },
      [copy.buffer],
    );
    this.loaded = { mode, bytes: reply.bytes };
  }

  loadedMode(): Mode | null {
    return this.loaded?.mode ?? null;
  }

  async search(needle: string): Promise<Uint8Array> {
    return (await this.send<Extract<Reply, { kind: "search" }>>({ kind: "search", needle })).hits;
  }

  async examples(rows: Uint32Array, coords: Uint32Array, edcs: Uint32Array): Promise<Examples> {
    // Copies, because the worker keeps them: the container's own views must
    // stay on this side.
    const rowCopy = rows.slice();
    const coordCopy = coords.slice();
    const edcsCopy = edcs.slice();
    const reply = await this.send<Extract<Reply, { kind: "examples" }>>(
      { kind: "examples", rows: rowCopy, coords: coordCopy, edcs: edcsCopy },
      [rowCopy.buffer as ArrayBuffer, coordCopy.buffer as ArrayBuffer, edcsCopy.buffer as ArrayBuffer],
    );
    return { coordIds: reply.coordIds, items: reply.items };
  }

  async texts(rows: Uint32Array): Promise<string[]> {
    return (await this.send<Extract<Reply, { kind: "texts" }>>({ kind: "texts", rows })).texts;
  }
}

let store: TextStore | null = null;

export function textStore(): TextStore {
  store ??= new TextStore();
  return store;
}

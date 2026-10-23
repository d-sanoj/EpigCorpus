"""
EDCS Scraper — edcs.hist.uzh.ch
================================
Harvests all inscriptions from the Epigraphik-Datenbank Clauss / Slaby.

Rewritten 2026-08-19 for the EDCS release of 2026-08-07, which withdrew the
DataTables endpoint `/api/query` (now 403) and replaced it with a static-file
architecture. See docs/EDCS_API.md for the full contract.

FOLDER STRUCTURE (paths are defined in app/config/paths.py):
    EpigCorpus/app/
    ├── data/
    │   ├── raw/
    │   │   ├── edcs_inscriptions.jsonl     ← one record per line
    │   │   ├── edcs_inscriptions.tsv
    │   │   └── harvest/
    │   │       ├── edcs_checkpoint.json    ← resume cursor
    │   │       └── edcs_failed_ids.json    ← monuments that could not be fetched
    │   └── reference/
    │       └── edcs_lookup.json            ← materials / provinces / categories / languages
    ├── services/scrape.py                  ← this file: the harvest use case
    ├── adapters/edcs_client.py             ← HTTP
    ├── adapters/harvest_store.py           ← output, checkpoint, failures, manifest
    └── cli/scrape.py                       ← command line

HOW THE CURRENT API WORKS:
    1. /data/indexes/searchable.json  — the whole corpus index in one file
       (542,854 entries, ~19 MB gzip, no pagination)
    2. /data/indexes/{lookups,places,languages,sources}.json — code tables
    3. /data/monument/{shard}/{id8}.json — one static file per monument,
       where id8 = zero-padded 8-digit id and shard = its first 3 characters.
       Inscription TEXT lives only here, so a full harvest needs one request
       per monument.

KEY DESIGN DECISIONS (unchanged from the original):
    - One row per inscription (not per monument)
    - record_id = edcs_id + inscription index, e.g. EDCS-00000001-0
    - dating, language, category all come from inside each inscription
    - all categories/materials/languages translated to English via lookup
    - JSONL: list-valued columns stay lists
    - TSV:   list-valued columns are pipe separated

FIELD COVERAGE:
    This captures every descriptive field the current EDCS exposes for a
    monument and its inscriptions: identity, place and coordinates, material,
    citations, dating, text, language, category, comments and images.

    Two categories of field are deliberately absent because the current API has
    no equivalent. External database cross-links (partner records, Trismegistos
    place ids) were rendered only on the retired PHP pages and appear nowhere in
    the present application. Free-form HTML remnants are meaningless against a
    structured JSON API.

READING IN PYTHON:
    import pandas as pd
    df = pd.read_json("app/data/raw/edcs_inscriptions.jsonl.gz", lines=True)

RUN (from project root):
    python -m app.cli.scrape                   # full harvest, resumes if interrupted
    python -m app.cli.scrape --limit 500       # first 500 monuments (smoke test)
    python -m app.cli.scrape --restart         # discard existing output and start over
    python -m app.cli.scrape --workers 8       # gentler on the server
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from app.config.settings import CHUNK_SIZE, MONUMENT_URL
from app.core.edcs_parsing import parse_monument
from app.services.ports import EdcsClient, HarvestStore, Log














# ─── HARVEST ──────────────────────────────────────────────────────────────────


def harvest(
    session_factory,
    ids,
    lookup,
    workers,
    start_cursor,
    monuments_done,
    rows_written,
    *,
    client: EdcsClient,
    store: HarvestStore,
    log: Log,
):
    is_resume = start_cursor > 0

    # Refuses to truncate non-empty output unless resuming (see CorpusWriter).
    writer = store.open_corpus_writer(is_resume)

    failed = store.load_failed() if is_resume else set()
    sessions = [session_factory() for _ in range(workers)]
    total = len(ids)
    cursor = start_cursor
    started = time.time()

    def fetch_one(task):
        slot, monument_id = task
        id8 = f"{int(monument_id):08d}"
        url = MONUMENT_URL.format(shard=id8[:3], id8=id8)
        try:
            return monument_id, client.get_json(sessions[slot % workers], url, timeout=60)
        except Exception:
            return monument_id, None

    def write_payload(monument_id, payload):
        """Write one monument's rows. Returns the number of rows written."""
        written = 0
        for row in parse_monument(payload, monument_id, lookup):
            writer.write_row(row)
            written += 1
        return written

    log(f"\n[+] Harvesting {total - cursor:,} of {total:,} monuments")
    log(f"[+] Workers: {workers} | chunk: {CHUNK_SIZE}")
    log("[+] Ctrl+C is safe — progress is checkpointed after every chunk\n")

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Retry anything that failed on an earlier run BEFORE moving on.
            # The cursor has already advanced past these ids, so without this
            # pass they would never be revisited and the harvest would quietly
            # be short by however many failed. This is the path that matters
            # after a laptop sleep, which kills every in-flight connection.
            if failed:
                retry_ids = sorted(failed)
                log(f"[+] Retrying {len(retry_ids):,} monuments that failed earlier...")
                for offset in range(0, len(retry_ids), CHUNK_SIZE):
                    batch = retry_ids[offset : offset + CHUNK_SIZE]
                    for monument_id, payload in pool.map(fetch_one, enumerate(batch)):
                        if payload is None:
                            continue
                        rows_written += write_payload(monument_id, payload)
                        monuments_done += 1
                        failed.discard(monument_id)
                    writer.flush()
                    store.save_checkpoint(cursor, monuments_done, rows_written)
                    store.save_failed(failed)
                recovered = len(retry_ids) - len(failed)
                log(f"[+] Recovered {recovered:,}; {len(failed):,} still failing\n")

            while cursor < total:
                chunk = ids[cursor : cursor + CHUNK_SIZE]
                tasks = list(enumerate(chunk))

                for monument_id, payload in pool.map(fetch_one, tasks):
                    if payload is None:
                        failed.add(monument_id)
                        continue
                    rows_written += write_payload(monument_id, payload)
                    monuments_done += 1

                cursor += len(chunk)
                writer.flush()
                store.save_checkpoint(cursor, monuments_done, rows_written)
                store.save_failed(failed)

                elapsed = time.time() - started
                done_now = cursor - start_cursor
                rate = done_now / elapsed if elapsed > 0 else 0
                remaining = (total - cursor) / rate / 60 if rate > 0 else 0
                log(
                    f"  {cursor:>7,}/{total:,} ({cursor / total * 100:5.1f}%) | "
                    f"rows={rows_written:>8,} | failed={len(failed):>4,} | "
                    f"{rate:5.1f} mon/s | ~{remaining:.0f} min left"
                )

    except KeyboardInterrupt:
        log(f"\n[!] Interrupted at {cursor:,}. Run again to resume.")

    finally:
        writer.close()
        store.save_checkpoint(cursor, monuments_done, rows_written)
        store.save_failed(failed)

    return cursor, monuments_done, rows_written, failed


# ─── MAIN ─────────────────────────────────────────────────────────────────────




def run_harvest(
    limit,
    workers,
    restart,
    refresh_index,
    *,
    client: EdcsClient,
    store: HarvestStore,
    log: Log,
):
    """The scraper's use case: fetch the index and lookups, then harvest or resume.

    Arguments mirror the command line: --limit, --workers, --restart,
    --refresh-index.
    """
    store.ensure_dirs()

    if restart:
        store.discard_outputs()
        log("[+] Restart: previous output removed.")

    session = client.make_session()

    try:
        ids, index_fetched_at = client.fetch_index(session, use_cache=not refresh_index)
    except RuntimeError as exc:
        log(f"[!] Could not fetch the EDCS index: {exc}")
        log("[!] The endpoint or its schema may have changed again — see docs/EDCS_API.md")
        sys.exit(1)

    if limit:
        ids = ids[:limit]
        log(f"[+] --limit {limit}: harvesting a subset")

    try:
        lookup = client.build_lookup(session)
    except RuntimeError as exc:
        log(f"[!] Could not fetch lookup tables: {exc}")
        sys.exit(1)
    store.save_lookup(lookup)

    checkpoint = store.load_checkpoint()
    manifest = store.load_manifest()
    cursor = monuments_done = rows_written = 0

    # A completed harvest deletes its checkpoint, so the manifest is what tells
    # us the corpus on disk is already whole. Without this check a plain re-run
    # would fall through with cursor=0 and truncate the output.
    if manifest and not checkpoint and not restart:
        log(f"[✓] Harvest already complete — {manifest['monuments']:,} monuments, "
            f"{manifest['rows']:,} rows, finished {manifest['completed_at']}.")
        log(f"[✓] Index snapshot      : {manifest.get('index_fetched_at', '?')}")
        if manifest.get("failed"):
            log(f"[!] {manifest['failed']:,} monuments failed — see {store.FAILED_FILE}")
        log("\n    Nothing to do. Use --restart to re-harvest from scratch "
            "(this DELETES the existing corpus),")
        log("    or --refresh-index to check whether EDCS has grown since.")
        return

    if checkpoint and not restart:
        cursor = checkpoint.get("cursor", 0)
        monuments_done = checkpoint.get("monuments_done", 0)
        rows_written = checkpoint.get("rows_written", 0)
        if cursor >= len(ids):
            log(f"[✓] Already complete — {monuments_done:,} monuments, "
                f"{rows_written:,} rows. Use --restart to re-harvest.")
            return
        log(f"[resume] Checkpoint at monument {cursor:,} "
            f"({rows_written:,} rows already written)")

    started = time.time()
    cursor, monuments_done, rows_written, failed = harvest(
        client.make_session,
        ids,
        lookup,
        workers,
        cursor,
        monuments_done,
        rows_written,
        client=client,
        store=store,
        log=log,
    )
    duration = time.time() - started

    log(f"\n[✓] Monuments harvested : {monuments_done:,}")
    log(f"[✓] Inscription rows    : {rows_written:,}")
    log(f"[✓] Failed monuments    : {len(failed):,}")
    log(f"[✓] Duration            : {duration / 60:.1f} min")
    log(f"[✓] Index fetched at    : {index_fetched_at}")

    if failed:
        log(f"[!] {len(failed):,} monuments could not be fetched — ids in {store.FAILED_FILE}")
        log("[!] Harvest is INCOMPLETE. Re-run to retry them.")
    elif cursor >= len(ids):
        store.compress_outputs()
        store.save_manifest(monuments_done, rows_written, index_fetched_at, duration, failed)
        store.clear_checkpoint()
        log(f"[✓] Checkpoint cleared — full harvest complete. Manifest: {store.MANIFEST}")

    log(f"\n[✓] JSONL  : {store.OUTPUT_JSONL}")
    log(f"[✓] TSV    : {store.OUTPUT_TSV}")
    log(f"[✓] Lookup : {store.LOOKUP_FILE}")

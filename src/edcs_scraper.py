"""
EDCS Scraper — edcs.hist.uzh.ch
================================
Harvests all inscriptions from the Epigraphik-Datenbank Clauss / Slaby.

Rewritten 2026-08-19 for the EDCS release of 2026-08-07, which withdrew the
DataTables endpoint `/api/query` (now 403) and replaced it with a static-file
architecture. See docs/EDCS_API.md for the full contract.

FOLDER STRUCTURE:
    EpigCorpus/
    ├── data/
    │   ├── edcs_inscriptions.jsonl     ← one record per line
    │   ├── edcs_inscriptions.tsv
    │   ├── edcs_lookup.json            ← materials / provinces / categories / languages
    │   ├── edcs_checkpoint.json        ← resume cursor
    │   └── edcs_failed_ids.json        ← monuments that could not be fetched
    └── src/
        └── edcs_scraper.py             ← this file

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
    df = pd.read_json("data/edcs_inscriptions.jsonl", lines=True)

RUN (from project root):
    python src/edcs_scraper.py                 # full harvest, resumes if interrupted
    python src/edcs_scraper.py --limit 500     # first 500 monuments (smoke test)
    python src/edcs_scraper.py --restart       # discard existing output and start over
    python src/edcs_scraper.py --workers 8     # gentler on the server
"""

import argparse
import csv
import gzip
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# ─── PATHS ────────────────────────────────────────────────────────────────────
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")

OUTPUT_JSONL = os.path.join(DATA_DIR, "edcs_inscriptions.jsonl")
OUTPUT_TSV = os.path.join(DATA_DIR, "edcs_inscriptions.tsv")
LOOKUP_FILE = os.path.join(DATA_DIR, "edcs_lookup.json")
CHECKPOINT = os.path.join(DATA_DIR, "edcs_checkpoint.json")
FAILED_FILE = os.path.join(DATA_DIR, "edcs_failed_ids.json")
INDEX_CACHE = os.path.join(DATA_DIR, "edcs_index_cache.json")
MANIFEST = os.path.join(DATA_DIR, "edcs_harvest_manifest.json")

# ─── API CONFIG ───────────────────────────────────────────────────────────────
BASE = "https://edcs.hist.uzh.ch"
INDEX_URL = f"{BASE}/data/indexes/searchable.json"
LOOKUPS_URL = f"{BASE}/data/indexes/lookups.json"
PLACES_URL = f"{BASE}/data/indexes/places.json"
LANGUAGES_URL = f"{BASE}/data/indexes/languages.json"
SOURCES_URL = f"{BASE}/data/indexes/sources.json"
MONUMENT_URL = f"{BASE}/data/monument/{{shard}}/{{id8}}.json"
IMAGE_URL = f"{BASE}/bilder/{{prefix}}/{{filename}}"

EPIGCORPUS_VERSION = "0.2.0"

# An identifying User-Agent. Verified 2026-08-19: the static /data/ endpoints
# serve this without complaint (300/300 and 600/600 in survey runs), so there is
# no access reason to spoof a browser. The old scraper's forged Firefox UA,
# Referer and X-Requested-With bought nothing — six header variants all got the
# same 403 from the withdrawn /api/query.
HEADERS = {
    "User-Agent": (
        f"EpigCorpus/{EPIGCORPUS_VERSION} "
        "(+https://github.com/d-sanoj/EpigCorpus; contact.sanoj.d@gmail.com)"
    ),
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}

DEFAULT_WORKERS = 16
CHUNK_SIZE = 2000
MAX_RETRIES = 4
BACKOFF_BASE = 2.0

# ─── OUTPUT COLUMNS ───────────────────────────────────────────────────────────
# The first block is the original EpigCorpus schema, unchanged, so main.py,
# edcs_cleaner.py and edcs_streamlit_map.py keep working untouched.
# The second block adds the remaining descriptive fields plus record-level provenance.
TSV_FIELDS = [
    # — original EpigCorpus columns —
    "record_id",
    "edcs_id",
    "inscription_index",
    "province",
    "place",
    "latitude",
    "longitude",
    "material",
    "material_en",
    "not_before",
    "not_after",
    "inscription_text",
    "language",
    "category",
    "category_en",
    "belege",
    "image_urls",
    # — additional descriptive fields —
    "publication",      # citations as a single display string
    "raw_dating",       # human-readable dating range
    "dating_from",      # earliest year (negative = BC)
    "dating_to",        # latest year (negative = BC)
    "status",           # inscription genus / personal status
    "comment",          # editorial comments on the monument
    "photo",            # resolved image URLs
    "language_codes",
    "photo_credits",
    # — provenance —
    "retrieved_at",
    "source_url",
]

LIST_COLUMNS = {"category", "category_en", "belege", "status", "language_codes"}

_print_lock = threading.Lock()


def log(message):
    with _print_lock:
        print(message, flush=True)


def install_signal_handlers():
    """Make SIGTERM stop the harvest as cleanly as Ctrl+C does.

    Background jobs started from a non-interactive shell inherit SIGINT set to
    ignore, so `kill -INT` does nothing and the only way to stop a detached run
    is SIGTERM. Without this, SIGTERM kills the process mid-chunk and leaves the
    output files ahead of the checkpoint, which produces duplicate rows on the
    next resume. Raising KeyboardInterrupt routes SIGTERM into the same handler
    that already flushes and checkpoints.
    """

    def handler(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, handler)


# ─── HTTP ─────────────────────────────────────────────────────────────────────


def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount("https://", adapter)
    return session


def get_json(session, url, timeout=300):
    """GET with exponential backoff. Honours Retry-After when the server sends it."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                wait = float(response.headers.get("Retry-After", BACKOFF_BASE**attempt))
                time.sleep(wait)
                last_error = f"HTTP {response.status_code}"
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE**attempt)
    raise RuntimeError(f"{url} failed after {MAX_RETRIES} attempts — {last_error}")


# ─── LOOKUPS ──────────────────────────────────────────────────────────────────


def build_lookup(session):
    """Fetch and flatten every code table into a single lookup dict."""
    log("[+] Fetching lookup tables...")
    lookups = get_json(session, LOOKUPS_URL)["d"]
    languages = get_json(session, LANGUAGES_URL)["d"]
    places_raw = get_json(session, PLACES_URL)
    sources_raw = get_json(session, SOURCES_URL)["d"]

    # materials: index = id, entry = [token, {de,en,...}]; slot 0 is null
    materials = {}
    for idx, entry in enumerate(lookups.get("materials", [])):
        if entry:
            materials[idx] = {"token": entry[0], "en": entry[1].get("en", entry[0])}

    # categories: [bit, token, {de,en,...}], keyed by bit
    categories = {}
    for entry in lookups.get("categories", []):
        if isinstance(entry, list) and len(entry) >= 3:
            categories[entry[0]] = {"token": entry[1], "en": entry[2].get("en", entry[1])}

    provinces = lookups.get("provinces", [])

    langs = {}
    for entry in languages:
        if isinstance(entry, dict):
            langs[entry.get("sprache_id")] = {
                "en": entry.get("names", {}).get("en", ""),
                "code": (entry.get("kuerzel") or "").strip(),
            }

    # places: [geo_id, ort, province_index, [lat, lon]]
    # NOTE: coord is [latitude, longitude] here. The OLD API returned
    # [longitude, latitude]. Getting this backwards transposes the whole corpus.
    places = {}
    for row in places_raw.get("d", []):
        coord = row[3] if len(row) > 3 else None
        province_idx = row[2] if len(row) > 2 else None
        places[row[0]] = {
            "ort": row[1] or "",
            "province": (
                provinces[province_idx]
                if isinstance(province_idx, int) and 0 <= province_idx < len(provinces)
                else ""
            ),
            "lat": coord[0] if coord and len(coord) == 2 else "",
            "lon": coord[1] if coord and len(coord) == 2 else "",
        }

    # sources: index = id, entry = [token, description, count]
    sources = {}
    for idx, entry in enumerate(sources_raw):
        if entry:
            sources[idx] = entry[0]

    lookup = {
        "materials": materials,
        "categories": categories,
        "provinces": provinces,
        "languages": langs,
        "places": places,
        "sources": sources,
        "_meta": {
            "retrieved_at": now_iso(),
            "epigcorpus_version": EPIGCORPUS_VERSION,
        },
    }
    log(
        f"    materials={len(materials)} categories={len(categories)} "
        f"provinces={len(provinces)} languages={len(langs)} "
        f"places={len(places)} sources={len(sources)}"
    )
    return lookup


def save_lookup(lookup):
    with open(LOOKUP_FILE, "w", encoding="utf-8") as handle:
        json.dump(lookup, handle, ensure_ascii=False, indent=2)


# ─── PARSING ──────────────────────────────────────────────────────────────────


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def format_citation(entry, sources):
    """[source_id, volume, number] -> 'CIL-06, *00226'.

    Mirrors the site's own `citationLabel()` (app.js) exactly: source name,
    then '-volume' if a volume is present, then ', number'. Matching EDCS's
    rendering keeps our `publication` column joinable against theirs.

    The '*' prefix EDCS uses to mark falsae is preserved verbatim -- it is the
    only forgery marker the API exposes (see docs/AUDIT.md F1).
    """
    if not isinstance(entry, list) or not entry:
        return ""
    name = sources.get(entry[0], str(entry[0]))
    value = str(name)
    volume = str(entry[1]).strip() if len(entry) > 1 and entry[1] else ""
    detail = str(entry[2]).strip() if len(entry) > 2 and entry[2] else ""
    if volume:
        value += f"-{volume}"
    if detail:
        value += f", {detail}"
    return value.strip()


def image_url(filename):
    clean = str(filename or "").strip()
    if not clean or len(clean) < 2:
        return ""
    return IMAGE_URL.format(prefix=clean[:2], filename=clean)


def format_dating(not_before, not_after):
    """Human-readable dating, matching the site's own `formatDatingRange()`.

    '-' when undated, a bare year when both bounds agree, otherwise
    'from .. to' with '?' for an open bound. Negative years are BC.
    """
    has_before = not_before not in ("", None)
    has_after = not_after not in ("", None)
    if not has_before and not has_after:
        return "-"
    start = str(not_before) if has_before else "?"
    end = str(not_after) if has_after else "?"
    if start == end:
        return start
    return f"{start} .. {end}"


def parse_monument(payload, monument_id, lookup):
    """One monument payload -> a list of rows, one per inscription."""
    obj = payload.get("d", payload) if isinstance(payload, dict) else {}
    if not isinstance(obj, dict):
        return []

    edcs_id = f"EDCS-{int(monument_id):08d}"
    source_url = MONUMENT_URL.format(
        shard=f"{int(monument_id):08d}"[:3], id8=f"{int(monument_id):08d}"
    )
    retrieved_at = now_iso()

    # ── Place / province / coordinates ──
    geo_id = obj.get("g")
    place_info = lookup["places"].get(geo_id, {}) if geo_id is not None else {}
    province = place_info.get("province", "")
    place = place_info.get("ort", "")
    latitude = place_info.get("lat", "")
    longitude = place_info.get("lon", "")

    # ── Material ──
    material_id = obj.get("m")
    material_info = lookup["materials"].get(material_id, {})
    material = material_info.get("token", "")
    material_en = material_info.get("en", "")

    # ── Citations (`belege` as a list, `publication` as a string) ──
    sources = lookup["sources"]
    belege = [format_citation(entry, sources) for entry in (obj.get("q") or [])]
    belege = [b for b in belege if b]
    publication = " | ".join(belege)

    # ── Photos ──
    photos, credits = [], []
    for photo in obj.get("p") or []:
        if isinstance(photo, dict):
            url = image_url(photo.get("b"))
            if url:
                photos.append(url)
            if photo.get("u"):
                credits.append(str(photo["u"]))
    image_urls = " | ".join(photos)
    photo_credits = " | ".join(dict.fromkeys(credits))

    # ── Comments (absent from the old API) ──
    comments = []
    for comment in obj.get("c") or []:
        if isinstance(comment, dict):
            text = comment.get("k") or ""
            note = comment.get("n")
            comments.append(f"{text} ({note})" if note else str(text))
        elif comment:
            comments.append(str(comment))
    comment_text = " | ".join(c for c in comments if c)

    shared = {
        "edcs_id": edcs_id,
        "province": province,
        "place": place,
        "latitude": latitude,
        "longitude": longitude,
        "material": material,
        "material_en": material_en,
        "belege": belege,
        "publication": publication,
        "image_urls": image_urls,
        "photo": image_urls,
        "photo_credits": photo_credits,
        "comment": comment_text,
        "retrieved_at": retrieved_at,
        "source_url": source_url,
    }

    rows = []
    for index, inscription in enumerate(obj.get("i") or []):
        if not isinstance(inscription, dict):
            continue

        text = inscription.get("t") or ""

        dating = inscription.get("d") or []
        not_before = dating[0] if len(dating) > 0 and dating[0] is not None else ""
        not_after = dating[1] if len(dating) > 1 and dating[1] is not None else ""

        language_ids = inscription.get("s") or []
        language_names, language_codes = [], []
        for language_id in language_ids:
            info = lookup["languages"].get(language_id, {})
            if info.get("en"):
                language_names.append(info["en"])
            if info.get("code"):
                language_codes.append(info["code"])

        category_ids = inscription.get("g") or []
        category, category_en = [], []
        for category_id in category_ids:
            info = lookup["categories"].get(category_id, {})
            category.append(info.get("token", str(category_id)))
            category_en.append(info.get("en", str(category_id)))

        rows.append(
            {
                **shared,
                "record_id": f"{edcs_id}-{index}",
                "inscription_index": index,
                "not_before": not_before,
                "not_after": not_after,
                "dating_from": not_before,
                "dating_to": not_after,
                "raw_dating": format_dating(not_before, not_after),
                "inscription_text": text,
                "language": ", ".join(language_names),
                "language_codes": language_codes,
                "category": category,
                "category_en": category_en,
                "status": category_en,
            }
        )

    # Monument with no inscriptions — still record it, as the original did.
    if not rows:
        rows.append(
            {
                **shared,
                "record_id": f"{edcs_id}-0",
                "inscription_index": 0,
                "not_before": "",
                "not_after": "",
                "dating_from": "",
                "dating_to": "",
                "raw_dating": "",
                "inscription_text": "",
                "language": "",
                "language_codes": [],
                "category": [],
                "category_en": [],
                "status": [],
            }
        )

    return rows


# ─── CHECKPOINT / FAILURES ────────────────────────────────────────────────────


def save_checkpoint(cursor, monuments_done, rows_written):
    with open(CHECKPOINT, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "cursor": cursor,
                "monuments_done": monuments_done,
                "rows_written": rows_written,
                "updated_at": now_iso(),
            },
            handle,
            indent=2,
        )


def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return None
    with open(CHECKPOINT, encoding="utf-8") as handle:
        return json.load(handle)


def save_manifest(monuments_done, rows_written, index_fetched_at, duration_s, failed):
    """Record that a harvest finished.

    The checkpoint is deleted on completion, so it cannot double as the
    completion marker -- its absence would otherwise be indistinguishable from
    "never run", which is what makes a blind re-run destructive.
    """
    with open(MANIFEST, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "completed_at": now_iso(),
                "monuments": monuments_done,
                "rows": rows_written,
                "failed": len(failed),
                "index_fetched_at": index_fetched_at,
                "duration_seconds": round(duration_s, 1),
                "epigcorpus_version": EPIGCORPUS_VERSION,
            },
            handle,
            indent=2,
        )


def load_manifest():
    if not os.path.exists(MANIFEST):
        return None
    try:
        with open(MANIFEST, encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        return None


def compress_outputs():
    """Write .gz copies of the corpus for distribution.

    The harvest itself writes plain text because appending to a gzip stream
    across a checkpointed resume is not safe. Compression therefore runs once,
    at the end, on the finished files. Only the .gz copies are committed --
    the corpus is ~11x smaller compressed, which keeps it under GitHub's
    100 MB per-file limit without Git LFS. Loaders read either form, so no
    manual decompression step is ever required.
    """
    for source in (OUTPUT_JSONL, OUTPUT_TSV):
        if not os.path.exists(source):
            continue
        target = source + ".gz"
        raw = os.path.getsize(source)
        log(f"[+] Compressing {os.path.basename(source)} ({raw / 1048576:.1f} MB)...")
        with open(source, "rb") as src, gzip.open(target, "wb", compresslevel=9) as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)
        packed = os.path.getsize(target)
        log(f"    -> {os.path.basename(target)} "
            f"({packed / 1048576:.1f} MB, {raw / packed:.1f}x smaller)")


def save_failed(failed):
    with open(FAILED_FILE, "w", encoding="utf-8") as handle:
        json.dump({"count": len(failed), "ids": sorted(failed)}, handle, indent=2)


def load_failed():
    if not os.path.exists(FAILED_FILE):
        return set()
    with open(FAILED_FILE, encoding="utf-8") as handle:
        return set(json.load(handle).get("ids", []))


# ─── INDEX ────────────────────────────────────────────────────────────────────


def fetch_index(session, use_cache=True):
    """Fetch the full corpus index. One request, ~19 MB, no pagination."""
    if use_cache and os.path.exists(INDEX_CACHE):
        with open(INDEX_CACHE, encoding="utf-8") as handle:
            cached = json.load(handle)
        log(f"[+] Using cached index: {len(cached['ids']):,} monuments "
            f"(fetched {cached['fetched_at']})")
        return cached["ids"], cached["fetched_at"]

    log("[+] Fetching corpus index (~19 MB)...")
    started = time.time()
    payload = get_json(session, INDEX_URL, timeout=600)
    ids = [row[0] for row in payload.get("d", [])]
    fetched_at = now_iso()
    log(f"[+] Index: {len(ids):,} monuments in {time.time() - started:.1f}s")

    with open(INDEX_CACHE, "w", encoding="utf-8") as handle:
        json.dump({"fetched_at": fetched_at, "count": len(ids), "ids": ids}, handle)
    return ids, fetched_at


# ─── HARVEST ──────────────────────────────────────────────────────────────────


def harvest(session_factory, ids, lookup, workers, start_cursor, monuments_done, rows_written):
    is_resume = start_cursor > 0

    # Guard: opening in "w" truncates. Only ever do that when there is nothing
    # to lose, or when the caller explicitly asked via --restart (which deletes
    # the files up front, so they will not exist here).
    if not is_resume:
        for path in (OUTPUT_JSONL, OUTPUT_TSV):
            if os.path.exists(path) and os.path.getsize(path) > 0:
                raise RuntimeError(
                    f"Refusing to overwrite existing output: {path} "
                    f"({os.path.getsize(path):,} bytes).\n"
                    "  To add to it,      run without --restart (needs a checkpoint).\n"
                    "  To re-harvest,     run with --restart (DELETES existing output).\n"
                    "  To keep it,        move the file aside first."
                )

    jsonl_file = open(OUTPUT_JSONL, "a" if is_resume else "w", encoding="utf-8")
    tsv_file = open(OUTPUT_TSV, "a" if is_resume else "w", encoding="utf-8", newline="")
    tsv_writer = csv.DictWriter(
        tsv_file, fieldnames=TSV_FIELDS, delimiter="\t", extrasaction="ignore"
    )
    if not is_resume:
        tsv_writer.writeheader()

    failed = load_failed() if is_resume else set()
    sessions = [session_factory() for _ in range(workers)]
    total = len(ids)
    cursor = start_cursor
    started = time.time()

    def fetch_one(task):
        slot, monument_id = task
        id8 = f"{int(monument_id):08d}"
        url = MONUMENT_URL.format(shard=id8[:3], id8=id8)
        try:
            return monument_id, get_json(sessions[slot % workers], url, timeout=60)
        except Exception:
            return monument_id, None

    def write_payload(monument_id, payload):
        """Write one monument's rows. Returns the number of rows written."""
        written = 0
        for row in parse_monument(payload, monument_id, lookup):
            jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            tsv_row = dict(row)
            for column in LIST_COLUMNS:
                if isinstance(tsv_row.get(column), list):
                    tsv_row[column] = " | ".join(str(v) for v in tsv_row[column])
            tsv_writer.writerow(tsv_row)
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
                    jsonl_file.flush()
                    tsv_file.flush()
                    save_checkpoint(cursor, monuments_done, rows_written)
                    save_failed(failed)
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
                jsonl_file.flush()
                tsv_file.flush()
                save_checkpoint(cursor, monuments_done, rows_written)
                save_failed(failed)

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
        jsonl_file.close()
        tsv_file.close()
        save_checkpoint(cursor, monuments_done, rows_written)
        save_failed(failed)

    return cursor, monuments_done, rows_written, failed


# ─── MAIN ─────────────────────────────────────────────────────────────────────


def build_parser():
    parser = argparse.ArgumentParser(description="Harvest EDCS inscriptions.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only harvest the first N monuments (smoke test).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent requests (default {DEFAULT_WORKERS}).")
    parser.add_argument("--restart", action="store_true",
                        help="Discard existing output and checkpoint, then start over.")
    parser.add_argument("--refresh-index", action="store_true",
                        help="Re-fetch the corpus index instead of using the cache.")
    return parser


def main():
    args = build_parser().parse_args()
    os.makedirs(DATA_DIR, exist_ok=True)
    install_signal_handlers()

    if args.restart:
        for path in (OUTPUT_JSONL, OUTPUT_TSV, CHECKPOINT, FAILED_FILE):
            if os.path.exists(path):
                os.remove(path)
        log("[+] Restart: previous output removed.")

    session = make_session()

    try:
        ids, index_fetched_at = fetch_index(session, use_cache=not args.refresh_index)
    except RuntimeError as exc:
        log(f"[!] Could not fetch the EDCS index: {exc}")
        log("[!] The endpoint or its schema may have changed again — see docs/EDCS_API.md")
        sys.exit(1)

    if args.limit:
        ids = ids[: args.limit]
        log(f"[+] --limit {args.limit}: harvesting a subset")

    try:
        lookup = build_lookup(session)
    except RuntimeError as exc:
        log(f"[!] Could not fetch lookup tables: {exc}")
        sys.exit(1)
    save_lookup(lookup)

    checkpoint = load_checkpoint()
    manifest = load_manifest()
    cursor = monuments_done = rows_written = 0

    # A completed harvest deletes its checkpoint, so the manifest is what tells
    # us the corpus on disk is already whole. Without this check a plain re-run
    # would fall through with cursor=0 and truncate the output.
    if manifest and not checkpoint and not args.restart:
        log(f"[✓] Harvest already complete — {manifest['monuments']:,} monuments, "
            f"{manifest['rows']:,} rows, finished {manifest['completed_at']}.")
        log(f"[✓] Index snapshot      : {manifest.get('index_fetched_at', '?')}")
        if manifest.get("failed"):
            log(f"[!] {manifest['failed']:,} monuments failed — see {FAILED_FILE}")
        log("\n    Nothing to do. Use --restart to re-harvest from scratch "
            "(this DELETES the existing corpus),")
        log("    or --refresh-index to check whether EDCS has grown since.")
        return

    if checkpoint and not args.restart:
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
        make_session, ids, lookup, args.workers, cursor, monuments_done, rows_written
    )
    duration = time.time() - started

    log(f"\n[✓] Monuments harvested : {monuments_done:,}")
    log(f"[✓] Inscription rows    : {rows_written:,}")
    log(f"[✓] Failed monuments    : {len(failed):,}")
    log(f"[✓] Duration            : {duration / 60:.1f} min")
    log(f"[✓] Index fetched at    : {index_fetched_at}")

    if failed:
        log(f"[!] {len(failed):,} monuments could not be fetched — ids in {FAILED_FILE}")
        log("[!] Harvest is INCOMPLETE. Re-run to retry them.")
    elif cursor >= len(ids):
        compress_outputs()
        save_manifest(monuments_done, rows_written, index_fetched_at, duration, failed)
        if os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
        log(f"[✓] Checkpoint cleared — full harvest complete. Manifest: {MANIFEST}")

    log(f"\n[✓] JSONL  : {OUTPUT_JSONL}")
    log(f"[✓] TSV    : {OUTPUT_TSV}")
    log(f"[✓] Lookup : {LOOKUP_FILE}")


if __name__ == "__main__":
    main()

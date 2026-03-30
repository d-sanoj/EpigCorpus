"""
EDCS Scraper — edcs.hist.uzh.ch
================================
Scrapes all inscriptions from the Epigraphy Database Clauss/Slaby.

FOLDER STRUCTURE:
    EDCS-Analytics/
    ├── data/
    │   ├── edcs_inscriptions.jsonl   ← one record per line
    │   ├── edcs_inscriptions.tsv
    │   └── edcs_lookup.json
    └── src/
        └── edcs_scraper.py           ← this file

KEY DESIGN DECISIONS:
    - One row per inscription (not per monument)
    - record_id = edcs_id + inscription index e.g. EDCS-00000001-0
    - dating, language, category all come from inside each inscription
    - all categories translated to English via lookup
    - image_urls stored as last column
    - JSONL: category and category_en as lists
    - TSV:   category and category_en pipe separated

LOGIC:
    1. Check if data files exist
    2. If not → fresh scrape
    3. If yes → check recordsTotal against local count
    4. If new records → scrape only new ones
    5. If no new records → print info and exit

READING IN PYTHON:
    import pandas as pd
    df = pd.read_json("data/edcs_inscriptions.jsonl", lines=True)

SETUP:
    pip install requests

RUN (from project root):
    python src/edcs_scraper.py
"""

import requests
import json
import csv
import time
import sys
import os
import re

# ─── PATHS ────────────────────────────────────────────────────────────────────
SRC_DIR      = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR  = os.path.dirname(SRC_DIR)
DATA_DIR     = os.path.join(PROJECT_DIR, "data")

OUTPUT_JSONL = os.path.join(DATA_DIR, "edcs_inscriptions.jsonl")
OUTPUT_TSV   = os.path.join(DATA_DIR, "edcs_inscriptions.tsv")
LOOKUP_FILE  = os.path.join(DATA_DIR, "edcs_lookup.json")
CHECKPOINT   = os.path.join(DATA_DIR, "edcs_checkpoint.json")

# ─── API CONFIG ───────────────────────────────────────────────────────────────
API_URL    = "https://edcs.hist.uzh.ch/api/query"
DELAY      = 1.5
PAGE_SIZES = [500, 100]

HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/120.0",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "en-US,en;q=0.5",
    "X-Requested-With": "XMLHttpRequest",
    "Referer":          "https://edcs.hist.uzh.ch/en/search",
}

# ─── 16 COLUMNS ───────────────────────────────────────────────────────────────
TSV_FIELDS = [
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
    "image_urls",
]

# ─── BUILD REQUEST PARAMS ─────────────────────────────────────────────────────

def build_params(draw, start, length):
    return {
        "draw":   draw,
        "start":  start,
        "length": length,

        "columns[0][data]":          "obj.edcs-id",
        "columns[0][name]":          "",
        "columns[0][searchable]":    "true",
        "columns[0][orderable]":     "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",

        "columns[1][data]":          "",
        "columns[1][name]":          "",
        "columns[1][searchable]":    "true",
        "columns[1][orderable]":     "true",
        "columns[1][search][value]": "",
        "columns[1][search][regex]": "false",

        "columns[2][data]":          "obj.inschriften",
        "columns[2][name]":          "",
        "columns[2][searchable]":    "true",
        "columns[2][orderable]":     "true",
        "columns[2][search][value]": "",
        "columns[2][search][regex]": "false",

        "columns[3][data]":          "obj.material",
        "columns[3][name]":          "",
        "columns[3][searchable]":    "true",
        "columns[3][orderable]":     "true",
        "columns[3][search][value]": "",
        "columns[3][search][regex]": "false",

        "columns[4][data]":          "obj.datierung",
        "columns[4][name]":          "",
        "columns[4][searchable]":    "true",
        "columns[4][orderable]":     "true",
        "columns[4][search][value]": "",
        "columns[4][search][regex]": "false",

        "columns[5][data]":          "obj.anzahl_bilder",
        "columns[5][name]":          "",
        "columns[5][searchable]":    "false",
        "columns[5][orderable]":     "false",
        "columns[5][search][value]": "",
        "columns[5][search][regex]": "false",

        "order[0][column]": "0",
        "order[0][dir]":    "asc",
        "order[0][name]":   "",

        "search[value]": "",
        "search[regex]": "false",
        "_":             int(time.time() * 1000),
    }

# ─── LOOKUP ───────────────────────────────────────────────────────────────────

def fetch_lookup(session):
    """Fetch one page just to extract the lookup dictionary."""
    params = build_params(draw=1, start=0, length=1)
    r = session.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("lookup", {})

def load_or_update_lookup(session):
    """
    Load lookup from file if exists.
    Fetch fresh from API and compare.
    Save only if changed.
    """
    fresh = fetch_lookup(session)

    if os.path.exists(LOOKUP_FILE):
        with open(LOOKUP_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)
        if existing == fresh:
            print("[lookup] No changes — using existing lookup file.")
            return existing
        else:
            print("[lookup] Changes detected — updating lookup file.")
    else:
        print("[lookup] No lookup file found — creating new one.")

    with open(LOOKUP_FILE, "w", encoding="utf-8") as f:
        json.dump(fresh, f, ensure_ascii=False, indent=2)
    return fresh

def get_material_en(lookup, code):
    """Translate material code to English."""
    if not code:
        return ""
    return lookup.get("material", {}).get(code, {}).get("en", code)

def translate_categories(lookup, cats):
    """
    Translate ALL category codes to English.
    Returns a list of English translations.
    Falls back to original term if not found in lookup.
    """
    if not cats or not isinstance(cats, list):
        return []
    gattung = lookup.get("gattung", {})
    return [
        gattung.get(cat, {}).get("en", cat)   # fallback to original if not found
        for cat in cats
    ]

# ─── PARSE ONE MONUMENT → MULTIPLE INSCRIPTION ROWS ──────────────────────────

def parse_monument(item, lookup):
    """
    One monument can have multiple inscriptions.
    Returns a LIST of rows — one per inscription.
    Each row has a unique record_id = edcs_id-inscription_index.
    """
    obj = item.get("obj", {})

    # ── Monument level fields (shared across all inscriptions) ──
    edcs_id  = obj.get("edcs-id", "")
    province = obj.get("provinz", "")
    place    = obj.get("ort", "")

    coord     = obj.get("coord") or []
    longitude = coord[0] if len(coord) > 0 else ""
    latitude  = coord[1] if len(coord) > 1 else ""

    material    = obj.get("material", "") or ""
    material_en = get_material_en(lookup, material)

    # ── Image URLs — shared across all inscriptions ──
    bilder     = obj.get("bilder") or []
    image_list = []
    for b in bilder:
        if isinstance(b, list) and b[0]:
            image_list.append(str(b[0]))
        elif isinstance(b, str) and b:
            image_list.append(b)
    image_urls = " | ".join(image_list)

    # ── Inscriptions — one row per inscription ──
    inschriften = obj.get("inschriften") or []
    rows        = []

    for i, insc in enumerate(inschriften):
        if not isinstance(insc, list):
            continue

        # index[0] → inscription text
        inscription_text = insc[0] if len(insc) > 0 else ""

        # index[1] → [not_before, not_after] — confirmed from JSON
        dating     = insc[1] if len(insc) > 1 else []
        not_before = ""
        not_after  = ""
        if isinstance(dating, list):
            not_before = dating[0] if len(dating) > 0 else ""
            not_after  = dating[1] if len(dating) > 1 else ""

        # index[2] → language list
        langs    = insc[2] if len(insc) > 2 else []
        language = ", ".join(langs) if isinstance(langs, list) else str(langs or "")

        # index[3] → category list — ALL values
        cats        = insc[3] if len(insc) > 3 else []
        category    = cats if isinstance(cats, list) else []
        category_en = translate_categories(lookup, category)

        rows.append({
            "record_id":         f"{edcs_id}-{i}",
            "edcs_id":           edcs_id,
            "inscription_index": i,
            "province":          province,
            "place":             place,
            "latitude":          latitude,
            "longitude":         longitude,
            "material":          material,
            "material_en":       material_en,
            "not_before":        not_before,
            "not_after":         not_after,
            "inscription_text":  inscription_text,
            "language":          language,
            "category":          category,       # list in JSONL
            "category_en":       category_en,    # list in JSONL
            "image_urls":        image_urls,
        })

    # If monument has no inschriften at all — still save one row
    if not rows:
        rows.append({
            "record_id":         f"{edcs_id}-0",
            "edcs_id":           edcs_id,
            "inscription_index": 0,
            "province":          province,
            "place":             place,
            "latitude":          latitude,
            "longitude":         longitude,
            "material":          material,
            "material_en":       material_en,
            "not_before":        "",
            "not_after":         "",
            "inscription_text":  "",
            "language":          "",
            "category":          [],
            "category_en":       [],
            "image_urls":        image_urls,
        })

    return rows

# ─── LOCAL FILE HELPERS ───────────────────────────────────────────────────────

def count_local_records():
    """Count monuments (not inscription rows) in JSONL by unique edcs_id."""
    if not os.path.exists(OUTPUT_JSONL):
        return 0
    seen = set()
    with open(OUTPUT_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                seen.add(rec.get("edcs_id", ""))
            except json.JSONDecodeError:
                continue
    return len(seen)

def get_last_edcs_int():
    """Get highest EDCS ID integer from existing JSONL."""
    if not os.path.exists(OUTPUT_JSONL):
        return 0
    last_int = 0
    with open(OUTPUT_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                eid = edcs_id_to_int(rec.get("edcs_id", "0"))
                if eid > last_int:
                    last_int = eid
            except json.JSONDecodeError:
                continue
    return last_int

def edcs_id_to_int(edcs_id):
    m = re.search(r'\d+', str(edcs_id))
    return int(m.group()) if m else 0

# ─── CHECKPOINT ───────────────────────────────────────────────────────────────

def save_checkpoint(start, last_edcs_id):
    with open(CHECKPOINT, "w") as f:
        json.dump({
            "start":         start,
            "last_edcs_id":  last_edcs_id,
            "last_edcs_int": edcs_id_to_int(last_edcs_id),
        }, f, indent=2)

def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, "r") as f:
            cp = json.load(f)
        print(f"[resume] Checkpoint found — last EDCS ID: {cp['last_edcs_id']} | start: {cp['start']}")
        return cp["start"], cp["last_edcs_int"]
    return None, None

# ─── CORE SCRAPE LOOP ─────────────────────────────────────────────────────────

def scrape(session, lookup, start, last_edcs_int, total, page_size, is_resume):
    jsonl_file = open(OUTPUT_JSONL, "a", encoding="utf-8")
    tsv_file   = open(OUTPUT_TSV, "a" if is_resume else "w", encoding="utf-8", newline="")
    tsv_writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t", extrasaction="ignore")

    if not is_resume:
        tsv_writer.writeheader()

    monuments_saved  = 0
    rows_saved       = 0
    draw             = 1
    last_edcs_id     = f"EDCS-{last_edcs_int:08d}" if last_edcs_int else ""

    print(f"\n[+] Starting from offset  : {start:,}")
    print(f"[+] Total records in EDCS : {total:,}")
    print(f"[+] Page size             : {page_size}")
    print(f"[+] Press Ctrl+C anytime  — progress saved every page\n")

    try:
        while start < total:
            params = build_params(draw=draw, start=start, length=page_size)
            try:
                r = session.get(API_URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except requests.exceptions.RequestException as e:
                print(f"\n[!] Network error at start={start}: {e}. Retrying in 15s...")
                time.sleep(15)
                continue
            except json.JSONDecodeError as e:
                print(f"\n[!] Bad JSON at start={start}: {e}. Skipping page.")
                start += page_size
                draw  += 1
                continue

            records = data.get("data", [])
            if not records:
                print(f"\n[=] Empty page at start={start}. Done.")
                break

            for item in records:
                edcs_id = item.get("obj", {}).get("edcs-id", "")
                eid_int = edcs_id_to_int(edcs_id)

                # Skip already saved monuments on resume
                if is_resume and eid_int <= last_edcs_int:
                    continue

                # Parse monument into one or more inscription rows
                rows = parse_monument(item, lookup)

                for row in rows:
                    # JSONL — category and category_en as lists
                    jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")

                    # TSV — lists joined with pipe
                    tsv_row = row.copy()
                    if isinstance(tsv_row["category"], list):
                        tsv_row["category"] = " | ".join(tsv_row["category"])
                    if isinstance(tsv_row["category_en"], list):
                        tsv_row["category_en"] = " | ".join(tsv_row["category_en"])
                    tsv_writer.writerow(tsv_row)

                    rows_saved += 1

                monuments_saved += 1
                last_edcs_id     = edcs_id
                last_edcs_int    = eid_int

            # Save checkpoint after every page
            save_checkpoint(start + page_size, last_edcs_id)

            # Progress
            pct     = min(100.0, (start + page_size) / total * 100)
            est_min = int(((total - start - page_size) / page_size) * DELAY / 60)
            print(
                f"  offset={start + page_size:>7,}/{total:,} | "
                f"monuments={monuments_saved:>7,} | "
                f"rows={rows_saved:>7,} | "
                f"{pct:5.1f}% | "
                f"~{est_min}min left      ",
                end="\r"
            )

            start += page_size
            draw  += 1
            time.sleep(DELAY)

    except KeyboardInterrupt:
        print(f"\n\n[!] Stopped. Run again to resume from {last_edcs_id}.")

    finally:
        jsonl_file.close()
        tsv_file.close()

    print(f"\n[✓] Monuments saved : {monuments_saved:,}")
    print(f"[✓] Total rows      : {rows_saved:,}  (more than monuments due to multi-inscription records)")
    print(f"[✓] Last EDCS ID    : {last_edcs_id}")

    if start >= total and os.path.exists(CHECKPOINT):
        os.remove(CHECKPOINT)
        print(f"[✓] Checkpoint deleted — full scrape complete!")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── Step 1: Connect and detect page size ──
    page_size = None
    total     = None
    print("[+] Connecting to EDCS API...")

    for size in PAGE_SIZES:
        try:
            params = build_params(draw=1, start=0, length=size)
            r      = session.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "data" in data and len(data["data"]) > 0:
                page_size = size
                total     = data["recordsTotal"]
                print(f"[+] Connected. Page size {size} works. Total in EDCS: {total:,}")
                break
            else:
                print(f"[!] Page size {size} returned no data, trying smaller...")
        except Exception as e:
            print(f"[!] Page size {size} failed: {e}")
        time.sleep(DELAY)

    if not page_size or not total:
        print("[!] Could not connect to EDCS API. Check your internet connection.")
        sys.exit(1)

    # ── Step 2: Load or update lookup ──
    print()
    lookup = load_or_update_lookup(session)

    # ── Step 3: Check local data files ──
    print()
    files_exist = os.path.exists(OUTPUT_JSONL) and os.path.exists(OUTPUT_TSV)

    if not files_exist:
        print("[+] No existing data files found — starting fresh scrape.")
        scrape(
            session       = session,
            lookup        = lookup,
            start         = 0,
            last_edcs_int = 0,
            total         = total,
            page_size     = page_size,
            is_resume     = False,
        )

    else:
        # Check for interrupted scrape first
        cp_start, cp_last_int = load_checkpoint()

        if cp_start is not None:
            print(f"[+] Resuming interrupted scrape from offset {cp_start:,}...")
            scrape(
                session       = session,
                lookup        = lookup,
                start         = cp_start,
                last_edcs_int = cp_last_int,
                total         = total,
                page_size     = page_size,
                is_resume     = True,
            )

        else:
            # Compare local monument count vs API total
            print("[+] Data files found. Checking for updates...")
            local_count = count_local_records()
            print(f"    Local monuments : {local_count:,}")
            print(f"    EDCS total      : {total:,}")

            if total > local_count:
                new_count     = total - local_count
                last_edcs_int = get_last_edcs_int()
                print(f"[+] {new_count:,} new records found — scraping updates...")
                scrape(
                    session       = session,
                    lookup        = lookup,
                    start         = local_count,
                    last_edcs_int = last_edcs_int,
                    total         = total,
                    page_size     = page_size,
                    is_resume     = True,
                )
            else:
                print(f"\n[✓] No new records found — local data is up to date.")
                print(f"[✓] Local monuments : {local_count:,}")
                print(f"[✓] EDCS total      : {total:,}")

    print(f"\n[✓] JSONL  : {OUTPUT_JSONL}")
    print(f"[✓] TSV    : {OUTPUT_TSV}")
    print(f"[✓] Lookup : {LOOKUP_FILE}")


if __name__ == "__main__":
    main()
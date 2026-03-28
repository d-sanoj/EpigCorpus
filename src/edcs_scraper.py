"""
EDCS Scraper — edcs.hist.uzh.ch
================================
Scrapes all inscriptions from the Epigraphy Database Clauss/Slaby.

FOLDER STRUCTURE:
    EDCS-Analytics/
    ├── data/
    │   ├── edcs_inscriptions.json
    │   ├── edcs_inscriptions.tsv
    │   └── edcs_lookup.json
    └── src/
        └── edcs_scraper.py   ← this file

LOGIC:
    1. Check if data files exist
    2. If not → fresh scrape
    3. If yes → check recordsTotal against local count
    4. If new records exist → scrape only new ones
    5. If no new records → print info and exit

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
# Script is in src/ — data folder is one level up
SRC_DIR      = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR  = os.path.dirname(SRC_DIR)
DATA_DIR     = os.path.join(PROJECT_DIR, "data")

OUTPUT_JSON  = os.path.join(DATA_DIR, "edcs_inscriptions.json")
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

# ─── FINAL 14 COLUMNS ─────────────────────────────────────────────────────────
TSV_FIELDS = [
    "edcs_id",
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
    "references",
]

# ─── BUILD REQUEST PARAMS ─────────────────────────────────────────────────────

def build_params(draw, start, length):
    cache_buster = int(time.time() * 1000)
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
        "_":             cache_buster,
    }

# ─── LOOKUP HELPERS ───────────────────────────────────────────────────────────

def fetch_lookup(session):
    """Fetch one page just to extract the lookup dictionary."""
    params = build_params(draw=1, start=0, length=1)
    r = session.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("lookup", {})

def load_or_update_lookup(session):
    """
    Load lookup from file if exists.
    Fetch fresh copy from API and compare.
    Save only if changed.
    """
    fresh_lookup = fetch_lookup(session)

    if os.path.exists(LOOKUP_FILE):
        with open(LOOKUP_FILE, "r", encoding="utf-8") as f:
            existing_lookup = json.load(f)

        if existing_lookup == fresh_lookup:
            print("[lookup] No changes in lookup dictionary — using existing file.")
            return existing_lookup
        else:
            print("[lookup] Lookup dictionary updated — saving new version.")
            with open(LOOKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(fresh_lookup, f, ensure_ascii=False, indent=2)
            return fresh_lookup
    else:
        print("[lookup] No lookup file found — saving fresh copy.")
        with open(LOOKUP_FILE, "w", encoding="utf-8") as f:
            json.dump(fresh_lookup, f, ensure_ascii=False, indent=2)
        return fresh_lookup

def get_material_en(lookup, material_code):
    """Translate material code to English using lookup."""
    if not material_code:
        return ""
    material_dict = lookup.get("material", {})
    entry = material_dict.get(material_code, {})
    return entry.get("en", material_code)

def get_category_en(lookup, category_code):
    """Translate category code to English using lookup."""
    if not category_code:
        return ""
    gattung_dict = lookup.get("gattung", {})
    entry = gattung_dict.get(category_code, {})
    return entry.get("en", category_code)

# ─── PARSE ONE RECORD ─────────────────────────────────────────────────────────

def parse_record(item, lookup):
    obj = item.get("obj", {})

    # ── Coordinates ──
    coord     = obj.get("coord") or []
    longitude = coord[0] if len(coord) > 0 else ""
    latitude  = coord[1] if len(coord) > 1 else ""

    # ── Dating ──
    datierung = obj.get("datierung") or []
    not_before = datierung[0] if len(datierung) > 0 else ""
    not_after  = datierung[1] if len(datierung) > 1 else ""

    # ── Material ──
    material    = obj.get("material", "") or ""
    material_en = get_material_en(lookup, material)

    # ── Inscription text, language, category ──
    inscription_text = ""
    language         = ""
    category         = ""
    category_en      = ""

    inschriften = obj.get("inschriften") or []
    if inschriften:
        first = inschriften[0] if isinstance(inschriften[0], list) else inschriften

        # index[0] → inscription text
        inscription_text = first[0] if len(first) > 0 else ""

        # index[1] → date range (not text — confirmed from JSON)
        # index[2] → language list
        langs    = first[2] if len(first) > 2 else []
        language = ", ".join(langs) if isinstance(langs, list) else str(langs or "")

        # index[3] → category list — ALL values joined as a list
        cats = first[3] if len(first) > 3 else []
        if isinstance(cats, list) and cats:
            category    = cats  # keep as list for JSON
            # translate first item (main category) to English
            category_en = get_category_en(lookup, cats[0])

    # ── References — all belege joined as single string ──
    belege = obj.get("belege") or []
    refs   = []
    for b in belege:
        if isinstance(b, list):
            refs.append(" ".join(str(x) for x in b if x is not None))
        else:
            refs.append(str(b))
    references = " | ".join(refs)

    return {
        "edcs_id":          obj.get("edcs-id", ""),
        "province":         obj.get("provinz", ""),
        "place":            obj.get("ort", ""),
        "latitude":         latitude,
        "longitude":        longitude,
        "material":         material,
        "material_en":      material_en,
        "not_before":       not_before,
        "not_after":        not_after,
        "inscription_text": inscription_text,
        "language":         language,
        "category":         category,       # list in JSON
        "category_en":      category_en,
        "references":       references,
    }

# ─── LOCAL RECORD COUNT ───────────────────────────────────────────────────────

def count_local_records():
    """Count how many records are in the existing JSON file."""
    if not os.path.exists(OUTPUT_JSON):
        return 0
    count = 0
    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",")
            if not line or line in ("[", "]"):
                continue
            try:
                json.loads(line)
                count += 1
            except json.JSONDecodeError:
                continue
    return count

def get_last_edcs_int():
    """Get the highest EDCS ID integer from existing JSON file."""
    if not os.path.exists(OUTPUT_JSON):
        return 0
    last_int = 0
    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",")
            if not line or line in ("[", "]"):
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

# ─── SCRAPE ───────────────────────────────────────────────────────────────────

def scrape(session, lookup, start, last_edcs_int, total, page_size, is_resume):
    """Core scrape loop — writes to JSON and TSV."""

    json_file  = open(OUTPUT_JSON, "a" if is_resume else "w", encoding="utf-8")
    tsv_file   = open(OUTPUT_TSV,  "a" if is_resume else "w", encoding="utf-8", newline="")
    tsv_writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t", extrasaction="ignore")

    if not is_resume:
        json_file.write("[\n")
        tsv_writer.writeheader()

    first_record  = not is_resume
    records_saved = 0
    draw          = 1
    last_edcs_id  = f"EDCS-{last_edcs_int:08d}" if last_edcs_int else ""

    print(f"\n[+] Scraping from offset  : {start:,}")
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
                row     = parse_record(item, lookup)
                eid_int = edcs_id_to_int(row["edcs_id"])

                # Skip already saved records on resume
                if is_resume and eid_int <= last_edcs_int:
                    continue

                # Write JSON — category as list, rest as values
                if not first_record:
                    json_file.write(",\n")
                json.dump(row, json_file, ensure_ascii=False)
                first_record = False

                # Write TSV — category as pipe separated string
                tsv_row = row.copy()
                if isinstance(tsv_row["category"], list):
                    tsv_row["category"] = " | ".join(tsv_row["category"])
                tsv_writer.writerow(tsv_row)

                records_saved += 1
                last_edcs_id   = row["edcs_id"]
                last_edcs_int  = eid_int

            # Save checkpoint after every page
            save_checkpoint(start + page_size, last_edcs_id)

            # Progress
            pct     = min(100.0, (start + page_size) / total * 100)
            est_min = int(((total - start - page_size) / page_size) * DELAY / 60)
            print(
                f"  offset={start + page_size:>7,}/{total:,} | "
                f"saved={records_saved:>7,} | "
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
        json_file.write("\n]")
        json_file.close()
        tsv_file.close()

    print(f"\n[✓] Records saved : {records_saved:,}")
    print(f"[✓] Last EDCS ID  : {last_edcs_id}")

    if start >= total and os.path.exists(CHECKPOINT):
        os.remove(CHECKPOINT)
        print(f"[✓] Checkpoint deleted — full scrape complete!")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── Step 1: Detect working page size and get total ──
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
                print(f"[+] Connected. Page size {size} works. Total records in EDCS: {total:,}")
                break
            else:
                print(f"[!] Page size {size} returned no data, trying smaller...")
        except Exception as e:
            print(f"[!] Page size {size} failed: {e}")
        time.sleep(DELAY)

    if not page_size or not total:
        print("[!] Could not connect to EDCS API. Check your internet connection.")
        sys.exit(1)

    # ── Step 2: Load or update lookup dictionary ──
    print()
    lookup = load_or_update_lookup(session)

    # ── Step 3: Check local data files ──
    json_exists = os.path.exists(OUTPUT_JSON)
    tsv_exists  = os.path.exists(OUTPUT_TSV)
    files_exist = json_exists and tsv_exists

    print()
    if not files_exist:
        # ── Fresh scrape ──
        print("[+] No existing data files found in data/ folder.")
        print("[+] Starting fresh scrape...")
        scrape(
            session      = session,
            lookup       = lookup,
            start        = 0,
            last_edcs_int= 0,
            total        = total,
            page_size    = page_size,
            is_resume    = False,
        )

    else:
        # ── Check for resume checkpoint first ──
        cp_start, cp_last_int = load_checkpoint()

        if cp_start is not None:
            # Interrupted scrape — resume it
            print(f"[+] Resuming interrupted scrape from offset {cp_start:,}...")
            scrape(
                session      = session,
                lookup       = lookup,
                start        = cp_start,
                last_edcs_int= cp_last_int,
                total        = total,
                page_size    = page_size,
                is_resume    = True,
            )

        else:
            # ── Compare local count vs API total ──
            print("[+] Data files found. Checking for updates...")
            local_count = count_local_records()
            print(f"    Local records  : {local_count:,}")
            print(f"    EDCS total     : {total:,}")

            if total > local_count:
                new_records = total - local_count
                print(f"[+] {new_records:,} new records found. Scraping updates...")
                last_edcs_int = get_last_edcs_int()
                scrape(
                    session      = session,
                    lookup       = lookup,
                    start        = local_count,
                    last_edcs_int= last_edcs_int,
                    total        = total,
                    page_size    = page_size,
                    is_resume    = True,
                )
            else:
                print(f"\n[✓] No new records found.")
                print(f"[✓] Local data is up to date — {local_count:,} records.")
                print(f"[✓] Last checked: EDCS total = {total:,}")

    print(f"\n[✓] JSON : {OUTPUT_JSON}")
    print(f"[✓] TSV  : {OUTPUT_TSV}")
    print(f"[✓] Lookup: {LOOKUP_FILE}")


if __name__ == "__main__":
    main()
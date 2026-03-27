"""EDCS incremental scraper.

Project layout:
EDCS-Analytics/
    data/
        edcs_inscriptions.json
        edcs_inscriptions.tsv
    src/
        edcs_scraper.py

Behavior:
- If local data exists, only fetches and appends new rows from the website.
- If no local data exists, creates fresh JSON/TSV outputs.
- Checkpoint is used to resume interrupted runs safely.
"""

import csv
import json
import os
import re
import sys
import time

import requests

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_URL = "https://edcs.hist.uzh.ch/api/query"
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
OUTPUT_JSON = os.path.join(DATA_DIR, "edcs_inscriptions.json")
OUTPUT_TSV = os.path.join(DATA_DIR, "edcs_inscriptions.tsv")
CHECKPOINT = os.path.join(DATA_DIR, "edcs_checkpoint.json")
DELAY = 1.5
PAGE_SIZES = [500, 100]

HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/120.0",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "en-US,en;q=0.5",
    "X-Requested-With": "XMLHttpRequest",
    "Referer":          "https://edcs.hist.uzh.ch/en/search",
}

TSV_FIELDS = [
    "edcs_id", "monument_id", "province", "place",
    "latitude", "longitude", "material", "dating",
    "not_before", "not_after", "inscription_text",
    "inscription_text_original", "language", "category",
    "subcategory", "praenomen", "nomen", "cognomen",
    "references", "images", "image_count", "links",
]

# ─── BUILD REQUEST PARAMS ─────────────────────────────────────────────────────

def build_params(draw, start, length):
    """
    Exact column structure confirmed from Firefox Network tab:
      col 0 = obj.edcs-id
      col 1 = (obj.ort — blank name)
      col 2 = obj.inschriften
      col 3 = obj.material
      col 4 = obj.datierung
      col 5 = obj.anzahl_bilder
    """
    import time as t
    cache_buster = int(t.time() * 1000)

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

        "_": cache_buster,   # cache buster — same as browser sends
    }

# ─── PARSE ONE RECORD ─────────────────────────────────────────────────────────

def parse_record(item):
    obj = item.get("obj", {})

    row = {
        "edcs_id":                   obj.get("edcs-id", ""),
        "monument_id":               item.get("monument_id", ""),
        "province":                  obj.get("provinz", ""),
        "place":                     obj.get("ort", ""),
        "material":                  obj.get("material", ""),
        "dating":                    obj.get("datierung", "") or "",
        "image_count":               obj.get("anzahl_bilder", 0),
        "longitude":                 "",
        "latitude":                  "",
        "inscription_text":          "",
        "inscription_text_original": "",
        "language":                  "",
        "category":                  "",
        "subcategory":               "",
        "praenomen":                 "",
        "nomen":                     "",
        "cognomen":                  "",
        "not_before":                "",
        "not_after":                 "",
        "references":                "",
        "images":                    "",
        "links":                     "",
    }

    # Coordinates
    coord = obj.get("coord") or []
    if len(coord) > 0:
        row["longitude"] = coord[0]
    if len(coord) > 1:
        row["latitude"] = coord[1]

    # Inscription text
    # inschriften: [ [standard_text, original_text, [languages], [categories]], ... ]
    inschriften = obj.get("inschriften") or []
    if inschriften:
        first = inschriften[0] if isinstance(inschriften[0], list) else inschriften
        row["inscription_text"]          = first[0] if len(first) > 0 else ""
        row["inscription_text_original"] = first[1] if len(first) > 1 else ""

        langs = first[2] if len(first) > 2 else []
        row["language"] = "|".join(langs) if isinstance(langs, list) else str(langs or "")

        cats = first[3] if len(first) > 3 else []
        if isinstance(cats, list):
            row["category"]    = cats[0] if len(cats) > 0 else ""
            row["subcategory"] = "|".join(cats[1:]) if len(cats) > 1 else ""

    # Not before / not after from dating string
    dating_str = str(row["dating"])
    years = re.findall(r'-?\d{1,4}', dating_str)
    row["not_before"] = years[0] if len(years) > 0 else ""
    row["not_after"]  = years[1] if len(years) > 1 else ""

    # References (belege)
    belege = obj.get("belege") or []
    refs = []
    for b in belege:
        if isinstance(b, list):
            refs.append(" ".join(str(x) for x in b if x is not None))
        else:
            refs.append(str(b))
    row["references"] = " | ".join(refs)

    # Images
    bilder = obj.get("bilder") or []
    imgs = []
    for b in bilder:
        if isinstance(b, list) and b[0]:
            imgs.append(str(b[0]))
        elif isinstance(b, str):
            imgs.append(b)
    row["images"] = " | ".join(imgs)

    # Links
    links = obj.get("links") or []
    if isinstance(links, list):
        row["links"] = " | ".join(str(l) for l in links if l)
    elif links:
        row["links"] = str(links)

    return row

# ─── LOCAL DATA HELPERS ──────────────────────────────────────────────────────

def edcs_id_to_int(edcs_id):
    m = re.search(r'\d+', str(edcs_id))
    return int(m.group()) if m else 0

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def load_existing_state():
    """Returns (existing_count, max_edcs_int)."""
    ensure_data_dir()

    # TSV is the fastest/cleanest source of truth for existing rows.
    if os.path.exists(OUTPUT_TSV):
        print(f"[resume] Scanning existing TSV: {OUTPUT_TSV}")
        count = 0
        max_edcs_int = 0
        with open(OUTPUT_TSV, "r", encoding="utf-8", newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            for row in reader:
                count += 1
                eid_int = edcs_id_to_int(row.get("edcs_id", ""))
                if eid_int > max_edcs_int:
                    max_edcs_int = eid_int

        if count > 0:
            print(f"[resume] Existing rows: {count:,} | Last EDCS ID: EDCS-{max_edcs_int:08d}")
            return count, max_edcs_int

    # Fallback to line-by-line scan of JSON if TSV is missing.
    if os.path.exists(OUTPUT_JSON):
        print(f"[resume] TSV missing; scanning JSON: {OUTPUT_JSON}")
        count = 0
        max_edcs_int = 0
        with open(OUTPUT_JSON, "r", encoding="utf-8") as json_file:
            for line in json_file:
                line = line.strip().rstrip(",")
                if not line or line in ("[", "]"):
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                count += 1
                eid_int = edcs_id_to_int(rec.get("edcs_id", ""))
                if eid_int > max_edcs_int:
                    max_edcs_int = eid_int

        if count > 0:
            print(f"[resume] Existing rows: {count:,} | Last EDCS ID: EDCS-{max_edcs_int:08d}")
            return count, max_edcs_int

    return 0, 0


def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return None

    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as cp_file:
            cp = json.load(cp_file)
    except (OSError, json.JSONDecodeError):
        return None

    start = int(cp.get("start", 0))
    last_edcs_int = int(cp.get("last_edcs_int", 0))
    last_edcs_id = cp.get("last_edcs_id", f"EDCS-{last_edcs_int:08d}" if last_edcs_int else "")
    return {
        "start": max(0, start),
        "last_edcs_int": max(0, last_edcs_int),
        "last_edcs_id": str(last_edcs_id or ""),
    }


def save_checkpoint(start, last_edcs_id):
    ensure_data_dir()
    with open(CHECKPOINT, "w", encoding="utf-8") as cp_file:
        json.dump(
            {
                "start": start,
                "last_edcs_id": last_edcs_id,
                "last_edcs_int": edcs_id_to_int(last_edcs_id),
            },
            cp_file,
            indent=2,
        )


def open_json_output(existing_count):
    """Open JSON file for append while preserving valid JSON array format."""
    ensure_data_dir()

    # Fresh file.
    if existing_count == 0 or not os.path.exists(OUTPUT_JSON) or os.path.getsize(OUTPUT_JSON) == 0:
        json_file = open(OUTPUT_JSON, "w", encoding="utf-8")
        json_file.write("[\n")
        return json_file, True

    # Append to existing array: remove trailing ']' and continue writing.
    json_file = open(OUTPUT_JSON, "r+", encoding="utf-8")
    json_file.seek(0, os.SEEK_END)
    pos = json_file.tell() - 1

    while pos >= 0:
        json_file.seek(pos)
        ch = json_file.read(1)
        if ch in " \t\r\n":
            pos -= 1
            continue
        break

    if pos >= 0 and ch == "]":
        json_file.truncate(pos)

    json_file.seek(0, os.SEEK_END)
    return json_file, False

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    ensure_data_dir()

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── Detect working page size ──
    page_size = None
    total     = None
    print("[+] Connecting to EDCS API and testing page size...")

    for size in PAGE_SIZES:
        try:
            params = build_params(draw=1, start=0, length=size)
            r = session.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "data" in data and len(data["data"]) > 0:
                page_size = size
                total     = data["recordsTotal"]
                print(f"[+] Page size {size} works! Total records in EDCS: {total:,}")
                break
            else:
                print(f"[!] Page size {size} returned no data, trying {PAGE_SIZES[-1]}...")
        except Exception as e:
            print(f"[!] Page size {size} failed: {e}")
        time.sleep(DELAY)

    if not page_size or not total:
        print("[!] Could not connect to EDCS API. Check your internet connection.")
        sys.exit(1)

    # ── Existing local data state + checkpoint state ──
    existing_count, file_last_edcs_int = load_existing_state()
    start = existing_count
    last_edcs_int = file_last_edcs_int

    checkpoint = load_checkpoint()
    if checkpoint:
        cp_start = checkpoint["start"]
        cp_last_edcs_int = checkpoint["last_edcs_int"]

        # Use the farthest safe progress marker seen between file scan and checkpoint.
        start = max(start, cp_start)
        last_edcs_int = max(last_edcs_int, cp_last_edcs_int)
        print(
            "[resume] Checkpoint found: "
            f"start={cp_start:,}, last_id={checkpoint['last_edcs_id']} "
            f"(using start={start:,}, last_int={last_edcs_int})"
        )

    if total <= start:
        print(f"[✓] Local data is up to date. Website rows: {total:,} | Local rows/checkpoint: {start:,}")
        if os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
            print(f"[✓] Removed stale checkpoint: {CHECKPOINT}")
        return

    is_resume = start > 0

    # ── Open output files ──
    json_file, first_record = open_json_output(existing_count)
    tsv_file = open(OUTPUT_TSV, "a" if is_resume else "w", encoding="utf-8", newline="")
    tsv_writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t", extrasaction="ignore")

    if not is_resume:
        tsv_writer.writeheader()

    records_saved = 0
    skipped       = 0
    draw          = 1
    last_edcs_id  = f"EDCS-{last_edcs_int:08d}" if last_edcs_int else ""

    print(f"\n[+] Starting from offset  : {start:,}")
    print(f"[+] Total records in EDCS : {total:,}")
    print(f"[+] Page size             : {page_size}")
    print(f"[+] Output JSON           : {OUTPUT_JSON}")
    print(f"[+] Output TSV            : {OUTPUT_TSV}")
    print(f"[+] Press Ctrl+C anytime to stop — progress saved every page\n")

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
                row     = parse_record(item)
                eid_int = edcs_id_to_int(row["edcs_id"])

                # Skip rows that are already in local data.
                if eid_int <= last_edcs_int:
                    skipped += 1
                    continue

                # JSON
                if not first_record:
                    json_file.write(",\n")
                json.dump(row, json_file, ensure_ascii=False)
                first_record = False

                # TSV
                tsv_writer.writerow(row)

                records_saved += 1
                last_edcs_id   = row["edcs_id"]
                last_edcs_int  = eid_int

            # Progress
            pct           = min(100.0, (start + page_size) / total * 100)
            est_min       = int(((total - start - page_size) / page_size) * DELAY / 60)

            # Save resume state every page.
            save_checkpoint(start + page_size, last_edcs_id)

            print(
                f"  offset={start + page_size:>7,}/{total:,} | "
                f"saved={records_saved:>7,} | "
                f"skipped={skipped:>5,} | "
                f"{pct:5.1f}% | "
                f"~{est_min}min left      ",
                end="\r"
            )

            start += page_size
            draw  += 1
            time.sleep(DELAY)

    except KeyboardInterrupt:
        print(f"\n\n[!] Stopped by user. Run again to resume from {last_edcs_id}.")

    finally:
        json_file.write("\n]")
        json_file.close()
        tsv_file.close()

    if records_saved == 0:
        print("\n[=] No new rows were found to append.")

    print(f"\n[✓] New records saved : {records_saved:,}")
    print(f"[✓] Last EDCS ID  : {last_edcs_id}")
    print(f"[✓] JSON          : {OUTPUT_JSON}")
    print(f"[✓] TSV           : {OUTPUT_TSV}")

    if start >= total and os.path.exists(CHECKPOINT):
        os.remove(CHECKPOINT)
        print(f"[✓] Checkpoint deleted — full scrape complete!")


if __name__ == "__main__":
    main()
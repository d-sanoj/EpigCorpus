"""Scrape EDCS API with full attribute coverage.

This script is intentionally independent from `edcs_scraper.py`.
It flattens each API record, discovers the union of all keys, and writes:

- data/edcs_all_attributes.json    (JSON array; missing keys as null)
- data/edcs_all_attributes.tsv     (tab-separated; missing keys as empty field)
- data/edcs_all_attributes_schema.json (list of discovered columns)

Usage examples:
    python src/edcs_scraper_all_attributes.py
    python src/edcs_scraper_all_attributes.py --length 500 --delay 1.5
    python src/edcs_scraper_all_attributes.py --max-pages 3  # quick test
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any

import requests

API_URL = "https://edcs.hist.uzh.ch/api/query"
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

TEMP_NDJSON = DATA_DIR / "edcs_all_attributes.tmp.ndjson"
OUT_JSON = DATA_DIR / "edcs_all_attributes.json"
OUT_TSV = DATA_DIR / "edcs_all_attributes.tsv"
OUT_SCHEMA = DATA_DIR / "edcs_all_attributes_schema.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/120.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://edcs.hist.uzh.ch/en/search",
}


def build_params(draw: int, start: int, length: int) -> dict[str, Any]:
    cache_buster = int(time.time() * 1000)
    return {
        "draw": draw,
        "start": start,
        "length": length,
        "columns[0][data]": "obj.edcs-id",
        "columns[0][name]": "",
        "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "columns[1][data]": "",
        "columns[1][name]": "",
        "columns[1][searchable]": "true",
        "columns[1][orderable]": "true",
        "columns[1][search][value]": "",
        "columns[1][search][regex]": "false",
        "columns[2][data]": "obj.inschriften",
        "columns[2][name]": "",
        "columns[2][searchable]": "true",
        "columns[2][orderable]": "true",
        "columns[2][search][value]": "",
        "columns[2][search][regex]": "false",
        "columns[3][data]": "obj.material",
        "columns[3][name]": "",
        "columns[3][searchable]": "true",
        "columns[3][orderable]": "true",
        "columns[3][search][value]": "",
        "columns[3][search][regex]": "false",
        "columns[4][data]": "obj.datierung",
        "columns[4][name]": "",
        "columns[4][searchable]": "true",
        "columns[4][orderable]": "true",
        "columns[4][search][value]": "",
        "columns[4][search][regex]": "false",
        "columns[5][data]": "obj.anzahl_bilder",
        "columns[5][name]": "",
        "columns[5][searchable]": "false",
        "columns[5][orderable]": "false",
        "columns[5][search][value]": "",
        "columns[5][search][regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "asc",
        "order[0][name]": "",
        "search[value]": "",
        "search[regex]": "false",
        "_": cache_buster,
    }


def flatten_record(value: Any, prefix: str = "", out: dict[str, Any] | None = None) -> dict[str, Any]:
    """Flatten nested dicts using dot notation.

    Lists are kept as-is (to preserve source fidelity) and serialized later for TSV.
    """
    if out is None:
        out = {}

    if isinstance(value, dict):
        for key, sub_value in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            flatten_record(sub_value, next_prefix, out)
    else:
        out[prefix] = value

    return out


def fetch_page(session: requests.Session, draw: int, start: int, length: int, retries: int = 3) -> dict[str, Any]:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(API_URL, params=build_params(draw, start, length), timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            if attempt == retries:
                raise RuntimeError(f"Failed at start={start} after {retries} attempts: {exc}") from exc
            time.sleep(2 * attempt)

    raise RuntimeError("Unexpected fetch retry flow")


def to_tsv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def write_final_outputs(schema: list[str], temp_path: Path, out_json: Path, out_tsv: Path) -> int:
    row_count = 0

    with temp_path.open("r", encoding="utf-8") as src, out_json.open("w", encoding="utf-8") as j_out:
        j_out.write("[\n")
        first = True
        for line in src:
            line = line.strip()
            if not line:
                continue
            flat = json.loads(line)
            complete = {key: flat.get(key, None) for key in schema}

            if not first:
                j_out.write(",\n")
            json.dump(complete, j_out, ensure_ascii=False)
            first = False
            row_count += 1

        j_out.write("\n]\n")

    with temp_path.open("r", encoding="utf-8") as src, out_tsv.open("w", encoding="utf-8", newline="") as t_out:
        writer = csv.DictWriter(t_out, fieldnames=schema, delimiter="\t", extrasaction="ignore")
        writer.writeheader()

        for line in src:
            line = line.strip()
            if not line:
                continue
            flat = json.loads(line)
            complete = {key: to_tsv_cell(flat.get(key, None)) for key in schema}
            writer.writerow(complete)

    return row_count


def scrape_all_attributes(length: int, delay: float, max_pages: int | None) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    first_page = fetch_page(session, draw=1, start=0, length=length)
    total = int(first_page.get("recordsTotal", 0))
    if total == 0:
        raise RuntimeError("API returned zero recordsTotal; cannot continue.")

    print(f"[+] recordsTotal: {total:,}")
    print(f"[+] page length: {length}")
    if max_pages is not None:
        print(f"[+] max pages: {max_pages} (test mode)")

    schema_set: set[str] = set()
    pages_done = 0
    records_seen = 0

    with TEMP_NDJSON.open("w", encoding="utf-8") as temp_out:
        start = 0
        draw = 1

        while start < total:
            if max_pages is not None and pages_done >= max_pages:
                break

            if draw == 1:
                payload = first_page
            else:
                payload = fetch_page(session, draw=draw, start=start, length=length)

            rows = payload.get("data", [])
            if not rows:
                print(f"[=] Empty page at offset {start:,}. Stopping.")
                break

            for item in rows:
                flat = flatten_record(item)
                schema_set.update(flat.keys())
                temp_out.write(json.dumps(flat, ensure_ascii=False) + "\n")
                records_seen += 1

            pages_done += 1
            start += length
            draw += 1

            pct = min(100.0, (start / total) * 100)
            print(f"  pages={pages_done:>5} | records={records_seen:>8,} | approx={pct:5.1f}%", end="\r")
            time.sleep(delay)

    print("\n[+] Discovery pass complete.")
    schema = sorted(schema_set)
    OUT_SCHEMA.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[+] Columns discovered: {len(schema):,}")
    print(f"[+] Schema file: {OUT_SCHEMA}")

    row_count = write_final_outputs(schema, TEMP_NDJSON, OUT_JSON, OUT_TSV)
    print(f"[✓] Rows written: {row_count:,}")
    print(f"[✓] JSON output: {OUT_JSON}")
    print(f"[✓] TSV output : {OUT_TSV}")

    try:
        TEMP_NDJSON.unlink()
    except OSError:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape EDCS API with all attributes and null-filled schema")
    parser.add_argument("--length", type=int, default=500, help="Rows per API page (default: 500)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between page requests in seconds")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page limit for testing (e.g., 3)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    scrape_all_attributes(length=args.length, delay=args.delay, max_pages=args.max_pages)


if __name__ == "__main__":
    main()

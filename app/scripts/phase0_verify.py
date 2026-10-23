"""Phase 0 independent verification. Reads only; trusts no prior report."""
import csv, json, gzip, hashlib, sys
from collections import Counter
from pathlib import Path

REPO = Path("/Users/sanoj/Documents/Projects/EpigCorpus")
RAW = REPO/"data"/"edcs_inscriptions.jsonl"
TSV = REPO/"data"/"derived"/"abbrev_pairs.tsv"

out = {}

# --- 1. raw corpus, parsed independently of the probe -------------------
n_lines = 0
n_json_ok = 0
n_json_bad = 0
ids = set()
base_ids = set()
missing_field = Counter()
REQ = ["record_id","inscription_text","province","not_before","not_after"]
has_paren = 0
with RAW.open(encoding="utf-8") as fh:
    for line in fh:
        n_lines += 1
        try:
            r = json.loads(line)
        except Exception:
            n_json_bad += 1
            continue
        n_json_ok += 1
        for f in REQ:
            if f not in r:
                missing_field[f] += 1
        rid = r.get("record_id")
        if rid is not None:
            ids.add(rid)
            base_ids.add(rid.rsplit("-",1)[0] if rid.count("-")>=2 else rid)
        t = r.get("inscription_text") or ""
        if "(" in t:
            has_paren += 1
out["raw_lines"] = n_lines
out["raw_json_parsed"] = n_json_ok
out["raw_json_failed"] = n_json_bad
out["raw_distinct_record_id"] = len(ids)
out["raw_distinct_base_id"] = len(base_ids)
out["raw_missing_required_field"] = dict(missing_field)
out["raw_records_containing_open_paren"] = has_paren

# --- 2. TSV parsed with csv, not by line counting ----------------------
rows = 0
badcols = 0
embedded_nl = 0
tsv_ids = set()
hdr = None
with TSV.open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
    hdr = next(rd)
    ncol = len(hdr)
    for row in rd:
        rows += 1
        if len(row) != ncol:
            badcols += 1
        if any("\n" in c or "\r" in c for c in row):
            embedded_nl += 1
        tsv_ids.add(row[0])
out["tsv_header"] = hdr
out["tsv_data_rows_csvparse"] = rows
out["tsv_rows_wrong_column_count"] = badcols
out["tsv_rows_with_embedded_newline"] = embedded_nl
out["tsv_distinct_inscription_id"] = len(tsv_ids)
out["tsv_ids_not_in_raw"] = len(tsv_ids - ids)

# --- 3. gz vs working file --------------------------------------------
def sha(p, opener=open):
    h = hashlib.sha256()
    with opener(p,"rb") as fh:
        for chunk in iter(lambda: fh.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()
out["sha_raw_jsonl"] = sha(RAW)
out["sha_raw_jsonl_gz_decompressed"] = sha(REPO/"data"/"edcs_inscriptions.jsonl.gz", gzip.open)
out["sha_tsv"] = sha(TSV)

print(json.dumps(out, indent=2, ensure_ascii=False))

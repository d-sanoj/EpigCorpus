"""Files a harvest reads and writes: corpus output, lookup, checkpoint, failures, manifest."""

import csv
import gzip
import json
import os

from epigcorpus.adapters.console import log
from epigcorpus.config import paths
from epigcorpus.config.settings import EPIGCORPUS_VERSION
from epigcorpus.core.edcs_parsing import now_iso
from epigcorpus.models.inscription import LIST_COLUMNS, TSV_FIELDS

# ─── PATHS ────────────────────────────────────────────────────────────────────
# Defined in epigcorpus.config.paths; the scraper works on os.path strings.
RAW_DIR = os.fspath(paths.RAW_DIR)
HARVEST_DIR = os.fspath(paths.HARVEST_DIR)
REFERENCE_DIR = os.fspath(paths.REFERENCE_DIR)

OUTPUT_JSONL = os.fspath(paths.RAW_JSONL)
OUTPUT_TSV = os.fspath(paths.RAW_TSV)
LOOKUP_FILE = os.fspath(paths.LOOKUP_FILE)
CHECKPOINT = os.fspath(paths.CHECKPOINT_FILE)
FAILED_FILE = os.fspath(paths.FAILED_IDS_FILE)
MANIFEST = os.fspath(paths.MANIFEST_FILE)


def ensure_dirs():
    """Create every directory a harvest writes into."""
    for directory in (RAW_DIR, HARVEST_DIR, REFERENCE_DIR):
        os.makedirs(directory, exist_ok=True)


def discard_outputs():
    """--restart: delete the corpus output, checkpoint and failure list."""
    for path in (OUTPUT_JSONL, OUTPUT_TSV, CHECKPOINT, FAILED_FILE):
        if os.path.exists(path):
            os.remove(path)


def clear_checkpoint():
    """A finished harvest removes its checkpoint; the manifest marks completion."""
    if os.path.exists(CHECKPOINT):
        os.remove(CHECKPOINT)


# ─── CORPUS OUTPUT ────────────────────────────────────────────────────────────


class CorpusWriter:
    """The JSONL and TSV corpus files, written in lockstep, one row at a time.

    JSONL keeps list-valued columns as lists; the TSV joins them with " | ".
    """

    def __init__(self, is_resume):
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

        self.jsonl_file = open(OUTPUT_JSONL, "a" if is_resume else "w", encoding="utf-8")
        self.tsv_file = open(OUTPUT_TSV, "a" if is_resume else "w", encoding="utf-8", newline="")
        self.tsv_writer = csv.DictWriter(
            self.tsv_file, fieldnames=TSV_FIELDS, delimiter="\t", extrasaction="ignore"
        )
        if not is_resume:
            self.tsv_writer.writeheader()

    def write_row(self, row):
        self.jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        tsv_row = dict(row)
        for column in LIST_COLUMNS:
            if isinstance(tsv_row.get(column), list):
                tsv_row[column] = " | ".join(str(v) for v in tsv_row[column])
        self.tsv_writer.writerow(tsv_row)

    def flush(self):
        self.jsonl_file.flush()
        self.tsv_file.flush()

    def close(self):
        self.jsonl_file.close()
        self.tsv_file.close()


def open_corpus_writer(is_resume):
    return CorpusWriter(is_resume)


# ─── LOOKUP ───────────────────────────────────────────────────────────────────


def save_lookup(lookup):
    with open(LOOKUP_FILE, "w", encoding="utf-8") as handle:
        json.dump(lookup, handle, ensure_ascii=False, indent=2)


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

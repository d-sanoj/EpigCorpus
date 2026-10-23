"""The corpus on disk: the scraped input the cleaner reads, and the cleaned output.

pandas infers gzip from the extension, so every reader here accepts either the
plain working copy or the committed .gz snapshot.
"""

import hashlib
import json
from pathlib import Path

import pandas as pd

from epigcorpus.config.paths import (
    CLEANED_JSONL,
    CLEANED_JSONL_GZ,
    LOOKUP_FILE,
    RAW_DIR,
    RAW_JSONL,
    RAW_TSV,
)

# ─── Scraped input and cleaned output (the pipeline's cleaning step) ──────────


def get_jsonl_files() -> list[Path]:
    """Get corpus files in data directory, newest first.

    Matches both plain .jsonl and gzipped .jsonl.gz. A fresh clone contains only
    the gzipped snapshot; running the scraper produces the plain file. Plain
    files sort ahead of their own .gz at equal mtime so a freshly harvested
    corpus wins over a committed snapshot.
    """
    files = list(RAW_DIR.glob("*.jsonl")) + list(RAW_DIR.glob("*.jsonl.gz"))
    # Exclude our own output. It is the newest file in data/ after a run, so
    # without this the next invocation would load the cleaned corpus and clean
    # it a second time, applying every rule twice.
    files = [p for p in files if "_cleaned" not in p.name]
    return sorted(
        files,
        key=lambda p: (p.stat().st_mtime, p.suffix != ".gz"),
        reverse=True,
    )


def load_latest_data() -> pd.DataFrame:
    """Load the most recently created JSONL file from data directory."""
    jsonl_files = get_jsonl_files()
    if not jsonl_files:
        raise FileNotFoundError("No JSONL file found in 'data/' after running scraper.")

    jsonl_path = jsonl_files[0]
    # pandas infers gzip from the .gz extension, so both forms load identically.
    data = pd.read_json(jsonl_path, lines=True)
    print(f"\nLoaded: {jsonl_path}")

    if "belege" in data.columns:
        data = data.rename(columns={"belege": "evidence"})

    return data


def save_cleaned_data(cleaned: pd.DataFrame) -> Path:
    """Save cleaned data as JSONL, plus a .gz copy for distribution.

    The plain file is what the app reads locally; the .gz is what gets committed
    (it is roughly 11x smaller, keeping it under GitHub's 100 MB per-file limit
    without Git LFS). Loaders accept either, so nobody has to decompress by hand.
    """
    output_jsonl = CLEANED_JSONL
    output_gz = CLEANED_JSONL_GZ

    cleaned.to_json(output_jsonl, orient="records", lines=True, force_ascii=False)
    print(f"\nSaved cleaned JSONL: {output_jsonl}")

    cleaned.to_json(output_gz, orient="records", lines=True, force_ascii=False,
                    compression="gzip")
    raw_mb = output_jsonl.stat().st_size / 1048576
    gz_mb = output_gz.stat().st_size / 1048576
    print(f"Saved compressed   : {output_gz} "
          f"({gz_mb:.1f} MB, {raw_mb / gz_mb:.1f}x smaller)")

    return output_jsonl


# ─── Cleaned corpus and lookup (search and map) ───────────────────────────────


def cleaned_corpus_path() -> Path:
    """Return the cleaned corpus, preferring the plain file over the .gz snapshot.

    pandas infers compression from the extension, so callers need no special
    handling and users never have to decompress anything by hand.
    """
    if CLEANED_JSONL.exists():
        return CLEANED_JSONL
    if CLEANED_JSONL_GZ.exists():
        return CLEANED_JSONL_GZ
    raise FileNotFoundError(
        f"Missing: {CLEANED_JSONL} (or {CLEANED_JSONL_GZ.name}). Run main.py first."
    )


def read_cleaned_corpus() -> pd.DataFrame:
    """The whole cleaned corpus, every column."""
    return pd.read_json(cleaned_corpus_path(), lines=True)


# The downloadable files, by export name. Each prefers the plain working copy
# and falls back to the committed .gz snapshot, like cleaned_corpus_path.
EXPORT_FILES = {
    "corpus.jsonl": RAW_JSONL,
    "corpus.tsv": RAW_TSV,
    "cleaned.jsonl": CLEANED_JSONL,
}


def export_file(name: str) -> Path | None:
    """The file behind an export: plain if present, else its .gz, else None."""
    plain = EXPORT_FILES[name]
    if plain.exists():
        return plain
    packed = plain.with_name(plain.name + ".gz")
    return packed if packed.exists() else None


def fingerprint() -> str:
    """Identifies the cleaned-corpus snapshot on disk (name, size, mtime)."""
    path = cleaned_corpus_path()
    stat = path.stat()
    identity = f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(identity.encode()).hexdigest()[:16]


def read_lookup() -> dict | None:
    """The flattened EDCS code tables, or None when the file is absent."""
    if not LOOKUP_FILE.exists():
        return None
    with LOOKUP_FILE.open(encoding="utf-8") as handle:
        return json.load(handle)

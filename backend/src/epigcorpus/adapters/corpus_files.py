"""The corpus on disk: the scraped input the cleaner reads, and the cleaned output.

pandas infers gzip from the extension, so every reader here accepts either the
plain working copy or the committed .gz snapshot.
"""

import gzip
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from epigcorpus.config.paths import (
    CLEANED_JSONL,
    CLEANED_JSONL_GZ,
    CLEANED_PARQUET,
    LOOKUP_FILE,
    RAW_DIR,
    RAW_JSONL,
    RAW_TSV,
)

# Rows per chunk when reading a JSONL. Parsing the whole 588k-row corpus in
# one call peaks at 6.5-7.5 GB; in chunks it peaks at about 1.5 GB and gives
# an identical frame (checked on both the raw and the cleaned corpus).
JSONL_CHUNK_ROWS = 50_000


def read_jsonl(path: Path) -> pd.DataFrame:
    """Read a JSON Lines file (plain or .gz) in chunks, to bound peak memory."""
    chunks = pd.read_json(path, lines=True, chunksize=JSONL_CHUNK_ROWS)
    with chunks:
        return pd.concat(chunks, ignore_index=True)


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


def iter_latest_data() -> Iterator[pd.DataFrame]:
    """The newest scraped corpus, JSONL_CHUNK_ROWS rows at a time.

    Read in chunks so cleaning never holds more than one chunk of the raw
    corpus: parsing it whole peaks at 6.5 GB.
    """
    jsonl_files = get_jsonl_files()
    if not jsonl_files:
        raise FileNotFoundError(f"No scraped corpus (*.jsonl or *.jsonl.gz) in {RAW_DIR}.")

    jsonl_path = jsonl_files[0]
    print(f"\nLoading: {jsonl_path}")
    # pandas infers gzip from the .gz extension, so both forms load identically.
    with pd.read_json(jsonl_path, lines=True, chunksize=JSONL_CHUNK_ROWS) as chunks:
        for chunk in chunks:
            if "belege" in chunk.columns:
                chunk = chunk.rename(columns={"belege": "evidence"})
            yield chunk


def cleaned_jsonl_text(cleaned: pd.DataFrame) -> str:
    """A cleaned frame as JSON Lines, the cleaned corpus's on-disk form.

    Timestamps (`retrieved_at`) are written as ISO 8601 in whole seconds,
    `2026-08-19T16:32:11Z`, exactly as the scraper records them. pandas'
    default, epoch milliseconds, is deprecated in pandas 3, and it turned the
    scraper's timestamp into a bare number.
    """
    text = cleaned.to_json(
        orient="records", lines=True, force_ascii=False, date_format="iso", date_unit="s"
    )
    if text and not text.endswith("\n"):
        text += "\n"
    return text


class CleanedCorpusWriter:
    """Writes the cleaned corpus chunk by chunk, as JSONL plus a .gz copy.

    The plain file is what the app reads locally; the .gz is what gets
    committed (roughly 11x smaller, under GitHub's 100 MB per-file limit
    without Git LFS). Both are written to temporary files and only replace
    the current corpus once every chunk is in, so a failed or killed run
    leaves the previous corpus intact.
    """

    def __init__(self):
        self._plain_tmp = CLEANED_JSONL.with_name(CLEANED_JSONL.name + ".partial")
        self._gz_tmp = CLEANED_JSONL_GZ.with_name(CLEANED_JSONL_GZ.name + ".partial")
        self._plain = None
        self._gz = None
        self.rows = 0

    def __enter__(self) -> "CleanedCorpusWriter":
        self._plain = open(self._plain_tmp, "wb")
        # mtime=0: the same corpus compresses to the same bytes on every run.
        self._gz = gzip.GzipFile(self._gz_tmp, "wb", mtime=0)
        return self

    def write(self, cleaned: pd.DataFrame) -> None:
        # Chunk by chunk, to_json writes exactly the bytes it writes for the
        # whole frame (checked on the full corpus: same sha256).
        data = cleaned_jsonl_text(cleaned).encode("utf-8")
        self._plain.write(data)
        self._gz.write(data)
        self.rows += len(cleaned)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._plain.close()
        self._gz.close()
        if exc_type is not None:
            self._plain_tmp.unlink(missing_ok=True)
            self._gz_tmp.unlink(missing_ok=True)
            return
        self._plain_tmp.replace(CLEANED_JSONL)
        self._gz_tmp.replace(CLEANED_JSONL_GZ)
        raw_mb = CLEANED_JSONL.stat().st_size / 1048576
        gz_mb = CLEANED_JSONL_GZ.stat().st_size / 1048576
        print(f"\nSaved cleaned JSONL: {CLEANED_JSONL} ({self.rows:,} rows)")
        print(f"Saved compressed   : {CLEANED_JSONL_GZ} "
              f"({gz_mb:.1f} MB, {raw_mb / gz_mb:.1f}x smaller)")


def cleaned_writer() -> CleanedCorpusWriter:
    return CleanedCorpusWriter()


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


def _source_identity(path: Path) -> str:
    stat = path.stat()
    return f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"


# Parquet schema metadata naming the JSONL a snapshot was built from.
_SOURCE_KEY = b"epigcorpus_source"


def _read_snapshot(source: Path) -> pd.DataFrame | None:
    """The Parquet snapshot of `source`, or None when it is missing or stale."""
    try:
        parquet = pq.ParquetFile(CLEANED_PARQUET)
    except (FileNotFoundError, pa.ArrowInvalid):
        return None
    metadata = parquet.schema_arrow.metadata or {}
    if metadata.get(_SOURCE_KEY) != _source_identity(source).encode():
        return None
    table = parquet.read()
    # Arrow hands list columns to pandas as numpy arrays; the JSONL reader gives
    # lists, and the search code tells the two apart. So convert those columns
    # to Python lists directly and put them back in their places.
    lists = [
        (position, field.name)
        for position, field in enumerate(table.schema)
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
    ]
    frame = table.drop_columns([name for _, name in lists]).to_pandas()
    for position, name in lists:
        frame.insert(position, name, table.column(name).to_pylist())
    return frame


def _write_snapshot(frame: pd.DataFrame, source: Path) -> None:
    """Write the Parquet snapshot; a read-only data directory only costs speed."""
    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[_SOURCE_KEY] = _source_identity(source).encode()
    table = table.replace_schema_metadata(metadata)
    partial = CLEANED_PARQUET.with_suffix(".parquet.partial")
    try:
        pq.write_table(table, partial, compression="zstd")
        partial.replace(CLEANED_PARQUET)
    except OSError:
        partial.unlink(missing_ok=True)


def read_cleaned_corpus() -> pd.DataFrame:
    """The whole cleaned corpus, every column.

    Read from the Parquet snapshot when it was built from the current JSONL;
    otherwise from the JSONL, which then refreshes the snapshot. Both give the
    same frame: same columns, dtypes and values.
    """
    source = cleaned_corpus_path()
    frame = _read_snapshot(source)
    if frame is None:
        frame = read_jsonl(source)
        _write_snapshot(frame, source)
    return frame


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
    identity = _source_identity(cleaned_corpus_path())
    return hashlib.sha256(identity.encode()).hexdigest()[:16]


def read_lookup() -> dict | None:
    """The flattened EDCS code tables, or None when the file is absent."""
    if not LOOKUP_FILE.exists():
        return None
    with LOOKUP_FILE.open(encoding="utf-8") as handle:
        return json.load(handle)

"""Interfaces the services depend on.

Services never import adapters. They receive objects that satisfy these
Protocols from a composition root (app/main.py, app/cli/*, app/ui/*). The
adapter modules in app/adapters satisfy them structurally: a module whose
functions match the Protocol's methods is a valid implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

# One line to stdout. The scraper's is thread-safe (app.adapters.console.log).
Log = Callable[[str], None]


class EdcsClient(Protocol):
    """HTTP access to the EDCS static-file API."""

    def make_session(self) -> Any: ...

    def get_json(self, session: Any, url: str, timeout: float = 300) -> Any: ...

    def build_lookup(self, session: Any) -> dict: ...

    def fetch_index(self, session: Any, use_cache: bool = True) -> tuple[list[int], str]: ...


class CorpusWriter(Protocol):
    """The corpus output files a harvest appends rows to."""

    def write_row(self, row: dict) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class HarvestStore(Protocol):
    """Everything a harvest persists: corpus output, lookup, checkpoint, failures, manifest."""

    OUTPUT_JSONL: str
    OUTPUT_TSV: str
    LOOKUP_FILE: str
    FAILED_FILE: str
    MANIFEST: str

    def ensure_dirs(self) -> None: ...

    def discard_outputs(self) -> None: ...

    def clear_checkpoint(self) -> None: ...

    def open_corpus_writer(self, is_resume: bool) -> CorpusWriter: ...

    def save_lookup(self, lookup: dict) -> None: ...

    def save_checkpoint(self, cursor: int, monuments_done: int, rows_written: int) -> None: ...

    def load_checkpoint(self) -> dict | None: ...

    def save_manifest(
        self,
        monuments_done: int,
        rows_written: int,
        index_fetched_at: str,
        duration_s: float,
        failed: set,
    ) -> None: ...

    def load_manifest(self) -> dict | None: ...

    def compress_outputs(self) -> None: ...

    def save_failed(self, failed: set) -> None: ...

    def load_failed(self) -> set: ...


class RawCorpusStore(Protocol):
    """The scraped corpus as main.py's cleaning step reads it, and the cleaned output."""

    def load_latest_data(self) -> pd.DataFrame: ...

    def save_cleaned_data(self, cleaned: pd.DataFrame) -> Path: ...


class CleanedCorpusReader(Protocol):
    """The cleaned corpus and the lookup table, as the search services read them."""

    def read_cleaned_corpus(self) -> pd.DataFrame: ...

    def read_lookup(self) -> dict | None: ...


class MapLayerSource(Protocol):
    """Vendored historical map layers."""

    def load_provinces(self) -> Any: ...

    def load_roads(self) -> Any: ...

    def load_cities(self) -> Any: ...

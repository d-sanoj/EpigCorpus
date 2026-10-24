"""Work the server runs in a child process (see services/isolation.py).

A composition root like server.py: each function wires real adapters into a
service. They are module-level so a spawned child can import them by name
without importing the server and building its app.
"""

from __future__ import annotations

from epigcorpus.adapters import corpus_files
from epigcorpus.services.cleaning import run_cleaning


def clean_corpus() -> int:
    """Clean the newest raw corpus, save it, and refresh its Parquet snapshot.

    Refreshing the snapshot here, in the child, means the reload that
    follows reads Parquet in seconds instead of parsing the new JSONL inside
    the server. Returns the rows written.
    """
    rows = run_cleaning(corpus_files)
    corpus_files.read_cleaned_corpus()
    return rows

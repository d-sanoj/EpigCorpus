"""Cleaning chunk by chunk writes exactly what cleaning the whole corpus wrote."""

from __future__ import annotations

import gzip
import re
import shutil

import pandas as pd
import pytest

from epigcorpus.adapters import corpus_files
from epigcorpus.services.cleaning import clean_data, run_cleaning


@pytest.fixture()
def store(corpus_dir, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    shutil.copy(corpus_dir / "edcs_inscriptions.jsonl.gz", raw)
    monkeypatch.setattr(corpus_files, "RAW_DIR", raw)
    monkeypatch.setattr(corpus_files, "CLEANED_JSONL", tmp_path / "cleaned.jsonl")
    monkeypatch.setattr(corpus_files, "CLEANED_JSONL_GZ", tmp_path / "cleaned.jsonl.gz")
    # Small chunks, so the fixture corpus spans several of them.
    monkeypatch.setattr(corpus_files, "JSONL_CHUNK_ROWS", 7)
    return raw / "edcs_inscriptions.jsonl.gz"


def test_streamed_output_equals_whole_frame_output(store):
    whole = pd.read_json(store, lines=True).rename(columns={"belege": "evidence"})
    expected = corpus_files.cleaned_jsonl_text(clean_data(whole, verbose=False))

    rows = run_cleaning(corpus_files)

    assert rows == len(whole) > 7
    written = corpus_files.CLEANED_JSONL.read_text(encoding="utf-8")
    assert written == expected
    # Timestamps as the scraper writes them, not epoch milliseconds.
    assert re.search(r'"retrieved_at":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ"', written)
    assert gzip.decompress(corpus_files.CLEANED_JSONL_GZ.read_bytes()).decode() == written


def test_a_failed_run_leaves_the_previous_corpus(store, monkeypatch):
    corpus_files.CLEANED_JSONL.write_text("previous\n", encoding="utf-8")

    def broken(data, verbose=True):
        raise MemoryError

    monkeypatch.setattr("epigcorpus.services.cleaning.clean_data", broken)
    with pytest.raises(MemoryError):
        run_cleaning(corpus_files)

    assert corpus_files.CLEANED_JSONL.read_text(encoding="utf-8") == "previous\n"
    assert not list(corpus_files.CLEANED_JSONL.parent.glob("*.partial"))

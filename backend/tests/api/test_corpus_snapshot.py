"""The Parquet snapshot of the cleaned corpus reads back as the JSONL does."""

from __future__ import annotations

import os
import shutil

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from epigcorpus.adapters import corpus_files

LIST_COLUMNS = ("evidence", "category", "category_en", "status", "language_codes")


@pytest.fixture()
def cleaned(corpus_dir, tmp_path, monkeypatch):
    jsonl = tmp_path / "edcs_inscriptions_cleaned.jsonl"
    shutil.copy(corpus_dir / "edcs_inscriptions_cleaned.jsonl", jsonl)
    monkeypatch.setattr(corpus_files, "CLEANED_JSONL", jsonl)
    monkeypatch.setattr(corpus_files, "CLEANED_JSONL_GZ", tmp_path / "absent.jsonl.gz")
    monkeypatch.setattr(corpus_files, "CLEANED_PARQUET", tmp_path / "cleaned.parquet")
    return jsonl


def test_snapshot_reads_back_identical_to_the_jsonl(cleaned):
    expected = pd.read_json(cleaned, lines=True)

    first = corpus_files.read_cleaned_corpus()
    assert corpus_files.CLEANED_PARQUET.exists()
    second = corpus_files.read_cleaned_corpus()

    for frame in (first, second):
        assert_frame_equal(frame, expected, check_exact=True)
        for column in LIST_COLUMNS:
            assert all(type(value) is list for value in frame[column])
            assert frame[column].tolist() == expected[column].tolist()


def test_snapshot_is_rebuilt_when_the_jsonl_changes(cleaned):
    corpus_files.read_cleaned_corpus()
    lines = cleaned.read_text(encoding="utf-8").splitlines(keepends=True)
    cleaned.write_text("".join(lines[:10]), encoding="utf-8")
    stat = cleaned.stat()
    os.utime(cleaned, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert len(corpus_files.read_cleaned_corpus()) == 10


def test_an_unreadable_snapshot_falls_back_to_the_jsonl(cleaned):
    corpus_files.CLEANED_PARQUET.write_bytes(b"not parquet")

    assert_frame_equal(
        corpus_files.read_cleaned_corpus(), pd.read_json(cleaned, lines=True), check_exact=True
    )

"""An API over a small but real corpus, built offline from the EDCS fixtures.

The 50 captured monuments go through the real lookup builder, parser and
cleaner, are written with the pipeline's own to_json call and read back with
pandas, so the API sees exactly the column types it sees in production. The
map layers are the vendored ones. Scrape jobs run a fake harvest.
"""

from __future__ import annotations

import gzip
import json
import threading
from pathlib import Path

import pandas as pd
import pytest
import yaml
from fastapi.testclient import TestClient

from epigcorpus.adapters import edcs_client, map_layers
from epigcorpus.api.app import create_app
from epigcorpus.api.deps import ApiDeps
from epigcorpus.config.paths import API_SPEC
from epigcorpus.core.edcs_parsing import parse_monument
from epigcorpus.services.catalog import Catalog
from epigcorpus.services.cleaning import clean_data
from epigcorpus.services.jobs import ScrapeJobs
from epigcorpus.services.scrape import COMPLETE, INTERRUPTED

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class _Response:
    def __init__(self, payload):
        self.status_code = 200
        self.headers = {}
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FixtureSession:
    """Serves the captured EDCS index files to edcs_client.build_lookup."""

    files = {
        "lookups.json": "lookups.json",
        "languages.json": "languages.json",
        "places.json": "places_subset.json",
    }

    def get(self, url, timeout=None):
        name = url.rsplit("/", 1)[1]
        if name == "sources.json":
            return _Response({"d": []})
        return _Response(json.loads((FIXTURES / self.files[name]).read_text(encoding="utf-8")))


class FileReader:
    """CleanedCorpusReader and ExportSource over files in a temporary folder."""

    def __init__(self, folder: Path, lookup: dict):
        self.folder = folder
        self.cleaned = folder / "edcs_inscriptions_cleaned.jsonl"
        self.lookup = lookup

    def read_cleaned_corpus(self) -> pd.DataFrame:
        return pd.read_json(self.cleaned, lines=True)

    def read_lookup(self):
        return json.loads(json.dumps(self.lookup))

    def fingerprint(self) -> str:
        return "fixture-snapshot"

    def export_file(self, name: str):
        candidates = {
            "corpus.jsonl": self.folder / "edcs_inscriptions.jsonl.gz",
            "corpus.tsv": self.folder / "edcs_inscriptions.tsv",
            "cleaned.jsonl": self.cleaned,
        }
        path = candidates[name]
        return path if path.exists() else None


@pytest.fixture(scope="session")
def corpus_dir(tmp_path_factory) -> Path:
    folder = tmp_path_factory.mktemp("corpus")
    # The scraper parses with the lookup as build_lookup returns it (integer keys)
    # and saves it as JSON (string keys); both forms are used, as in production.
    lookup = edcs_client.build_lookup(FixtureSession())
    rows = []
    for path in sorted((FIXTURES / "monuments").glob("*.json")):
        monument_id = int(path.stem.split("_")[1])
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows.extend(parse_monument(payload, monument_id, lookup))
    raw_path = folder / "edcs_inscriptions.jsonl.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    # As app/main.py: read the raw corpus, rename belege, clean, write JSONL.
    raw = pd.read_json(raw_path, lines=True).rename(columns={"belege": "evidence"})
    cleaned = clean_data(raw, verbose=False)
    cleaned.to_json(folder / "edcs_inscriptions_cleaned.jsonl", orient="records", lines=True,
                    force_ascii=False)
    (folder / "lookup.json").write_text(json.dumps(lookup, ensure_ascii=False), encoding="utf-8")
    return folder


@pytest.fixture(scope="session")
def reader(corpus_dir) -> FileReader:
    lookup = json.loads((corpus_dir / "lookup.json").read_text(encoding="utf-8"))
    return FileReader(corpus_dir, lookup)


class FakeHarvest:
    """Stands in for run_harvest: walks 5 'chunks', honouring stop and progress."""

    def __init__(self):
        self.release = threading.Event()
        self.calls = []

    def __call__(self, limit, workers, restart, refresh_index, *, should_stop, on_progress):
        self.calls.append((limit, workers, restart, refresh_index))
        for step in range(1, 6):
            if should_stop():
                return INTERRUPTED
            on_progress({"cursor": step, "total": 5, "monuments_done": step,
                         "rows_written": step, "failed": 0, "rate_per_second": 1.0,
                         "index_fetched_at": "2026-10-23T18:00:04Z"})
            self.release.wait(5)
        return COMPLETE


@pytest.fixture(scope="session")
def catalog(reader) -> Catalog:
    return Catalog(reader, map_layers)


@pytest.fixture()
def harvest() -> FakeHarvest:
    return FakeHarvest()


@pytest.fixture()
def client(catalog, reader, harvest, tmp_path) -> TestClient:
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html><title>EpigCorpus</title>", encoding="utf-8")
    deps = ApiDeps(
        catalog=catalog,
        exports=reader,
        jobs=ScrapeJobs(harvest),
        spec_path=API_SPEC,
        static_dir=static,
    )
    with TestClient(create_app(deps)) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def spec() -> dict:
    return yaml.safe_load(API_SPEC.read_text(encoding="utf-8"))


"""Shared fixtures. All test data is captured from the live EDCS; tests run offline."""

import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(name: str):
    with open(FIXTURE_DIR / name, encoding="utf-8") as handle:
        return json.load(handle)


@pytest.fixture(scope="session")
def monuments() -> list[dict]:
    """50 real monument payloads from /data/monument/{shard}/{id8}.json (2026-08-19)."""
    paths = sorted((FIXTURE_DIR / "monuments").glob("*.json"))
    return [json.loads(p.read_text(encoding="utf-8")) for p in paths]


@pytest.fixture(scope="session")
def lookups() -> dict:
    """/data/indexes/lookups.json — materials, provinces, categories."""
    return _load("lookups.json")


@pytest.fixture(scope="session")
def places() -> dict:
    """Subset of /data/indexes/places.json covering the monument fixtures."""
    return _load("places_subset.json")


@pytest.fixture(scope="session")
def searchable_sample() -> dict:
    """First 200 rows of /data/indexes/searchable.json."""
    return _load("searchable_sample.json")


@pytest.fixture(scope="session")
def places_by_id(places) -> dict:
    return {row[0]: row for row in places["d"]}

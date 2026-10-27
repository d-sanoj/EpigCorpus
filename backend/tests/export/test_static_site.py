"""The static export: the container format, and the files the browser fetches.

The browser reads the container with its own decoder (frontend/src/api/static/
container.ts), so these tests pin the format down here: a change that breaks
the frontend breaks this file first.
"""

from __future__ import annotations

import gzip
import json

import numpy as np
import pandas as pd
import pytest

from epigcorpus.export import static_site


def small_frame() -> pd.DataFrame:
    """Three rows: a plain one, one missing province and place, one missing text.

    Rows 1 and 3 share a findspot, and row 3's coordinates differ from row 1's
    beyond the fifth decimal, so the round-then-group rule is exercised. The
    `retrieved_at` stamps are one scrape session, the newest last.
    """
    return pd.DataFrame(
        [
            {
                "record_id": "EDCS-00000001-0",
                "edcs_id": "EDCS-00000001",
                "place": "Roma",
                "province": "Italia",
                "material_en": "stone",
                "language": "Latin",
                "category_en": ["tomb inscriptions", "men"],
                "latitude": 41.9028,
                "longitude": 12.4964,
                "not_before": 100,
                "not_after": 200,
                "inscription_text": "D M S",
                "inscription_text_conservative": "D M S",
                "inscription_text_interpretive": "Dis Manibus Sacrum",
                "retrieved_at": "2026-08-19T16:32:11Z",
            },
            {
                "record_id": "EDCS-00000002-0",
                "edcs_id": "EDCS-00000002",
                "place": None,
                "province": None,
                "material_en": "marble",
                "language": "Greek, Latin",
                "category_en": ["milestones"],
                "latitude": 41.5,
                "longitude": 12.0,
                "not_before": None,
                "not_after": None,
                "inscription_text": "via",
                "inscription_text_conservative": "via",
                "inscription_text_interpretive": "via",
                "retrieved_at": "2026-08-19T17:05:40Z",
            },
            {
                "record_id": "EDCS-00000001-1",
                "edcs_id": "EDCS-00000001",
                "place": "Roma",
                "province": "Italia",
                "material_en": "stone",
                "language": "Latin",
                "category_en": ["tomb inscriptions"],
                "latitude": 41.902800004,
                "longitude": 12.496400004,
                "not_before": 150,
                "not_after": 150,
                "inscription_text": None,
                "inscription_text_conservative": None,
                "inscription_text_interpretive": None,
                "retrieved_at": "2026-08-19T19:51:02Z",
            },
        ]
    )


def build(frame: pd.DataFrame) -> dict:
    texts = {}
    blobs = {}
    for mode, column in static_site.TEXT_COLUMNS.items():
        blob, offsets = static_site._text_blob(list(frame[column]))
        texts[mode] = offsets
        blobs[mode] = blob
    container = static_site.read_container(
        static_site.build_container(frame, texts, "testfingerprint")
    )
    return {**container, "blobs": blobs}


def test_container_round_trips_the_search_frame() -> None:
    container = build(small_frame())
    header, arrays = container["header"], container["arrays"]

    assert header["format"] == static_site.FORMAT
    assert header["rows"] == 3
    assert list(header["modes"]) == ["raw", "conservative", "interpretive"]

    # Places: "?" at index 0 carries both the missing place and a literal one.
    places = header["dicts"]["place"]
    assert places[0] == "?"
    assert [places[code] for code in arrays["place"]] == ["Roma", "?", "Roma"]

    # A missing province is index 0: nothing selectable maps to it.
    provinces = header["dicts"]["province"]
    assert provinces[0] == ""
    assert [provinces[code] for code in arrays["province"]] == ["Italia", "", "Italia"]

    # Coordinates are rounded to five decimals before they are de-duplicated,
    # so rows 1 and 3 share one coordinate.
    assert header["coord_count"] == 2
    assert arrays["coord"][0] == arrays["coord"][2]
    first = arrays["coord"][0]
    assert arrays["coord_latitude"][first] == pytest.approx(41.9028)
    assert arrays["coord_longitude"][first] == pytest.approx(12.4964)

    # Years: null stays null, an exact year stays exact.
    assert list(arrays["not_before"]) == [100, static_site.MISSING_YEAR, 150]
    assert list(arrays["not_after"]) == [200, static_site.MISSING_YEAR, 150]

    # Records: EDCS-00000001-0 as its number and its index.
    assert list(arrays["edcs"]) == [1, 2, 1]
    assert list(arrays["index"]) == [0, 0, 1]

    # The table prints the corpus's own string, not the tokens the filter uses.
    language_text = header["dicts"]["language_text"]
    assert [language_text[code] for code in arrays["language_text"]] == [
        "Latin",
        "Greek, Latin",
        "Latin",
    ]


def test_container_keeps_the_multi_value_filters_as_token_sets() -> None:
    container = build(small_frame())
    header, arrays = container["header"], container["arrays"]

    languages = header["dicts"]["language"]
    offsets, ids = arrays["language_offsets"], arrays["language_ids"]
    sets = [
        {languages[identifier] for identifier in ids[offsets[row] : offsets[row + 1]]}
        for row in range(3)
    ]
    # "Greek, Latin" arrives comma-joined and is matched on token overlap.
    assert sets == [{"Latin"}, {"Greek", "Latin"}, {"Latin"}]

    categories = header["dicts"]["category"]
    offsets, ids = arrays["category_offsets"], arrays["category_ids"]
    sets = [
        {categories[identifier] for identifier in ids[offsets[row] : offsets[row + 1]]}
        for row in range(3)
    ]
    assert sets == [{"men", "tomb inscriptions"}, {"milestones"}, {"tomb inscriptions"}]


def test_container_text_offsets_slice_each_row_back() -> None:
    container = build(small_frame())
    for mode in static_site.TEXT_COLUMNS:
        offsets = container["arrays"][f"text_{mode}"]
        blob = container["blobs"][mode]
        texts = [
            blob[offsets[row] : offsets[row + 1]].decode("utf-8") for row in range(3)
        ]
        assert len(offsets) == 4
        # Missing text is zero bytes, which the browser reads back as null.
        assert texts[2] == ""
        assert texts[0] == ("Dis Manibus Sacrum" if mode == "interpretive" else "D M S")
    assert len(container["blobs"]["raw"]) == container["arrays"]["text_raw"][3]


def test_record_ids_are_checked_against_the_pattern() -> None:
    frame = small_frame()
    frame.loc[0, "record_id"] = "EDCS-1"
    with pytest.raises(ValueError, match="record_id"):
        static_site.build_container(frame, {"raw": np.zeros(4, dtype=np.uint32)}, "fp")


class StubCatalog:
    """The pieces of a Catalog the exporter reads, as fixture data."""

    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def inscriptions(self) -> pd.DataFrame:
        return self._frame

    def full(self) -> pd.DataFrame:
        return self._frame

    def fingerprint(self) -> str:
        return "testfingerprint"

    def filter_options(self) -> dict[str, list[str]]:
        return {
            "material_en": ["marble", "stone"],
            "province": ["Italia"],
            "place": ["Roma"],
            "category_en": ["men", "milestones", "tomb inscriptions"],
            "language": ["Greek", "Latin"],
        }

    def short_forms(self) -> dict[str, dict[str, str]]:
        return {
            "material_en": {"stone": "lapis"},
            "category_en": {},
            "language": {"Latin": "la"},
        }

    def known_places(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"place": ["Roma"], "latitude": [41.9028], "longitude": [12.4964], "count": [3]}
        )

    def provinces_geojson(self) -> dict:
        return {"type": "FeatureCollection", "features": []}

    def province_labels(self) -> list[dict]:
        return [{"name": "Italia", "latitude": 42.0, "longitude": 12.0, "color": "#ffffff"}]

    def roads_geojson(self) -> dict:
        return {"type": "FeatureCollection", "features": []}

    def cities_points(self) -> list[list]:
        return [[41.9, 12.5, "Roma"]]


def test_a_corpus_without_scrape_stamps_dates_nothing() -> None:
    without = small_frame().drop(columns=["retrieved_at"])
    assert static_site.scrape_date(StubCatalog(without)) is None

    unreadable = small_frame().assign(retrieved_at=["", None, "not a date"])
    assert static_site.scrape_date(StubCatalog(unreadable)) is None


def test_epoch_millisecond_stamps_read_back_too() -> None:
    """Corpora cleaned before 2026-10-24 hold epoch milliseconds."""
    epoch_ms = int(pd.Timestamp("2026-08-19T19:51:02Z").timestamp() * 1000)
    frame = small_frame().assign(retrieved_at=[epoch_ms, epoch_ms, epoch_ms])
    assert static_site.scrape_date(StubCatalog(frame)) == "2026-08-19"


def test_write_static_site_names_every_file_in_the_manifest(tmp_path) -> None:
    manifest = static_site.write_static_site(
        StubCatalog(small_frame()), tmp_path, log=lambda _: None
    )

    assert manifest["rows"] == 3
    files = manifest["files"]
    assert files["corpus"] == "corpus.bin.gz"
    assert set(files["text"]) == set(static_site.TEXT_COLUMNS)
    assert files["known_places"] == "places-known.geojson"
    assert set(files["map"]) == {"provinces", "province-labels", "roads", "cities"}
    for name in [files["corpus"], files["facets"], files["known_places"], files["openapi"]]:
        assert (tmp_path / name).exists()
    for name in [*files["text"].values(), *files["map"].values()]:
        assert (tmp_path / name).exists()

    # The browser checks the container's fingerprint and the text lengths
    # against the manifest before it uses either.
    container = static_site.read_container(
        gzip.decompress((tmp_path / files["corpus"]).read_bytes())
    )
    assert container["header"]["fingerprint"] == manifest["fingerprint"]
    for mode, name in files["text"].items():  # type: ignore[union-attr]
        blob = gzip.decompress((tmp_path / name).read_bytes())
        assert len(blob) == manifest["text_bytes"][mode]
        assert len(blob) == container["arrays"][f"text_{mode}"][-1]
    # The manifest is the last thing written: without it the build is unfinished.
    written = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert written == manifest
    assert written["fingerprint"] == "testfingerprint"
    assert written["modes"] == list(static_site.TEXT_COLUMNS)
    assert set(written["text_bytes"]) == set(static_site.TEXT_COLUMNS)
    # The page dates its snapshot with the newest scrape stamp.
    assert written["data_as_of"] == "2026-08-19"

    # The facets file carries the API's names, not the corpus column names.
    facets = json.loads((tmp_path / files["facets"]).read_text(encoding="utf-8"))
    assert set(facets["values"]) == {"material", "province", "place", "category", "language"}
    assert facets["values"]["material"] == ["marble", "stone"]
    assert facets["short_forms"]["language"] == {"Latin": "la"}

    known = json.loads((tmp_path / files["known_places"]).read_text(encoding="utf-8"))
    assert known["summary"] == {"places": 1, "inscriptions": 3}
    assert known["features"][0]["properties"] == {"place": "Roma", "count": 3}
    assert known["features"][0]["geometry"]["coordinates"] == [12.4964, 41.9028]

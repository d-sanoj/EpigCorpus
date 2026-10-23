"""Pin the EDCS schema as observed on 2026-08-19 (release 20260807-142626).

These tests do not exercise our code. They exist so that the next EDCS rebuild
produces a red test instead of silent data loss -- the exact failure mode that
killed LatEpig and then EpigCorpus (docs/LATEPIG_BREAKAGE.md).
"""

import pytest


class TestMonumentPayload:
    def test_every_fixture_is_schema_version_2(self, monuments):
        assert {m["v"] for m in monuments} == {2}

    def test_payload_wraps_data_in_d(self, monuments):
        for m in monuments:
            assert set(m) <= {"v", "d"}
            assert isinstance(m["d"], dict)

    def test_known_keys_only(self, monuments):
        known = {"g", "m", "q", "p", "i", "c"}
        for m in monuments:
            unexpected = set(m["d"]) - known
            assert not unexpected, f"new EDCS field(s): {unexpected}"

    def test_inscriptions_are_a_list(self, monuments):
        """The one-monument-to-many-inscriptions model EpigCorpus is built on."""
        for m in monuments:
            assert isinstance(m["d"].get("i", []), list)

    def test_inscription_entries_are_dicts_with_text(self, monuments):
        for m in monuments:
            for insc in m["d"].get("i", []):
                assert isinstance(insc, dict)
                assert isinstance(insc.get("t", ""), str)

    def test_dating_is_a_two_element_list(self, monuments):
        for m in monuments:
            for insc in m["d"].get("i", []):
                if "d" in insc:
                    assert isinstance(insc["d"], list) and len(insc["d"]) == 2

    def test_citations_are_triples(self, monuments):
        """d.q replaces obj.belege; parse_belege truncating to [:3] is T36."""
        for m in monuments:
            for cite in m["d"].get("q", []):
                assert isinstance(cite, list) and len(cite) >= 3

    def test_at_least_one_multi_inscription_monument(self, monuments):
        """Guards the record_id scheme against a sample that would not exercise it."""
        assert any(len(m["d"].get("i", [])) > 1 for m in monuments)


class TestPlacesIndex:
    def test_column_keys(self, places):
        assert places["k"] == ["geo_id", "ort", "p", "coord"]

    def test_coord_is_latitude_longitude(self, places_by_id):
        """The old API gave [lon, lat]; the new one gives [lat, lon].

        edcs_scraper.parse_monument still hardcodes longitude = coord[0].
        Porting without fixing that transposes the entire corpus (T34).
        """
        roma = next(r for r in places_by_id.values() if r[1] == "Roma")
        lat, lon = roma[3]
        assert 41.5 < lat < 42.5, f"expected Roma latitude first, got {roma[3]}"
        assert 12.0 < lon < 13.0

    def test_coordinate_ranges_are_plausible(self, places):
        for row in places["d"]:
            if not row[3]:
                continue
            lat, lon = row[3]
            assert -90 <= lat <= 90, f"{row[1]}: latitude {lat} out of range"
            assert -180 <= lon <= 180, f"{row[1]}: longitude {lon} out of range"

    def test_coord_is_present_or_none_never_partial(self, places):
        """Coordinates are either a full [lat, lon] pair or absent.

        Across the full index 14,200 entries (2.6%) resolve to a place with no
        coordinates, so consumers must handle None -- but never a 1-element pair.
        """
        for row in places["d"]:
            assert row[3] is None or len(row[3]) == 2


class TestSearchableIndex:
    def test_column_keys(self, searchable_sample):
        assert searchable_sample["k"] == ["id", "g", "m", "ci", "li", "d", "h"]

    def test_rows_match_column_count(self, searchable_sample):
        for row in searchable_sample["d"]:
            assert len(row) == len(searchable_sample["k"])

    def test_geo_id_may_be_null(self, searchable_sample):
        """0.6% of entries carry no geo id at all."""
        assert any(row[1] is None for row in searchable_sample["d"])

    @pytest.mark.parametrize("index", range(20))
    def test_dating_field_is_polymorphic(self, searchable_sample, index):
        """d is [] or an int or [from, to] -- a parsing trap for T35."""
        dating = searchable_sample["d"][index][5]
        assert isinstance(dating, (list, int)), type(dating)


class TestLookups:
    def test_top_level_shape(self, lookups):
        assert set(lookups) == {"d", "v"}
        assert set(lookups["d"]) == {"materials", "provinces", "categories"}

    def test_english_labels_present(self, lookups):
        """get_material_en and translate_categories depend on the 'en' key."""
        for entry in lookups["d"]["materials"]:
            if entry is None:
                continue
            assert "en" in entry[1]

    def test_provinces_are_plain_strings(self, lookups):
        assert all(isinstance(p, str) for p in lookups["d"]["provinces"])

    def test_britannia_is_a_province(self, lookups):
        """19,247 Britannic inscriptions are currently deleted by ROMAN_BOUNDS (T10)."""
        assert "Britannia" in lookups["d"]["provinces"]

"""Characterisation tests for the cleaning pipeline as it behaves today.

Tests marked xfail(strict=True) assert the CORRECT epigraphic output and
currently fail. Each names the task that will fix it. When a Phase 1 fix lands,
its xfail is removed in the same commit -- so no behaviour change to the corpus
can happen silently. Ground rules 1 and 2.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from edcs_cleaner import (  # noqa: E402
    clean_conservative,
    clean_data,
    clean_interpretive,
    step8_arabic_numerals,
    step10_que_enclitic,
    step11_numeral_vir,
)


class TestQueEnclitic:
    """T05 -- step10 splits non-enclitic -que in high-frequency Latin."""

    @pytest.mark.parametrize("word", ["populusque", "senatusque", "armaque"])
    def test_genuine_enclitic_is_split(self, word):
        """The rule is correct for real enclitics; the fix must not break these."""
        assert step10_que_enclitic(word) == word[:-3] + " que"

    @pytest.mark.parametrize(
        "word",
        ["atque", "neque", "usque", "quinque", "cuiusque",
         "denique", "itaque", "quisque", "utque"],
    )
    @pytest.mark.xfail(strict=True, reason="T05: needs a non-enclitic stoplist")
    def test_non_enclitic_is_left_alone(self, word):
        assert step10_que_enclitic(word) == word


class TestNumeralVir:
    """T06 -- step11 handles Roman-numeral prefixes but not spelled-out ones."""

    @pytest.mark.parametrize("word,expected", [("IIvir", "II vir"), ("IIIIvir", "IIII vir"),
                                               ("VIvir", "VI vir")])
    def test_roman_numeral_prefix_is_split(self, word, expected):
        assert step11_numeral_vir(word) == expected

    @pytest.mark.parametrize("word,expected", [("sevir", "se vir"), ("duovir", "duo vir")])
    @pytest.mark.xfail(strict=True, reason="T06: spelled-out numeral prefixes unhandled")
    def test_spelled_out_prefix_is_split(self, word, expected):
        assert step11_numeral_vir(word) == expected


class TestIllegibleMarkers:
    """T07 -- EDCS marks unreadable letters with '+', which survives cleaning."""

    @pytest.mark.xfail(strict=True, reason="T07: '+' not stripped in step6")
    def test_illegible_marker_is_removed(self):
        raw = "L(ucius) Aemilius +++ vixit an[nos] ++"
        assert "+" not in clean_conservative(raw)
        assert "+" not in clean_interpretive(raw)

    def test_illegible_marker_currently_survives(self):
        """Documents present behaviour: '+++' becomes a token."""
        assert clean_conservative("L(ucius) Aemilius +++ vixit an[nos] ++") == \
            "L Aemilius +++ vixit an ++"


class TestNumerals:
    """T08 -- arabic digits are stripped but Roman numerals are not."""

    def test_arabic_digits_are_stripped(self):
        assert step8_arabic_numerals("anno 2024") == "anno "

    def test_roman_numerals_currently_survive(self):
        assert clean_conservative("vixit annos XXV et menses III anno 2024") == \
            "vixit annos XXV et menses III anno"


class TestNestedMarkup:
    """T09 -- flat regexes cannot parse nested delimiters, and step9 hides it."""

    def test_double_parens_at_top_level_happen_to_work(self):
        """((x)) with no nesting inside survives the flat regex by luck."""
        assert clean_conservative("((sestertium)) HS ((mille))") == "HS"
        assert clean_interpretive("((sestertium)) HS ((mille))") == "sestertium HS mille"

    @pytest.mark.xfail(strict=True, reason="T09: needs a balanced-delimiter parser")
    def test_nested_parens_are_parsed_as_a_unit(self):
        """A parenthetical containing a parenthetical is the real failure.

        `\\([^)]*\\)` stops at the FIRST ')', so `(A(uli) f(ilius))` is split
        into `(A(uli)` + `(ilius)`, leaving a stray `f` that step9 then strips of
        its bracket. Conservative should drop the whole editorial expansion.
        """
        assert clean_conservative("(A(uli) f(ilius))") == ""

    @pytest.mark.xfail(strict=True, reason="T09: erasure markup silently discarded")
    def test_erasure_markup_is_recorded(self):
        """[[X]] marks a deliberate erasure (damnatio memoriae), not a gap."""
        raw = "[[Domitiani]] erasa"
        assert clean_conservative(raw) != "erasa"

    def test_unbalanced_residue_is_laundered_today(self):
        """step9 deletes leftover brackets, so mis-parsing leaves no trace."""
        assert "(" not in clean_conservative("a(nnos) ((X))")
        assert ")" not in clean_conservative("a(nnos) ((X))")


class TestIsForged:
    """T04 -- is_forged can never fire. Both routes to the bug are covered."""

    def test_forged_flag_fires_on_the_real_edcs_marker(self):
        """EDCS marks falsae with a '*' PREFIX on the citation number.

        Confirmed against /data/indexes/citations/875.json (CIL) on 2026-08-19:
        4,719 citation numbers carry the marker, formatted `*00226`, not `00226*`.
        The existing substring test does fire on it -- so is_forged is not dead
        logic, it is untested logic fed by a confusingly renamed column (T04).
        """
        data = pd.DataFrame({
            "inscription_text": ["falsa est", "vera est"],
            "evidence": [["CIL 06 *00226"], ["CIL 06 00002"]],
        })
        assert clean_data(data, verbose=False)["is_forged"].tolist() == [True, False]

    def test_forged_is_all_false_on_citation_data(self):
        """main.py renames belege -> evidence, so this column holds citations."""
        data = pd.DataFrame({
            "inscription_text": ["abc", "def"],
            "evidence": [["CIL 16 00041"], ["AE 1913 00179"]],
        })
        assert not clean_data(data, verbose=False)["is_forged"].any()

    def test_forged_is_all_false_without_main_py_rename(self):
        """Running the cleaner on raw scraper output creates an empty column."""
        data = pd.DataFrame({"inscription_text": ["abc", "def"]})
        cleaned = clean_data(data, verbose=False)
        assert "evidence" in cleaned.columns
        assert not cleaned["is_forged"].any()


class TestPipelineInvariants:
    """Properties that must hold regardless of Phase 1 changes."""

    @pytest.mark.parametrize("raw", ["", "   ", "?"])
    def test_empty_input_stays_empty(self, raw):
        assert clean_conservative(raw) == ""
        assert clean_interpretive(raw) == ""

    def test_no_leading_or_trailing_whitespace(self):
        for raw in ["  L(ucius)  ", "/ Aemilius /", "[-] vixit [-]"]:
            assert clean_conservative(raw) == clean_conservative(raw).strip()
            assert clean_interpretive(raw) == clean_interpretive(raw).strip()

    def test_no_double_spaces(self):
        assert "  " not in clean_interpretive("L(ucius)  /  Aemilius   [-]")

    def test_interpretive_is_never_shorter_in_content_than_conservative(self):
        """Interpretive expands abbreviations; conservative drops them."""
        raw = "L(ucius) Aemilius Paullus co(n)s(ul)"
        assert len(clean_interpretive(raw)) > len(clean_conservative(raw))

    def test_is_unreadable_flag(self):
        data = pd.DataFrame({"inscription_text": ["", "?", "L(ucius)"]})
        assert clean_data(data, verbose=False)["is_unreadable"].tolist() == [True, True, False]

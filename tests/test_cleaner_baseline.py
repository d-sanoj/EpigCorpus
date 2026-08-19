"""Characterisation tests for the cleaning pipeline.

The rule sets are pinned here so that any change to corpus semantics is a
deliberate, visible edit rather than a side effect. Tests marked
``xfail(strict=True)`` assert the epigraphically correct output and currently
fail; each names the task that will fix it. When a fix lands, its xfail is
removed in the same commit.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from edcs_cleaner import (  # noqa: E402
    CONSERVATIVE_RULES,
    INTERPRETIVE_RULES,
    clean_conservative,
    clean_data,
    clean_interpretive,
)


class TestRuleSets:
    """The pipeline is an ordered list of substitutions; order is load-bearing."""

    def test_rule_counts_are_pinned(self):
        assert len(CONSERVATIVE_RULES) == 27
        assert len(INTERPRETIVE_RULES) == 25

    def test_every_rule_is_a_compiled_pattern_and_replacement(self):
        for rules in (CONSERVATIVE_RULES, INTERPRETIVE_RULES):
            for pattern, replacement in rules:
                assert hasattr(pattern, "sub")
                assert isinstance(replacement, str)


class TestEditorialCorrection:
    """`<X=Y>`: X is the editors' correction, Y is what is carved.

    EDCS documents this as "correction of an error in the inscription by the
    editors or normalization of spelling (example f<e=F>cit for FFCIT on the
    stone)". Conservative therefore keeps Y, interpretive keeps X.
    """

    def test_conservative_keeps_the_inscribed_letter(self):
        assert clean_conservative("ar<i=E>sta") == "arEsta"

    def test_interpretive_keeps_the_correction(self):
        assert clean_interpretive("ar<i=E>sta") == "arista"

    def test_documented_example(self):
        """f<e=F>cit reads 'fecit'; the stone carries FFCIT."""
        assert clean_interpretive("f<e=F>cit") == "fecit"
        assert clean_conservative("f<e=F>cit") == "fFcit"

    @pytest.mark.xfail(strict=True, reason="single-character rule; multi-letter spans unresolved")
    def test_multi_character_correction(self):
        assert clean_interpretive("no<vem=BAE>") == "novem"
        assert clean_conservative("no<vem=BAE>") == "noBAE"


class TestRestorationAndLineBreaks:
    def test_restoration_leaves_a_word_boundary(self):
        """Dropping [...] must not fuse the fragments either side of it."""
        assert clean_conservative("de[p]os(i)t(us)") == "de ost"

    def test_interpretive_keeps_restored_text(self):
        assert clean_interpretive("de[p]os(i)t(us)") == "depositus"

    def test_line_break_rejoins_a_split_word(self):
        """EDCS breaks words across lines with '/'; removing it rejoins them."""
        assert clean_conservative("Antonio Mar/tiali") == "Antonio Martiali"
        assert clean_interpretive("[co]niu/gi") == "coniugi"

    def test_line_break_between_words_keeps_them_apart(self):
        assert clean_interpretive("D(is) M(anibus) / L(ucio)") == "Dis Manibus Lucio"


class TestAbbreviationAndSuppression:
    def test_conservative_drops_expansions(self):
        assert clean_conservative("L(ucius) Aemilius") == "L Aemilius"

    def test_interpretive_keeps_expansions(self):
        assert clean_interpretive("L(ucius) Aemilius") == "Lucius Aemilius"

    def test_conservative_keeps_editorially_deleted_text(self):
        """{ } marks text the editors reject; it is still on the stone."""
        assert clean_conservative("{vacat} suppressed") == "vacat suppressed"

    def test_interpretive_drops_editorially_deleted_text(self):
        assert clean_interpretive("{vacat} suppressed") == "suppressed"


class TestIllegibleMarkers:
    def test_illegible_marker_is_removed(self):
        raw = "L(ucius) Aemilius +++ vixit an[nos] ++"
        assert "+" not in clean_conservative(raw)
        assert "+" not in clean_interpretive(raw)
        assert clean_interpretive(raw) == "Lucius Aemilius vixit annos"


class TestQueEnclitic:
    @pytest.mark.parametrize("word", ["populusque", "senatusque", "armaque"])
    def test_genuine_enclitic_is_split(self, word):
        assert clean_conservative(word) == word[:-3] + " que"

    @pytest.mark.parametrize(
        "word",
        ["atque", "neque", "usque", "quinque", "cuiusque", "denique", "itaque", "quisque"],
    )
    @pytest.mark.xfail(strict=True, reason="T05: needs a non-enclitic stoplist")
    def test_non_enclitic_is_left_alone(self, word):
        assert clean_conservative(word) == word


class TestNumeralVir:
    @pytest.mark.parametrize("word,expected", [("IIvir", "II vir"), ("VIvir", "VI vir")])
    def test_roman_numeral_prefix_is_split(self, word, expected):
        assert clean_conservative(word) == expected

    @pytest.mark.parametrize("word,expected", [("sevir", "se vir"), ("duovir", "duo vir")])
    @pytest.mark.xfail(strict=True, reason="T06: spelled-out numeral prefixes unhandled")
    def test_spelled_out_prefix_is_split(self, word, expected):
        assert clean_conservative(word) == expected


class TestNumerals:
    def test_arabic_digits_are_stripped(self):
        assert clean_conservative("anno 2024 die") == "anno die"

    @pytest.mark.xfail(strict=True, reason="T08: Roman numerals kept while digits are stripped")
    def test_numeral_handling_is_symmetric(self):
        assert clean_conservative("vixit annos XXV") == "vixit annos"


class TestUnhandledMarkup:
    """EDCS conventions the current rules do not yet address."""

    @pytest.mark.xfail(strict=True, reason="erasure marker not handled")
    def test_erasure_marker_is_removed(self):
        assert "⟦" not in clean_conservative("⟦Domitiani⟧ erasa")
        assert "⟧" not in clean_conservative("⟦Domitiani⟧ erasa")

    @pytest.mark.xfail(strict=True, reason="antique insertion marker not handled")
    def test_antique_insertion_marker_is_removed(self):
        assert "«" not in clean_interpretive("«Constantino»")

    @pytest.mark.xfail(strict=True, reason="'|' introduces a symbol, it is not a line break")
    def test_symbol_marker_survives_conservative_cleaning(self):
        """`|(centurio)` is a symbol carved on the stone, not an expansion.

        The '|' is currently deleted as a line break and the remainder is then
        treated as an editorial expansion, so conservative loses the symbol
        entirely. Interpretive happens to yield the right string by accident.
        """
        assert clean_conservative("|(centurio)") != ""
        assert clean_conservative("|(denarius) V") != "V"

    @pytest.mark.xfail(strict=True, reason="T09: needs a balanced-delimiter parser")
    def test_nested_parens_are_parsed_as_a_unit(self):
        assert clean_conservative("(A(uli) f(ilius))") == ""


class TestIsForged:
    def test_forged_flag_fires_on_the_edcs_marker(self):
        """EDCS marks falsae with '*' before the number for CIL, after it otherwise."""
        data = pd.DataFrame({
            "inscription_text": ["falsa", "vera", "falsa quoque"],
            "evidence": [["CIL 06, *00226"], ["CIL 06, 00002"], ["RIB-03, 03534*"]],
        })
        assert clean_data(data, verbose=False)["is_forged"].tolist() == [True, False, True]

    def test_forged_is_false_without_the_marker(self):
        data = pd.DataFrame({
            "inscription_text": ["abc", "def"],
            "evidence": [["CIL 16, 00041"], ["AE 1913, 00179"]],
        })
        assert not clean_data(data, verbose=False)["is_forged"].any()

    def test_missing_evidence_column_is_tolerated(self):
        data = pd.DataFrame({"inscription_text": ["abc", "def"]})
        cleaned = clean_data(data, verbose=False)
        assert "evidence" in cleaned.columns
        assert not cleaned["is_forged"].any()


class TestPipelineInvariants:
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

    def test_interpretive_expands_where_conservative_drops(self):
        raw = "L(ucius) Aemilius Paullus co(n)s(ul)"
        assert len(clean_interpretive(raw)) > len(clean_conservative(raw))

    def test_is_unreadable_flag(self):
        data = pd.DataFrame({"inscription_text": ["", "?", "L(ucius)"]})
        assert clean_data(data, verbose=False)["is_unreadable"].tolist() == [True, True, False]

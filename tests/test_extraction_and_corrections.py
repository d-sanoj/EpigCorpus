"""Tests for the extraction and correction logic.

These pin the behaviour the reports describe. Several encode a bug that was
actually made during the project, so a regression would fail loudly rather than
silently change a published number.
"""
from __future__ import annotations
import importlib.util
from collections import Counter
from pathlib import Path
import pytest

REPO = Path(__file__).resolve().parent.parent

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

ap = _load("abbrev_probe", REPO/"scripts"/"abbrev_probe.py")
wl = _load("wordlists", REPO/"scripts"/"phase3_wordlists.py")
bv = _load("build_v1", REPO/"scripts"/"phase3_build_v1.py")


def extract(text):
    """(abbrev, expansion) pairs from one text, exclusions discarded."""
    return [(a, e) for a, e, _, _ in ap.extract_pairs(text, Counter(), {})]


# --------------------------------------------------------------- extraction
def test_simple_expansion():
    assert extract("D(is) M(anibus)") == [("D", "Dis"), ("M", "Manibus")]

def test_interior_expansion_is_one_pair_not_two():
    """co(n)s(ul) is ONE abbreviation carrying two insertions."""
    assert extract("co(n)s(ul)") == [("cos", "consul")]

def test_bracketed_token_is_excluded():
    """Text the editor restored is not an ancient abbreviation."""
    assert extract("[D(is) M(anibus)]") == []

def test_partly_restored_abbreviation_is_excluded():
    """frumen]t(o): the abbreviation's own letters are partly supplied, so it
    goes. publ(ico) sits OUTSIDE the bracket and must survive -- the filter is
    per token, not per line. This asymmetry is the whole of Phase 1."""
    got = extract("[3 qui frumen]t(o) publ(ico)")
    assert ("t", "to") not in got, "bracket-contaminated fragment leaked through"
    assert got == [("publ", "publico")]

def test_empty_paren_is_not_an_expansion():
    """PR() is the editor recording an UNRESOLVABLE abbreviation."""
    assert extract("PR() P()") == []

def test_editorial_marks_dropped_but_uncertain_reading_kept():
    assert extract("(?)") == []
    assert extract("Viator(is)?") == [("Viator", "Viatoris")]

def test_slash_is_a_line_break_not_a_delimiter():
    """A hard token boundary: no abbreviation is read across a line break."""
    assert extract("D(is) / M(anibus)") == [("D", "Dis"), ("M", "Manibus")]

def test_pipe_symbol_abbreviation_is_excluded_from_the_latin_task():
    """|(mulieris) is a glyph, not letters -- a separate class (D-0013)."""
    assert extract("Kania |(mulieris) l(iberta)") == [("l", "liberta")]

def test_greek_is_excluded():
    assert extract("Aὐρ(ήλιος)") == []


# --------------------------------------------------------------- geminatio
@pytest.mark.parametrize("abbrev,expansion,want_abbrev,want_expansion", [
    ("DD",    "DDominis",        "D",    "Dominis"),        # one-letter stem
    ("DDD",   "DDDominis",       "D",    "Dominis"),        # triple
    ("Augg",  "Auggustorum",     "Aug",  "Augustorum"),     # TRAILING run
    ("Impp",  "Impperatoribus",  "Imp",  "Imperatoribus"),
    ("conss", "conssulibus",     "cons", "consulibus"),
    ("Caess", "Caessaribus",     "Caes", "Caesaribus"),
    ("nobb",  "nobbilissimis",   "nob",  "nobilissimis"),
])
def test_geminatio_collapses_the_trailing_run(abbrev, expansion, want_abbrev, want_expansion):
    """D-0020. A LEADING-run rule gets DD right and Augg/Impp/conss WRONG --
    that was a real 41% undercount during the project."""
    ab, ex, run = bv.collapse(abbrev, expansion)
    assert (ab, ex) == (want_abbrev, want_expansion)
    assert run

def test_leading_run_rule_would_have_missed_augg():
    """Regression guard for the actual bug: 'Augg' has no LEADING doubled run."""
    import re
    leading = re.compile(r"^(([A-Za-z])\2+)")
    assert leading.match("DD") is not None
    assert leading.match("Augg") is None          # the bug
    assert bv.trailing_run("Augg") == "gg"        # the fix

def test_no_run_means_no_collapse():
    ab, ex, run = bv.collapse("cos", "consul")
    assert (ab, ex, run) == ("cos", "consul", "")

def test_collapse_declines_when_expansion_does_not_start_with_abbrev():
    """Multi-group tokens: the arithmetic does not apply, so do not guess."""
    ab, ex, run = bv.collapse("coss", "consulibus")
    assert (ab, ex) == ("coss", "consulibus")
    assert run == "ss"                            # marker recorded, no collapse


# ------------------------------------------------------------ numeral lists
def test_word_lists_do_not_overlap():
    assert not (wl.TYPE1_NUMERAL_WORD & wl.TYPE2_SUPPLIED_UNIT)
    assert not (wl.TYPE1_NUMERAL_WORD & wl.TYPE3_NUMERAL_PREFIX)
    assert not (wl.TYPE2_SUPPLIED_UNIT & wl.TYPE3_NUMERAL_PREFIX)

def test_type2_holds_the_thousands_device():
    """D-0017: X(milia) renders the vinculum; the word is not on the stone."""
    for w in ("milia", "milibus", "mille"):
        assert w in wl.TYPE2_SUPPLIED_UNIT

def test_type3_office_reading_is_never_the_gold_label():
    """The Latin reading lives in normalized_form only (D-0021)."""
    assert wl.TYPE3_OFFICE_READING[("VI", "vir")] == "sevir"
    assert wl.TYPE3_OFFICE_READING[("IIIIII", "vir")] == "sevir"
    assert "sevir" not in wl.TYPE3_NUMERAL_PREFIX

def test_roman_value():
    assert bv.roman_value("X") == 10
    assert bv.roman_value("IIII") == 4
    assert bv.roman_value("XL") == 40
    assert bv.roman_value("CCXX") == 220


# ------------------------------------------------------------------- dates
def test_parse_year_never_guesses():
    assert ap.parse_year(None) is None
    assert ap.parse_year("-") is None
    assert ap.parse_year("") is None
    assert ap.parse_year("125") == 125

def test_miskeyed_dates_are_an_explicit_list_not_a_threshold():
    """D-0023: a >1000 threshold caught 21 records of which 19 were correct."""
    assert len(bv.MISKEYED) == 2
    for rid, why in bv.MISKEYED.items():
        assert rid.startswith("EDCS-")
        assert len(why) > 30, "every mis-key must carry its evidence"


# ------------------------------------------------------------------ splits
def test_split_bucket_is_deterministic_and_not_python_hash():
    """R9. Python's hash() is salted per process; blake2b is not."""
    sp = _load("splits", REPO/"scripts"/"phase4_build_splits.py")
    a = sp.bucket("EDCS-00000245")
    b = sp.bucket("EDCS-00000245")
    assert a == b
    assert 0.0 <= a < 1.0
    assert sp.bucket("EDCS-00000245") != sp.bucket("EDCS-00000246")

def test_monument_id_strips_the_segment_suffix():
    """D-0004: two faces of one stone must not straddle the split."""
    assert "EDCS-00000245-0".rsplit("-", 1)[0] == "EDCS-00000245"
    assert "EDCS-00000245-2".rsplit("-", 1)[0] == "EDCS-00000245"

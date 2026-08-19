"""EDCS inscription text cleaning pipeline.

Two variants are produced for every inscription:

* **conservative** — approximates what is actually carved on the stone.
  Editorial expansions are dropped, and where the editors corrected the text
  the *inscribed* letters are kept.
* **interpretive** — the editors' reading. Expansions are kept, and where the
  text was corrected the *corrected* letters are kept.

Each variant is an ordered list of regex substitutions applied in sequence;
order is significant, because later rules assume earlier ones have run. The
rule sets below are 27 and 25 substitutions respectively.

Markup handled here (EDCS conventions):

    /          line break
    ( )        expansion of an abbreviation
    [ ]        editorial supplement of lost text
    [3] [6]    gap within a line / gap the length of a line
    { }        text the editors judge should be deleted
    <X=Y>      editorial correction: X is the correction, Y is on the stone
    ̣           underdot: letter read with uncertainty

KNOWN LIMITATIONS (deliberate, pending the next revision):

    * The `[1]` rule is a character class, so it removes every digit ``1``
      anywhere in the text, not just the ``[1]`` marker.
    * `<X=Y>` matches single characters only; multi-letter corrections such as
      ``no<vem=BAE>`` are left partially resolved.
    * ``|`` is treated as a line break, though EDCS uses it to introduce
      symbols such as ``|(centurio)``.
    * ``-que`` is split on every occurrence, including non-enclitics such as
      ``atque`` and ``neque``.
    * Numeral-plus-``vir`` splitting covers only I/V/X, so ``sevir`` and
      ``duovir`` are left untouched.
    * Roman numerals survive while Arabic digits are removed.
    * Erasure (``⟦ ⟧``), antique insertion (``« »``) and the ``(!)`` spelling
      flag are not handled.

    See docs/CLEANING_COMPARISON.md for measurements of each.
"""

import re

import pandas as pd

# Superscript digits that may follow a suppression to index it.
_SUPERSCRIPTS = "⁰¹²³⁴⁵⁶⁷⁸⁹"

# Greek and Latin letters accepted on either side of an editorial correction.
_LETTER = "α-ωΑ-Ωa-zA-Z"


def _rule(pattern, replacement):
    return (re.compile(pattern, re.UNICODE), replacement)


# ─── CONSERVATIVE — 27 rules, applied in order ────────────────────────────────
CONSERVATIVE_RULES = [
    # 1. underdot marking an uncertainly read letter
    _rule("\u0323", r""),
    # 2-5. gap of indeterminate length within a line, incl. unclosed forms
    _rule(r"\[3\]", r"[-] "),
    _rule(r"3\]", r"-] "),
    _rule(r"\[3", r" [-"),
    _rule(r"(\[\w+)( [3] )(\w+\])", r" \1 \3 "),
    # 6. gap the length of a whole line
    _rule(r"\[6\]", r"[-] "),
    # 7. gap marker of a single character
    _rule(r"[1]", r" "),
    # 8-9. stray quotes and escaped backslashes
    _rule("\u0022", r" "),
    _rule(r"\\", r" "),
    # 10. expansion of an abbreviation — dropped: not on the stone
    _rule(r"\([^(]*\)", r""),
    # 11-12. editorial deletion; the enclosed text IS on the stone, so keep it
    _rule(r"{[^}]*}[" + _SUPERSCRIPTS + r"]+", r""),
    _rule(r"[\{*\}]", r""),
    # 13. editorial supplement of lost text — dropped, leaving a word boundary
    _rule(r"\[[^[]*\]", r" "),
    # 14-15. editorial correction <correction=inscribed>: keep what is inscribed
    _rule(r"(\<)([" + _LETTER + r"])=([" + _LETTER + r"])(\>)", r"\3"),
    _rule(r"(\<)([" + _LETTER + r"])*=([" + _LETTER + r"])(\>)", r"\3"),
    # 16. any remaining angle-bracket markup
    _rule(r"\<[^<]*\>", r""),
    # 17. line break — removed so words split across lines rejoin
    _rule(r"[\||\/|\/\/]", r""),
    # 18. interpunction and editorial symbols
    _rule(r"[=\+\,|\.|․|:|⋮|⁙|;|!|\-|—|–|#|%|\^|&|\~|@]", r" "),
    # 19. epigraphic and metrical symbols
    #     NOTE: covers U+0387 (Greek ano teleia), not U+00B7 (Latin
    #     interpunct); U+00B7 occurs in 4 inscriptions and is left in place.
    _rule(
        "[\u2766|\u0387|\u2219|\U00010196|\u23d1|\u23d3|\u23d5]",
        r"",
    ),
    # 20. uncertainty marker
    _rule(r"[\\?]", r""),
    # 21. Arabic numerals
    _rule(r"[0-9]+", r""),
    # 22. brackets left unbalanced by the rules above
    _rule(r"[\[|\{|\(|\)|\}|\]]", r""),
    # 23. enclitic -que separated into its own token
    _rule(r"(\w+)(que)\b", r"\1 \2"),
    # 24. numeral prefix separated from the -vir title
    _rule(r"([I|V|X])(vir*)", r"\1 \2"),
    # 25-27. whitespace normalisation
    _rule(r"[ ]+", r" "),
    _rule(r"\s+", r" "),
    _rule(r"(^\s|\s$)", r""),
]

# ─── INTERPRETIVE — 25 rules, applied in order ────────────────────────────────
INTERPRETIVE_RULES = [
    # 1. underdot marking an uncertainly read letter
    _rule("\u0323", r""),
    # 2-4. gap of indeterminate length within a line, incl. unclosed forms
    _rule(r"\[3\]", r"[-]"),
    _rule(r"3\]", r"-]"),
    _rule(r"\[3", r"[-"),
    # 5. gap the length of a whole line
    _rule(r"\[6\]", r"[-]"),
    # 6. gap marker of a single character
    _rule(r"[1]", r" "),
    # 7-8. stray quotes and escaped backslashes
    _rule("\u0022", r" "),
    _rule(r"\\", r" "),
    # 9. expansion of an abbreviation — kept: this is the editors' reading
    _rule(r"[\(*\)]", r""),
    # 10. editorial deletion — removed, since the editors reject this text
    _rule(r"{[^}]*}", r""),
    # 11. editorial supplement of lost text — kept
    _rule(r"[\[*\]]", r""),
    # 12-13. editorial correction <correction=inscribed>: keep the correction
    _rule(r"([" + _LETTER + r"])=([" + _LETTER + r"])", r"\1"),
    _rule(r"([" + _LETTER + r"])*=([" + _LETTER + r"])", r"\2"),
    # 14. any remaining angle-bracket markup
    _rule(r"[\<*\>]", r""),
    # 15. line break — removed so words split across lines rejoin
    _rule(r"[\||\/|\/\/]", r""),
    # 16. interpunction and editorial symbols
    _rule(r"[=\+\,|\.|․|:|⋮|⁙|;|!|\-|—|–|#|%|\^|&|\~|@]", r" "),
    # 17. epigraphic and metrical symbols (see conservative rule 19)
    _rule(
        "[\u2766|\u0387|\u2219|\U00010196|\u23d1|\u23d3|\u23d5]",
        r"",
    ),
    # 18. uncertainty marker
    _rule(r"[\\?]", r""),
    # 19. Arabic numerals
    _rule(r"[0-9]+", r""),
    # 20. brackets left unbalanced by the rules above
    _rule(r"[\[|\{|\(|\)|\}|\]]", r""),
    # 21. enclitic -que separated into its own token
    _rule(r"(\w+)(que)\b", r"\1 \2"),
    # 22. numeral prefix separated from the -vir title
    _rule(r"([I|V|X])(vir*)", r"\1 \2"),
    # 23-25. whitespace normalisation
    _rule(r"[ ]+", r" "),
    _rule(r"\s+", r" "),
    _rule(r"(^\s|\s$)", r""),
]


def apply_rules(text: str, rules) -> str:
    """Apply an ordered rule list to one inscription. Order is significant."""
    for pattern, replacement in rules:
        text = pattern.sub(replacement, text)
    return text


def clean_conservative(raw: str) -> str:
    """Approximate the letters actually carved on the stone."""
    return apply_rules(raw, CONSERVATIVE_RULES)


def clean_interpretive(raw: str) -> str:
    """Produce the editors' reading, with abbreviations expanded."""
    return apply_rules(raw, INTERPRETIVE_RULES)


def clean_data(data: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Clean and enhance inscription data.

    Args:
        data: DataFrame with an 'inscription_text' column
        verbose: Print detailed statistics if True

    Returns:
        Cleaned DataFrame with additional columns:
        - inscription_text_conservative: letters as carved
        - inscription_text_interpretive: the editors' reading
        - is_unreadable: Boolean flag for inscriptions with no legible text
        - is_forged: Boolean flag, set when a citation carries the '*' marker
          EDCS uses for falsae (before the number for CIL, after it otherwise)
    """
    raw_series = data["inscription_text"].fillna("").astype(str)

    cleaned = data.copy()
    if "evidence" not in cleaned.columns:
        cleaned["evidence"] = ""

    cleaned["inscription_text_conservative"] = raw_series.map(clean_conservative)
    cleaned["inscription_text_interpretive"] = raw_series.map(clean_interpretive)
    cleaned["is_unreadable"] = raw_series.map(lambda s: s.strip() in ("", "?"))
    cleaned["is_forged"] = (
        cleaned["evidence"].fillna("").astype(str).str.contains("*", regex=False)
    )

    if verbose:
        forged = int(cleaned["is_forged"].sum())
        unreadable = int(cleaned["is_unreadable"].sum())
        print(f"Forged: {forged}\nUnreadable: {unreadable}")

        raw = cleaned["inscription_text"].fillna("").astype(str).str.strip()
        cons = cleaned["inscription_text_conservative"].fillna("").astype(str).str.strip()
        interp = cleaned["inscription_text_interpretive"].fillna("").astype(str).str.strip()

        raw_nonempty_mask = raw.ne("")
        raw_nonempty = int(raw_nonempty_mask.sum())
        cons_became_empty = int((raw_nonempty_mask & cons.eq("")).sum())
        interp_became_empty = int((raw_nonempty_mask & interp.eq("")).sum())

        summary = pd.DataFrame(
            {"before pd.NA": [raw_nonempty, cons_became_empty, interp_became_empty]},
            index=["raw inscription", "conservative", "interpretive"],
        )
        summary["after pd.NA"] = [
            int(raw.eq("").sum()),
            int(cons.eq("").sum()),
            int(interp.eq("").sum()),
        ]
        print("\nSummary of raw inscriptions before and after pd.NA:")
        print(summary)

        missing_counts = cleaned.isna().sum()
        missing_df = (
            missing_counts[missing_counts > 0]
            .sort_values(ascending=False)
            .rename("missing_count")
            .to_frame()
        )
        if not missing_df.empty:
            print("\nMissing values per column:")
            print(missing_df)

    cleaned = cleaned.replace(r"^\s*$", pd.NA, regex=True)

    if verbose:
        null_info = cleaned.isna().sum()
        null_columns = pd.DataFrame({
            "null_count": null_info.astype("int64"),
            "rows": len(cleaned),
        })
        null_columns["null_pct"] = (null_columns["null_count"] / len(cleaned) * 100).round(2)
        null_report = null_columns[null_columns["null_count"] > 0].sort_values(
            "null_count", ascending=False
        )
        if not null_report.empty:
            print("\nNull report:")
            print(null_report)

    return cleaned

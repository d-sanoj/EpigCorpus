"""The cleaning use case: raw corpus in, conservative and interpretive variants out.

The rule sets themselves are pure and live in epigcorpus.core.cleaning.
"""

import pandas as pd

from epigcorpus.core.cleaning import clean_conservative, clean_interpretive
from epigcorpus.services.ports import RawCorpusStore


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


def run_cleaning(store: RawCorpusStore) -> int:
    """Clean the newest scraped corpus chunk by chunk and save it; return the rows.

    Every rule in clean_data works on one row at a time, so cleaning chunk by
    chunk writes exactly what cleaning the whole frame wrote (the full corpus
    gives the same sha256) while holding one chunk instead of the corpus.
    """
    forged = unreadable = 0
    with store.cleaned_writer() as writer:
        for chunk in store.iter_latest_data():
            cleaned = clean_data(chunk, verbose=False)
            forged += int(cleaned["is_forged"].sum())
            unreadable += int(cleaned["is_unreadable"].sum())
            writer.write(cleaned)
        rows = writer.rows
    print(f"Cleaned: {rows:,} rows\nForged: {forged}\nUnreadable: {unreadable}")
    return rows

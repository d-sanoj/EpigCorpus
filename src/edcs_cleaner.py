"""EDCS inscription text cleaning pipeline."""

import re
import pandas as pd


def step1_dubious_dot(text: str) -> str:
    """Remove dubious dot character."""
    return text.replace("\u0323", "")


def step2_edcs_gaps(text: str) -> str:
    """Replace EDCS gap markers with standard gap notation."""
    text = re.sub(r"\[6\]", "[-]", text)
    text = re.sub(r"\[3\]", "[-]", text)
    text = re.sub(r"\[1\]", " ", text)
    return text


def step3_quotes_backslashes(text: str) -> str:
    """Remove quotes and backslashes."""
    return text.replace("\\", "").replace('"', "").replace("'", "")


def step4_conservative(text: str) -> str:
    """Extract conservative (minimal) interpretation by removing uncertain content."""
    text = re.sub(r"\([^)]*\)", "", text)  # Remove parentheses content
    text = re.sub(r"\{([^}]*)\}", r"\1", text)  # Keep curly brace content, remove braces
    text = re.sub(r"\[[^\]]*\]", "", text)  # Remove brackets content
    text = re.sub(r"<([^=>]*)=[^>]*>", r"\1", text)  # Extract attribute name from equality markup
    text = re.sub(r"<[^>]*>", "", text)  # Remove remaining tags
    return text


def step4_interpretive(text: str) -> str:
    """Extract interpretive (maximal) interpretation by including uncertain content."""
    text = re.sub(r"\(([^)]*)\)", r"\1", text)  # Keep parentheses content, remove parens
    text = re.sub(r"\[([^\]]*)\]", r"\1", text)  # Keep brackets content, remove brackets
    text = re.sub(r"\{[^}]*\}", "", text)  # Remove curly brace content
    text = re.sub(r"<[^=><]*=([^>]*)>", r"\1", text)  # Extract content from equality markup
    text = re.sub(r"<([^>]*)>", r"\1", text)  # Keep tag content, remove tags
    return text


def step5_line_breaks(text: str) -> str:
    """Replace line break markers with spaces."""
    return text.replace("/", " ")


def step6_punctuation_symbols(text: str) -> str:
    """Remove punctuation and special symbols."""
    text = re.sub(r"[,\.\-\u2014:;!#%\^&~@]", "", text)
    text = re.sub(r"[\u2766\u00b7\u2219]", "", text)
    return text


def step7_uncertainty(text: str) -> str:
    """Remove uncertainty markers."""
    return text.replace("?", "")


def step8_arabic_numerals(text: str) -> str:
    """Remove all digits."""
    return re.sub(r"[0-9]", "", text)


def step9_unclosed_brackets(text: str) -> str:
    """Remove any remaining unclosed brackets."""
    return re.sub(r"[\[\]\{\}()]", "", text)


def step10_que_enclitic(text: str) -> str:
    """Add space before 'que' enclitic for proper word separation."""
    return re.sub(r"(?<=[A-Za-z])(que)(?=\s|$)", r" \1", text)


def step11_numeral_vir(text: str) -> str:
    """Add space between Roman numerals and 'vir' suffix."""
    return re.sub(r"([IVXLCDMivxlcdm]+)(vir\w*)", r"\1 \2", text)


def step12_collapse_spaces(text: str) -> str:
    """Collapse multiple spaces into single spaces."""
    return re.sub(r"\s+", " ", text)


def step13_strip(text: str) -> str:
    """Strip leading and trailing whitespace."""
    return text.strip()


def clean_conservative(raw: str) -> str:
    """Apply full cleaning pipeline with conservative text extraction."""
    t = raw
    t = step1_dubious_dot(t)
    t = step2_edcs_gaps(t)
    t = step3_quotes_backslashes(t)
    t = step4_conservative(t)
    t = step5_line_breaks(t)
    t = step6_punctuation_symbols(t)
    t = step7_uncertainty(t)
    t = step8_arabic_numerals(t)
    t = step9_unclosed_brackets(t)
    t = step10_que_enclitic(t)
    t = step11_numeral_vir(t)
    t = step12_collapse_spaces(t)
    t = step13_strip(t)
    return t


def clean_interpretive(raw: str) -> str:
    """Apply full cleaning pipeline with interpretive (maximalist) text extraction."""
    t = raw
    t = step1_dubious_dot(t)
    t = step2_edcs_gaps(t)
    t = step3_quotes_backslashes(t)
    t = step4_interpretive(t)
    t = step5_line_breaks(t)
    t = step6_punctuation_symbols(t)
    t = step7_uncertainty(t)
    t = step8_arabic_numerals(t)
    t = step9_unclosed_brackets(t)
    t = step10_que_enclitic(t)
    t = step11_numeral_vir(t)
    t = step12_collapse_spaces(t)
    t = step13_strip(t)
    return t


def clean_data(data: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Clean and enhance inscription data.
    
    Args:
        data: DataFrame with 'inscription_text' column
        verbose: Print detailed statistics if True
    
    Returns:
        Cleaned DataFrame with additional columns:
        - inscription_text_conservative: Conservative interpretation
        - inscription_text_interpretive: Maximalist interpretation
        - is_unreadable: Boolean flag for unreadable inscriptions
        - is_forged: Boolean flag for forged inscriptions (based on evidence)
    """
    raw_series = data["inscription_text"].fillna("").astype(str)

    cleaned = data.copy()
    if "evidence" not in cleaned.columns:
        cleaned["evidence"] = ""
    
    cleaned["inscription_text_conservative"] = raw_series.map(clean_conservative)
    cleaned["inscription_text_interpretive"] = raw_series.map(clean_interpretive)
    cleaned["is_unreadable"] = raw_series.map(lambda s: s.strip() in ("", "?"))
    cleaned["is_forged"] = cleaned["evidence"].fillna("").astype(str).str.contains("*", regex=False)

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
        null_report = null_columns[null_columns["null_count"] > 0].sort_values("null_count", ascending=False)
        if not null_report.empty:
            print("\nNull report:")
            print(null_report)

    return cleaned

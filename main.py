from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
DATA_DIR = ROOT_DIR / "data"


def print_table(title: str, df: pd.DataFrame, index: bool = True, max_rows: int | None = None) -> None:
    """Render a DataFrame as a readable fixed-width table in terminal."""
    view = df if max_rows is None else df.head(max_rows)
    width = shutil.get_terminal_size(fallback=(120, 40)).columns
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        max(80, width - 2),
        "display.max_colwidth",
        64,
        "display.expand_frame_repr",
        False,
    ):
        print(f"\n{title}")
        print(view.to_string(index=index))


def build_terminal_preview(df: pd.DataFrame, max_rows: int = 10) -> pd.DataFrame:
    """Create a compact, readable subset for terminal display."""
    preferred_cols = [
        "record_id",
        "inscription_text_interpretive",
        "is_unreadable",
        "is_forged",
    ]
    cols = [c for c in preferred_cols if c in df.columns]
    preview = df[cols].head(max_rows).copy()

    # Keep long inscription text readable in fixed-width terminal output.
    if "inscription_text_interpretive" in preview.columns:
        preview["inscription_text_interpretive"] = (
            preview["inscription_text_interpretive"]
            .fillna("")
            .astype(str)
            .map(lambda s: s if len(s) <= 34 else s[:31] + "...")
        )

    rename_map = {
        "record_id": "record",
        "inscription_text_interpretive": "interpretive_text",
        "is_unreadable": "unreadable",
        "is_forged": "forged",
    }
    preview = preview.rename(columns=rename_map)

    return preview


def get_jsonl_files() -> list[Path]:
    return sorted(DATA_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)


def run_scripts(run_all_scripts: bool = False) -> None:
    """Run scraper scripts under src/.

    By default, this runs only edcs_scraper.py to avoid launching map/interactive scripts.
    Set run_all_scripts=True to match the notebook behavior exactly.
    """
    if run_all_scripts:
        script_paths = sorted(SRC_DIR.glob("*.py"))
    else:
        script_paths = [SRC_DIR / "edcs_scraper.py"]

    script_paths = [p for p in script_paths if p.exists()]
    if not script_paths:
        raise FileNotFoundError("No Python scripts found to run in 'src/'.")

    for script in script_paths:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            env={**os.environ.copy(), **{"MPLBACKEND": "Agg"}},
            cwd=str(ROOT_DIR),
        )
        print(f"Ran: {script.name}")
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise RuntimeError(f"Script failed: {script.name}")


def load_latest_data() -> pd.DataFrame:
    jsonl_files = get_jsonl_files()
    if not jsonl_files:
        raise FileNotFoundError("No JSONL file found in 'data/' after running scraper.")

    jsonl_path = jsonl_files[0]
    data = pd.read_json(jsonl_path, lines=True)
    print(f"Loaded: {jsonl_path}")

    if "belege" in data.columns:
        data = data.rename(columns={"belege": "evidence"})

    return data


def step1_dubious_dot(text: str) -> str:
    return text.replace("\u0323", "")


def step2_edcs_gaps(text: str) -> str:
    text = re.sub(r"\[6\]", "[-]", text)
    text = re.sub(r"\[3\]", "[-]", text)
    text = re.sub(r"\[1\]", " ", text)
    return text


def step3_quotes_backslashes(text: str) -> str:
    return text.replace("\\", "").replace('"', "").replace("'", "")


def step4_conservative(text: str) -> str:
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\{([^}]*)\}", r"\1", text)
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"<([^=>]*)=[^>]*>", r"\1", text)
    text = re.sub(r"<[^>]*>", "", text)
    return text


def step4_interpretive(text: str) -> str:
    text = re.sub(r"\(([^)]*)\)", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]", r"\1", text)
    text = re.sub(r"\{[^}]*\}", "", text)
    text = re.sub(r"<[^=><]*=([^>]*)>", r"\1", text)
    text = re.sub(r"<([^>]*)>", r"\1", text)
    return text


def step5_line_breaks(text: str) -> str:
    return text.replace("/", " ")


def step6_punctuation_symbols(text: str) -> str:
    text = re.sub(r"[,\.\-\u2014:;!#%\^&~@]", "", text)
    text = re.sub(r"[\u2766\u00b7\u2219]", "", text)
    return text


def step7_uncertainty(text: str) -> str:
    return text.replace("?", "")


def step8_arabic_numerals(text: str) -> str:
    return re.sub(r"[0-9]", "", text)


def step9_unclosed_brackets(text: str) -> str:
    return re.sub(r"[\[\]\{\}()]", "", text)


def step10_que_enclitic(text: str) -> str:
    return re.sub(r"(?<=[A-Za-z])(que)(?=\s|$)", r" \1", text)


def step11_numeral_vir(text: str) -> str:
    return re.sub(r"([IVXLCDMivxlcdm]+)(vir\w*)", r"\1 \2", text)


def step12_collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def step13_strip(text: str) -> str:
    return text.strip()


def clean_conservative(raw: str) -> str:
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


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    raw_series = data["inscription_text"].fillna("").astype(str)

    cleaned = data.copy()
    if "evidence" not in cleaned.columns:
        cleaned["evidence"] = ""
    cleaned["inscription_text_conservative"] = raw_series.map(clean_conservative)
    cleaned["inscription_text_interpretive"] = raw_series.map(clean_interpretive)
    cleaned["is_unreadable"] = raw_series.map(lambda s: s.strip() in ("", "?"))
    cleaned["is_forged"] = cleaned["evidence"].fillna("").astype(str).str.contains("*", regex=False)

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
    print_table("Summary and raw inscriptions before and after pd.NA:", summary)

    missing_counts = cleaned.isna().sum()
    missing_df = (
        missing_counts[missing_counts > 0]
        .sort_values(ascending=False)
        .rename("missing_count")
        .to_frame()
    )
    print_table("Missing values per column:", missing_df)

    cleaned = cleaned.replace(r"^\s*$", pd.NA, regex=True)

    null_info = cleaned.isna().sum()
    null_columns = pd.DataFrame({
        "null_count": null_info.astype("int64"),
        "rows": len(cleaned),
    })
    null_columns["null_pct"] = (null_columns["null_count"] / len(cleaned) * 100).round(2)
    null_report = null_columns[null_columns["null_count"] > 0].sort_values("null_count", ascending=False)
    print_table("Null report:", null_report)

    return cleaned


def save_cleaned_data(cleaned: pd.DataFrame) -> tuple[Path, Path]:
    output_jsonl = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"
    output_csv = DATA_DIR / "edcs_inscriptions_cleaned.csv"

    cleaned.to_json(output_jsonl, orient="records", lines=True, force_ascii=False)
    cleaned.to_csv(output_csv, index=False)

    print(f"Saved cleaned JSONL: {output_jsonl}")
    print(f"Saved cleaned CSV: {output_csv}")
    return output_jsonl, output_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EDCS scrape + cleaning pipeline from notebook logic.")
    parser.add_argument(
        "--run-all-scripts",
        action="store_true",
        help="Run every Python file in src/ before loading data (matches notebook behavior).",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip running scripts and only clean the latest data/*.jsonl file.",
    )
    return parser


def main(run_all_scripts: bool = False, skip_scrape: bool = False) -> pd.DataFrame:
    if not skip_scrape:
        run_scripts(run_all_scripts=run_all_scripts)

    data = load_latest_data()
    cleaned = clean_data(data)
    save_cleaned_data(cleaned)
    return cleaned


if __name__ == "__main__":
    args = build_parser().parse_args()
    cleaned_data = main(run_all_scripts=args.run_all_scripts, skip_scrape=args.skip_scrape)
    terminal_preview = build_terminal_preview(cleaned_data, max_rows=10)
    print_table("Cleaned data preview (first 10 rows):", terminal_preview, index=False)

"""EDCS Analytics main orchestration: scrape → clean → launch Streamlit map."""

import argparse
import os
import subprocess
import sys
import shutil
from pathlib import Path

import pandas as pd

from src.edcs_cleaner import clean_data


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
    """Get corpus files in data directory, newest first.

    Matches both plain .jsonl and gzipped .jsonl.gz. A fresh clone contains only
    the gzipped snapshot; running the scraper produces the plain file. Plain
    files sort ahead of their own .gz at equal mtime so a freshly harvested
    corpus wins over a committed snapshot.
    """
    files = list(DATA_DIR.glob("*.jsonl")) + list(DATA_DIR.glob("*.jsonl.gz"))
    return sorted(
        files,
        key=lambda p: (p.stat().st_mtime, p.suffix != ".gz"),
        reverse=True,
    )


def run_scraper() -> None:
    """Run the EDCS scraper script."""
    scraper_script = SRC_DIR / "edcs_scraper.py"
    
    if not scraper_script.exists():
        raise FileNotFoundError(f"Scraper script not found: {scraper_script}")
    
    print(f"\n{'='*70}")
    print("STEP 1: SCRAPING EDCS DATA")
    print(f"{'='*70}\n")
    
    result = subprocess.run(
        [sys.executable, str(scraper_script)],
        capture_output=True,
        text=True,
        env={**os.environ.copy(), **{"MPLBACKEND": "Agg"}},
        cwd=str(ROOT_DIR),
    )
    
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise RuntimeError(f"Scraper failed with exit code {result.returncode}")


def load_latest_data() -> pd.DataFrame:
    """Load the most recently created JSONL file from data directory."""
    jsonl_files = get_jsonl_files()
    if not jsonl_files:
        raise FileNotFoundError("No JSONL file found in 'data/' after running scraper.")

    jsonl_path = jsonl_files[0]
    # pandas infers gzip from the .gz extension, so both forms load identically.
    data = pd.read_json(jsonl_path, lines=True)
    print(f"\nLoaded: {jsonl_path}")

    if "belege" in data.columns:
        data = data.rename(columns={"belege": "evidence"})

    return data


def save_cleaned_data(cleaned: pd.DataFrame) -> Path:
    """Save cleaned data as JSONL, plus a .gz copy for distribution.

    The plain file is what the app reads locally; the .gz is what gets committed
    (it is roughly 11x smaller, keeping it under GitHub's 100 MB per-file limit
    without Git LFS). Loaders accept either, so nobody has to decompress by hand.
    """
    output_jsonl = DATA_DIR / "edcs_inscriptions_cleaned.jsonl"
    output_gz = DATA_DIR / "edcs_inscriptions_cleaned.jsonl.gz"

    cleaned.to_json(output_jsonl, orient="records", lines=True, force_ascii=False)
    print(f"\nSaved cleaned JSONL: {output_jsonl}")

    cleaned.to_json(output_gz, orient="records", lines=True, force_ascii=False,
                    compression="gzip")
    raw_mb = output_jsonl.stat().st_size / 1048576
    gz_mb = output_gz.stat().st_size / 1048576
    print(f"Saved compressed   : {output_gz} "
          f"({gz_mb:.1f} MB, {raw_mb / gz_mb:.1f}x smaller)")

    return output_jsonl


def launch_streamlit_map() -> None:
    """Launch the Streamlit map application."""
    map_script = SRC_DIR / "edcs_streamlit_map.py"
    
    if not map_script.exists():
        raise FileNotFoundError(f"Streamlit map script not found: {map_script}")
    
    print(f"\n{'='*70}")
    print("STEP 3: LAUNCHING STREAMLIT MAP")
    print(f"{'='*70}")
    print("\nStarting Streamlit server...")
    print("The map will open in your browser shortly...")
    print("If it doesn't, navigate to: http://localhost:8501\n")

    streamlit_probe = subprocess.run(
        [sys.executable, "-c", "import streamlit"],
        capture_output=True,
        text=True,
        cwd=str(ROOT_DIR),
    )

    if streamlit_probe.returncode == 0:
        run_cmd = [sys.executable, "-m", "streamlit", "run", str(map_script)]
        print(f"Using interpreter: {sys.executable}")
    else:
        run_cmd = ["uv", "run", "streamlit", "run", str(map_script)]
        print("Current Python environment does not include streamlit.")
        print("Falling back to: uv run streamlit ...")

    try:
        subprocess.run(run_cmd, cwd=str(ROOT_DIR), check=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Unable to launch Streamlit. Ensure 'uv' is installed, or run with the project venv:\n"
            "  source .venv/bin/activate && python main.py"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Streamlit launch failed (exit code {exc.returncode}). Try:\n"
            "  uv sync\n"
            "  source .venv/bin/activate && python main.py"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="EDCS Analytics: Scrape → Clean → Launch Interactive Map"
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping and only clean the latest data/*.jsonl file.",
    )
    parser.add_argument(
        "--skip-map",
        action="store_true",
        help="Skip launching the Streamlit map after cleaning.",
    )
    return parser


def main(skip_scrape: bool = False, skip_map: bool = False) -> None:
    """Main orchestration function: scrape → clean → launch Streamlit map."""
    print("\n" + "="*70)
    print("EDCS ANALYTICS PIPELINE")
    print("="*70)
    
    # Step 1: Scrape
    if not skip_scrape:
        run_scraper()
    
    # Step 2: Clean
    print(f"\n{'='*70}")
    print("STEP 2: CLEANING DATA")
    print(f"{'='*70}\n")
    
    data = load_latest_data()
    cleaned = clean_data(data, verbose=True)
    save_cleaned_data(cleaned)
    
    terminal_preview = build_terminal_preview(cleaned, max_rows=10)
    print_table("Cleaned data preview (first 10 rows):", terminal_preview, index=False)
    
    # Step 3: Launch Streamlit Map
    if not skip_map:
        launch_streamlit_map()


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(skip_scrape=args.skip_scrape, skip_map=args.skip_map)

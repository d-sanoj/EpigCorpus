"""Command line for the pipeline (python -m app.main): arguments, terminal output, launchers."""

import argparse
import os
import shutil
import subprocess
import sys

import pandas as pd

from app.config.paths import ROOT_DIR, SCRAPER_MODULE, SCRAPER_SCRIPT, STREAMLIT_APP


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


def run_scraper() -> None:
    """Run the EDCS scraper script."""
    scraper_script = SCRAPER_SCRIPT
    
    if not scraper_script.exists():
        raise FileNotFoundError(f"Scraper script not found: {scraper_script}")
    
    print(f"\n{'='*70}")
    print("STEP 1: SCRAPING EDCS DATA")
    print(f"{'='*70}\n")
    
    result = subprocess.run(
        [sys.executable, "-m", SCRAPER_MODULE],
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


def launch_streamlit_map() -> None:
    """Launch the Streamlit map application."""
    map_script = STREAMLIT_APP
    
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

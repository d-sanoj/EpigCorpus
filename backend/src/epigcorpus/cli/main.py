"""Command line for the pipeline (python -m app.main): arguments, terminal output, launchers."""

import argparse
import os
import shutil
import subprocess
import sys

import pandas as pd

from app.config.paths import (
    ROOT_DIR,
    SCRAPER_MODULE,
    SCRAPER_SCRIPT,
    SERVER_MODULE,
    SERVER_SCRIPT,
)


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


def launch_web_app() -> None:
    """Serve the API and the web UI (python -m app.cli.serve) until interrupted."""
    server_script = SERVER_SCRIPT

    if not server_script.exists():
        raise FileNotFoundError(f"Server script not found: {server_script}")

    print(f"\n{'='*70}")
    print("STEP 3: LAUNCHING THE WEB APP")
    print(f"{'='*70}")
    print("\nStarting the EpigCorpus server...")
    print("Open http://127.0.0.1:8000/ in your browser once it is up.")
    print("The corpus loads in the background; the page waits for it.\n")

    try:
        subprocess.run([sys.executable, "-m", SERVER_MODULE], cwd=str(ROOT_DIR), check=True)
    except KeyboardInterrupt:
        print("\nServer stopped.")
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Server exited with code {exc.returncode}. Try:\n"
            "  uv sync\n"
            "  uv run python -m app.cli.serve"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="EDCS Analytics: Scrape → Clean → Launch the web app"
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping and only clean the latest data/*.jsonl file.",
    )
    parser.add_argument(
        "--skip-map",
        action="store_true",
        help="Skip launching the web app (API and map UI) after cleaning.",
    )
    return parser

"""EDCS Analytics main orchestration: scrape → clean → launch the web app.

Run from the repository root: ``python -m app.main [--skip-scrape] [--skip-map]``.
This is the composition root for the pipeline: it wires the file adapters into
the cleaning service and calls the CLI's launchers.
"""

from app.adapters import corpus_files
from app.cli.main import (
    build_parser,
    build_terminal_preview,
    launch_web_app,
    print_table,
    run_scraper,
)
from app.services.cleaning import run_cleaning


def main(skip_scrape: bool = False, skip_map: bool = False) -> None:
    """Main orchestration function: scrape → clean → launch the web app."""
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
    
    cleaned = run_cleaning(corpus_files)
    
    terminal_preview = build_terminal_preview(cleaned, max_rows=10)
    print_table("Cleaned data preview (first 10 rows):", terminal_preview, index=False)
    
    # Step 3: Launch the web app (API + map UI)
    if not skip_map:
        launch_web_app()


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(skip_scrape=args.skip_scrape, skip_map=args.skip_map)

"""Every filesystem path EpigCorpus reads or writes.

This is the only module that defines paths. The data layout, by role:

    app/data/raw/          what the scraper fetched, untransformed
    app/data/raw/harvest/  harvest state: checkpoint, failures, manifest, index cache
    app/data/processed/    deterministic transforms of raw (cleaned corpus, derived datasets)
    app/data/reference/    lookup tables and vendored third-party map layers
    app/data/results/      analysis outputs
    app/data/reports/      research reports

See docs/refactor/path-map.tsv for where each file used to live.
"""

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
APP_DIR = ROOT_DIR / "app"
DATA_DIR = APP_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
HARVEST_DIR = RAW_DIR / "harvest"
PROCESSED_DIR = DATA_DIR / "processed"
REFERENCE_DIR = DATA_DIR / "reference"
RESULTS_DIR = DATA_DIR / "results"
REPORTS_DIR = DATA_DIR / "reports"

# ─── Scraper outputs ──────────────────────────────────────────────────────────
RAW_JSONL = RAW_DIR / "edcs_inscriptions.jsonl"
RAW_TSV = RAW_DIR / "edcs_inscriptions.tsv"
CHECKPOINT_FILE = HARVEST_DIR / "edcs_checkpoint.json"
FAILED_IDS_FILE = HARVEST_DIR / "edcs_failed_ids.json"
INDEX_CACHE_FILE = HARVEST_DIR / "edcs_index_cache.json"
MANIFEST_FILE = HARVEST_DIR / "edcs_harvest_manifest.json"
LOOKUP_FILE = REFERENCE_DIR / "edcs_lookup.json"

# ─── Cleaning outputs ─────────────────────────────────────────────────────────
CLEANED_JSONL = PROCESSED_DIR / "edcs_inscriptions_cleaned.jsonl"
CLEANED_JSONL_GZ = PROCESSED_DIR / "edcs_inscriptions_cleaned.jsonl.gz"

# ─── Reference layers ─────────────────────────────────────────────────────────
# Vendored rather than fetched at runtime; provenance and licence terms are in
# app/data/reference/map_layers/README.md.
MAP_LAYERS_DIR = REFERENCE_DIR / "map_layers"

# ─── Research datasets and outputs (app/scripts) ──────────────────────────────
DERIVED_DIR = PROCESSED_DIR / "derived"
DERIVED_V1_DIR = DERIVED_DIR / "v1"
SPLITS_DIR = DERIVED_V1_DIR / "splits"
ANALYSIS_DIR = RESULTS_DIR / "analysis"
ANALYSIS_V1_DIR = ANALYSIS_DIR / "v1"
CELLS_DIR = RESULTS_DIR / "cells"
SCRIPTS_DIR = APP_DIR / "scripts"

# ─── API and web UI ───────────────────────────────────────────────────────────
API_SPEC = APP_DIR / "api" / "openapi.yaml"
UI_STATIC_DIR = APP_DIR / "ui" / "static"

# ─── Entry points launched as subprocesses by app/main.py ─────────────────────
SCRAPER_MODULE = "app.cli.scrape"
SCRAPER_SCRIPT = APP_DIR / "cli" / "scrape.py"
SERVER_MODULE = "app.cli.serve"
SERVER_SCRIPT = APP_DIR / "cli" / "serve.py"

"""Every filesystem path EpigCorpus reads or writes.

This is the only module that defines paths. The repository layout:

    backend/                 this package (backend/src/epigcorpus) and its tests
    frontend/                the web app; its production build is frontend/dist
    data/raw/                what the harvest fetched, untransformed
    data/raw/harvest/        harvest state: checkpoint, failures, manifest, index cache
    data/processed/          deterministic transforms of raw: cleaned corpus,
                             its Parquet search index, derived datasets
    data/reference/          lookup tables and vendored map layers
    research/                the abbreviation study: scripts, reports, results

Three environment variables relocate things for a container or a server:
EPIGCORPUS_DATA_DIR (default <repo>/data), EPIGCORPUS_RESEARCH_DIR
(default <repo>/research) and EPIGCORPUS_STATIC_DIR (default
<repo>/frontend/dist). See docs/refactor/path-map-backend.tsv for where each
file lived before.
"""

import os
from pathlib import Path

# backend/src/epigcorpus/config/paths.py -> the repository root
ROOT_DIR = Path(__file__).resolve().parents[4]
PACKAGE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("EPIGCORPUS_DATA_DIR", ROOT_DIR / "data"))
RESEARCH_DIR = Path(os.environ.get("EPIGCORPUS_RESEARCH_DIR", ROOT_DIR / "research"))

RAW_DIR = DATA_DIR / "raw"
HARVEST_DIR = RAW_DIR / "harvest"
PROCESSED_DIR = DATA_DIR / "processed"
REFERENCE_DIR = DATA_DIR / "reference"
RESULTS_DIR = RESEARCH_DIR / "results"
REPORTS_DIR = RESEARCH_DIR / "reports"

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
# A Parquet copy of the cleaned corpus, rebuilt from the JSONL whenever that
# changes. It loads in about a second where the JSONL takes most of a minute.
CLEANED_PARQUET = PROCESSED_DIR / "edcs_inscriptions_cleaned.parquet"

# ─── Reference layers ─────────────────────────────────────────────────────────
# Vendored rather than fetched at runtime; provenance and licence terms are in
# data/reference/map_layers/README.md.
MAP_LAYERS_DIR = REFERENCE_DIR / "map_layers"

# ─── Research datasets and outputs (research/scripts) ─────────────────────────
DERIVED_DIR = PROCESSED_DIR / "derived"
DERIVED_V1_DIR = DERIVED_DIR / "v1"
SPLITS_DIR = DERIVED_V1_DIR / "splits"
ANALYSIS_DIR = RESULTS_DIR / "analysis"
ANALYSIS_V1_DIR = ANALYSIS_DIR / "v1"
CELLS_DIR = RESULTS_DIR / "cells"
SCRIPTS_DIR = RESEARCH_DIR / "scripts"

# ─── API and web UI ───────────────────────────────────────────────────────────
API_SPEC = PACKAGE_DIR / "api" / "openapi.yaml"
UI_STATIC_DIR = Path(os.environ.get("EPIGCORPUS_STATIC_DIR", ROOT_DIR / "frontend" / "dist"))

# ADR 0001 — Layered `app/` layout

- **Status:** Accepted
- **Date:** 2026-10-23
- **Records:** the restructure planned in `docs/refactor/plan.md` and verified
  in `docs/refactor/verification.md`

## Context

EpigCorpus grew as three scripts and an orchestrator: `src/edcs_scraper.py`
(HTTP, parsing, checkpointing, file output and its own CLI in one file),
`src/edcs_cleaner.py`, `src/edcs_streamlit_map.py` (1,600 lines of search
logic, file and shapefile loading, Leaflet HTML and PNG rendering), and
`main.py`. Data lived in `data/`, `results/` and `reports/` at the root.
Each module computed its own paths from `__file__`.

That shape made two upcoming needs hard:

1. **An HTTP API** (ADR 0002) needs to call the same search, cleaning and
   scrape logic the UI uses. That logic was inside the Streamlit module,
   wrapped in `st.cache_data`.
2. **Moving or regrouping data** meant editing paths in every module and 26
   research scripts.

## Decision

All code, tests and data live under `app/`, in layers with a one-way
dependency rule:

| Layer | Holds | May import (internal) |
|---|---|---|
| `config/` | `paths.py` (every filesystem path), `settings.py` (EDCS endpoints, HTTP, bounds, tiles) | nothing |
| `models/` | the inscription column schema | config |
| `core/` | pure helpers: cleaning rules, EDCS payload parsing, search and aggregation | config, models |
| `adapters/` | EDCS HTTP client, harvest store, corpus files, map layers, console log | config, models, core |
| `services/` | use cases (scrape, cleaning, search, map data) and `ports.py` | config, models, core |
| `api/`, `ui/`, `cli/` | entry points | config, models, services |

Services never import adapters. They receive them, typed by the
`typing.Protocol`s in `app/services/ports.py`, from a **composition root**:
`app/main.py`, `app/cli/scrape.py` and `app/ui/streamlit_app.py`. Only these
three import adapters from outside the adapter layer.
`app/tests/test_layering.py` enforces the table with `ast`. It also confines
`streamlit` to `ui/`.

Data is grouped by role under `app/data/`: `raw/` (+ `raw/harvest/`),
`processed/`, `reference/`, `results/`, `reports/`.

Entry points run as modules from the repository root: `python -m app.main`,
`python -m app.cli.scrape`, and `streamlit run app/ui/streamlit_app.py` (that
file adds the root to `sys.path`). No build backend was added, so `uv.lock`
is unchanged.

Repository metadata stays at the root, as does `epigcorpus.sh`, the
documented one-command entry point, which needs to sit next to `.venv/` and
`pyproject.toml`. Research scripts live in `app/scripts/`. They are not part
of the package and are excluded from lint. Their path lines go through
`app/scripts/_paths.py` to `app.config.paths`.

## Consequences

- One place to change a path, and one place to read the data layout.
- The search, cleaning and scrape logic can be called without Streamlit,
  which is what the API contract in `app/api/openapi.yaml` maps onto.
- Streamlit caching is now explicit: the UI wraps each service in
  `st.cache_data` and passes its cached loaders back in, so nested loads
  still hit the cache (checked by counting reads across reruns).
- `python main.py` and `python src/edcs_scraper.py` no longer exist. The
  module commands take the same flags.
- `reports/`, `CHANGELOG.md` and the audit documents still cite pre-move
  paths. They are historical records and were not rewritten.
  `docs/refactor/path-map.tsv` translates them.
- Behaviour was held fixed and checked byte for byte: the full-corpus cleaning
  output, a fixture harvest (JSONL, TSV, gzip, checkpoint, manifest), a
  fixture cleaning run, the Streamlit queries and both PNG exports.

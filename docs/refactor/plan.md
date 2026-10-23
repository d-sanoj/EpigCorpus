# Phase 1 — Restructuring plan

Status: **awaiting approval.** Nothing below has been executed. Read with
`docs/refactor/discovery.md`, which holds the evidence this plan relies on.

Ground rules carried from the brief: no new features or sources; logic,
CLI flags, output file names and formats unchanged; data content unchanged;
no files deleted; no new dependencies; `git mv` for every move; small
Conventional Commits; no push.

Items marked **DECIDE** need your call. Each one has a recommendation, and
"approved" without comment means I take the recommendation.

---

## 1. Target tree

```text
EpigCorpus/
├── README.md  LICENSE  CHANGELOG.md          # repo metadata (unchanged location)
├── pyproject.toml  uv.lock  .python-version
├── .gitignore  .gitattributes
├── epigcorpus.sh                             # stays at root, see §1.1
├── docs/                                     # stays at root (you asked for docs/refactor, docs/adr)
│   ├── AUDIT.md  CLEANING_COMPARISON.md  EDCS_API.md  LATEPIG_BREAKAGE.md
│   ├── adr/0001-layered-app-layout.md        # new, Phase 2 (DECIDE D9)
│   ├── adr/0002-api-first-design.md          # new, Phase 3
│   ├── img/                                  # from img/ (DECIDE D6)
│   └── refactor/  discovery.md plan.md baseline.md checksums-*.txt path-map.tsv unused.md
└── app/
    ├── __init__.py
    ├── main.py                               # from main.py: composition root for the pipeline
    ├── config/
    │   ├── __init__.py
    │   ├── paths.py                          # every filesystem path, nowhere else
    │   └── settings.py                       # EDCS URLs, HTTP headers, workers, chunk, retries, bounds
    ├── models/
    │   ├── __init__.py
    │   └── inscription.py                    # TSV_FIELDS, LIST_COLUMNS, REQUIRED_COLS, column-name TypedDicts
    ├── core/
    │   ├── __init__.py
    │   ├── cleaning.py                       # rule sets, apply_rules, clean_conservative/interpretive
    │   ├── edcs_parsing.py                   # format_citation, image_url, format_dating, parse_monument, now_iso
    │   └── search.py                         # token sets, apply_filters, aggregate_places, describe_query, …
    ├── adapters/
    │   ├── __init__.py
    │   ├── edcs_client.py                    # make_session, get_json, build_lookup, fetch_index
    │   ├── harvest_store.py                  # checkpoint / manifest / failed-ids / lookup save+load, compress_outputs, CorpusWriter
│   ├── console.py                        # log(): the scraper's locked, flushed stdout writer
    │   ├── corpus_files.py                   # raw-corpus discovery+load, cleaned-corpus save+load, lookup read
    │   └── map_layers.py                     # map_layer_path, provinces/roads/cities loaders (geopandas)
    ├── services/
    │   ├── __init__.py
    │   ├── ports.py                          # typing.Protocol interfaces the services depend on
    │   ├── scrape.py                         # harvest(), run_harvest()
    │   ├── cleaning.py                       # clean_data(), run_cleaning()
    │   ├── search.py                         # load_inscriptions, filter_options, short_forms, run_query, known places, results table
    │   └── map_data.py                       # provinces_geojson, roads_geojson, cities_points
    ├── api/
    │   ├── openapi.yaml                      # Phase 3
    │   └── README.md                         # Phase 3
    ├── ui/
    │   ├── __init__.py
    │   └── streamlit_app.py                  # from src/edcs_streamlit_map.py: st.* code, Leaflet template, PNG
    ├── cli/
    │   ├── __init__.py
    │   ├── main.py                           # argparse for app/main.py, terminal preview, subprocess launchers
    │   └── scrape.py                         # argparse + signal handling for the scraper (python -m app.cli.scrape)
    ├── scripts/                              # from scripts/: research scripts, not part of the package (no __init__.py)
    ├── data/                                 # see §3
    └── tests/
        ├── conftest.py
        ├── fixtures/                         # from tests/fixtures/ (50 monuments + 4 index files)
        ├── adapters/test_edcs_upstream_contract.py   # from tests/test_api_contract.py (renamed, see §1.2)
        ├── core/test_cleaner_baseline.py     # from tests/test_cleaner_baseline.py
        └── test_layering.py                  # new: enforces the dependency rule with ast, no new deps
```

### 1.1 Why `epigcorpus.sh` stays at the root

It is the documented one-command entry point (`./epigcorpus.sh` in the
README), and it locates `.venv/`, `pyproject.toml` and `uv.lock` by `cd`-ing to
its own directory. All of those must stay at the root for uv. Moving it under
`app/` would change the documented command and need a `cd ..` to find the
environment. The only edit: `"$SCRIPT_DIR/main.py"` becomes `-m app.main`.

### 1.2 Test renames

`tests/test_api_contract.py` pins the **EDCS upstream** schema and touches no
project code (discovery §2). Once `app/api/` exists, "API contract" would read
as *our* API, so it moves to
`app/tests/adapters/test_edcs_upstream_contract.py`, with content unchanged.
The tests for our own HTTP API will go under `app/tests/api/` once endpoints
exist (not in this work).

### 1.3 Function-by-function placement

Logic moves verbatim. Only imports and the names of path/setting constants
change.

| Current location | Symbol(s) | New home |
|---|---|---|
| `src/edcs_cleaner.py` | `_SUPERSCRIPTS`, `_LETTER`, `_rule`, `CONSERVATIVE_RULES`, `INTERPRETIVE_RULES`, `apply_rules`, `clean_conservative`, `clean_interpretive` | `core/cleaning.py` |
| `src/edcs_cleaner.py` | `clean_data` (pandas + verbose `print`) | `services/cleaning.py` (tests import it from there) |
| `main.py` | `get_jsonl_files`, `load_latest_data`, `save_cleaned_data` | `adapters/corpus_files.py` |
| `main.py` | step 2 of `main()` (load → clean → save → preview) | `services/cleaning.run_cleaning()` returns the frame; preview printing stays in `cli/main.py` |
| `main.py` | `print_table`, `build_terminal_preview`, `build_parser`, `run_scraper`, `launch_streamlit_map` | `cli/main.py` |
| `main.py` | `main()` | `app/main.py` (calls `cli.main.build_parser`, services, launchers, in the same order with the same banners) |
| `src/edcs_scraper.py` | `SRC_DIR/PROJECT_DIR/DATA_DIR/OUTPUT_*/…` | `config/paths.py` |
| `src/edcs_scraper.py` | `BASE`, `*_URL`, `EPIGCORPUS_VERSION`, `HEADERS`, `DEFAULT_WORKERS`, `CHUNK_SIZE`, `MAX_RETRIES`, `BACKOFF_BASE` | `config/settings.py` |
| `src/edcs_scraper.py` | `TSV_FIELDS`, `LIST_COLUMNS` | `models/inscription.py` |
| `src/edcs_scraper.py` | `log`, `_print_lock` | `adapters/console.py`. Adapters call it directly; services receive it as a `Log` port (`Callable[[str], None]`) from the composition root |
| `src/edcs_scraper.py` | `install_signal_handlers`, `build_parser`, the argparse part of `main()` | `cli/scrape.py` |
| `src/edcs_scraper.py` | `make_session`, `get_json`, `build_lookup`, `fetch_index` | `adapters/edcs_client.py` |
| `src/edcs_scraper.py` | `now_iso`, `format_citation`, `image_url`, `format_dating`, `parse_monument` | `core/edcs_parsing.py` |
| `src/edcs_scraper.py` | `save_lookup`, `save/load_checkpoint`, `save/load_manifest`, `save/load_failed`, `compress_outputs` | `adapters/harvest_store.py` |
| `src/edcs_scraper.py` | the two `open()` calls, `csv.DictWriter`, header write, `write_payload` row serialisation inside `harvest` | `adapters/harvest_store.CorpusWriter` (same modes, same flush points) |
| `src/edcs_scraper.py` | `harvest`, the orchestration body of `main()` | `services/scrape.py` |
| `src/edcs_streamlit_map.py` | `ROOT_DIR/DATA_DIR/MAP_LAYERS_DIR/CLEANED_*/LOOKUP_FILE` | `config/paths.py` |
| `src/edcs_streamlit_map.py` | `ROMAN_BOUNDS` | `config/settings.py` |
| `src/edcs_streamlit_map.py` | `REQUIRED_COLS`, `MULTI_VALUE_FILTERS` | `models/inscription.py` |
| `src/edcs_streamlit_map.py` | `map_layer_path`, `load_provinces`, `load_roads`, `load_cities` (undecorated) | `adapters/map_layers.py` |
| `src/edcs_streamlit_map.py` | `cleaned_corpus_path` | `adapters/corpus_files.py` |
| `src/edcs_streamlit_map.py` | `_token_set`, `apply_filters`, `filter_inscriptions` (dead), `aggregate_places`, `_place_result_items`, `radius_for_count`, `_normalize_search_mode`, `FILTER_LABELS`, `SEARCH_COLUMNS`, `FILTER_PHRASING`, `PHRASE_LIST_LIMIT`, `SUBTITLE_WIDTH`, `SEARCH_MODE_PHRASING[_SHORT]`, `_join_values`, `_clause`, `describe_query`, `POPUP_ITEM_LIMIT`, `RADIUS_*`, `EMPTY_RADIUS` | `core/search.py` |
| `src/edcs_streamlit_map.py` | `load_all_inscriptions`, `load_full_cleaned_data`, `short_forms`, `filter_options`, `load_known_places`, `run_query`, the matched-table + TSV bytes block in `main()` | `services/search.py` (undecorated) |
| `src/edcs_streamlit_map.py` | `_province_palette`, `provinces_geojson`, `roads_geojson`, `cities_points` | `services/map_data.py` |
| `src/edcs_streamlit_map.py` | styles, colours, `MAP_TEMPLATE`, `_json_for_script`, `build_map_html`, `_print_province_palette`, `PRINT_*`, `build_png_bytes`, `option_formatter`, `_submit_search`, `main()` | `ui/streamlit_app.py` |

**Streamlit caching.** Every `@st.cache_data` stays in the UI. `ui/streamlit_app.py`
defines thin cached wrappers (`load_all_inscriptions = st.cache_data(...)(search.load_inscriptions)`)
with the same `show_spinner=False, ttl=3600`. Two behaviours depend on
**nested** caching: `filter_options` and `load_known_places` call
`load_all_inscriptions`, and `run_query` calls it too. To keep those inner
calls cached, the services take the loader as a parameter (a `CorpusSource`
port), and the UI passes its cached wrapper. So the call graph and cache hits
stay what they are today, and services never import streamlit.

`log()` in the scraper uses a lock and `print(flush=True)`. It moves verbatim.
Output stays byte-identical on stdout.

### 1.4 Dependency rule and how it is enforced

```
            ┌───────── api ─────────┐  ┌── ui ──┐  ┌── cli ──┐   (app/main.py = composition root)
            └──────────┬────────────┘  └───┬────┘  └────┬────┘
                       └──────────────► services ◄──────┘
                                   │         │ (via ports.Protocol)
                          models ◄─┤  core ◄─┤       adapters
                                             └─────────►  (implement ports; may use core, models)
config ◄── imported by every layer
```

- `api`, `ui`, `cli` import `services` (plus `config`, and `models` for types).
  They never import `adapters` directly. The single exception is
  `app/main.py` and each entry point's wiring function, which construct
  adapters and hand them to services.
- `services` import `models`, `core`, `config` and `services.ports`. They
  receive adapters as arguments typed by the Protocols in `ports.py`. Default
  arguments that construct the real adapter are **not** used, so the rule
  holds literally.
- `adapters` import `config`, `models`, `core` and implement the ports
  structurally. They never import `services`.
- `models` and `core` import nothing from `app` except `config`.
- `test_layering.py` parses every `app/**/*.py` (except `app/scripts`) with
  `ast` and fails on any import that breaks this table. It adds no dependency.

### 1.5 Making `app` importable

Today `streamlit run` puts the script's own directory on `sys.path`, not the
repo root, so `app/ui/streamlit_app.py` cannot `import app.…` by default.

**DECIDE D1 — how `app` becomes importable.**
- **(A, recommended)** No packaging change. Every entry runs from the repo
  root as a module (`python -m app.main`, `python -m app.cli.scrape`), which
  puts the root on `sys.path`. `ui/streamlit_app.py` starts with the same
  three-line `sys.path` bootstrap pattern that `tests/test_cleaner_baseline.py`
  already uses, so `streamlit run app/ui/streamlit_app.py` and the `uv run
  streamlit run …` fallback in `main.py` both work. pytest gets
  `pythonpath = ["."]` in `[tool.pytest.ini_options]`, a built-in option.
  No new dependency, and `uv.lock` is untouched.
- (B) Add a `[build-system]` so `uv sync` installs `app` editable. This is
  cleaner long-term, but it adds a build backend and changes `uv.lock`. That
  runs into "no new dependencies", so I would not do it without your yes.

**DECIDE D2 — root `main.py`.** The brief puts everything under `app/`, so
`main.py` moves to `app/main.py`, and the command becomes
`python -m app.main [--skip-scrape] [--skip-map]`. The flags are identical,
`./epigcorpus.sh` keeps working unchanged, and README "Run modes" is updated.
**Recommended: no root shim.** If you want `python main.py` to keep working,
I would add a 3-line root `main.py` that calls `app.main.main`. That is one
extra root file outside the metadata list.

Same question for the scraper: `python src/edcs_scraper.py --limit 500`
becomes `python -m app.cli.scrape --limit 500`, with identical flags. That is
the only invocation change.

---

## 2. Unchanged behaviours to preserve (checklist for Phase 2)

- `main.py` banners, step order, and the `[sys.executable, <scraper>]` subprocess
  with `cwd=ROOT`, `MPLBACKEND=Agg`, and captured stdout echoed after exit. New
  command list: `[sys.executable, "-m", "app.cli.scrape"]`.
- The input picker: newest mtime among `*.jsonl` and `*.jsonl.gz` in the raw
  directory, names containing `_cleaned` excluded (kept even though the
  processed file now lives elsewhere), plain before `.gz` at equal mtime.
- The `belege` → `evidence` rename on load.
- Cleaned output: `edcs_inscriptions_cleaned.jsonl` and `.jsonl.gz`, written
  with the same `to_json` arguments.
- Scraper: same file names, open modes, TSV header, list-column pipe join,
  flush/checkpoint cadence, refuse-to-overwrite guard, `--restart` deletions,
  manifest logic, and same stdout text. The one exception: the logged paths
  print the new absolute paths, because the files moved.
- Streamlit: same widgets, keys, labels, cache TTLs, download names
  (`edcs_map_{stem}.png`, `edcs_search_results.tsv`), and PNG bytes.
- `os.makedirs(DATA_DIR, exist_ok=True)` in the scraper becomes `makedirs` of
  each output's parent (`raw/`, `raw/harvest/`, `reference/`). This is a
  necessary, behaviour-neutral consequence of splitting one folder into
  several. It is called out here so it is not a surprise.

---

## 3. Data layout under `app/data/`

```text
app/data/
├── README.md
├── raw/                       # what the scraper fetched, untransformed
│   ├── edcs_inscriptions.jsonl.gz
│   ├── edcs_inscriptions.tsv.gz
│   └── harvest/               # harvest state and provenance
│       ├── edcs_harvest_manifest.json
│       ├── edcs_failed_ids.json
│       └── (runtime, gitignored) edcs_checkpoint.json, edcs_index_cache.json
├── processed/                 # deterministic transforms of raw
│   ├── edcs_inscriptions_cleaned.jsonl.gz
│   └── derived/               # abbreviation-study datasets (name kept, see D4)
│       ├── abbrev_pairs.tsv.gz
│       └── v1/
│           ├── abbrev_pairs_v1.tsv.gz
│           ├── phase3_stats.json
│           └── splits/  (12 × .tsv.gz, SHA256SUMS.txt, split_manifest.json, split_stats.json)
├── reference/                 # lookups and third-party layers the app reads but never writes from analysis
│   ├── edcs_lookup.json
│   └── map_layers/  (README.md, Hanson2016_Cities_OxREP.csv, ba_roads/, roman_empire_ad_117/)
├── results/                   # analysis outputs (numbers, not datasets)
│   ├── analysis/  phase1_supplement*.json, phase2_*.json
│   │   └── v1/    phase3_diff.json, phase3f_blockperm.json, phase3f_signal.json
│   ├── cells/
│   ├── cells_m2_alpha1e-5/
│   └── cells_m2_alpha1e-7/
└── reports/                   # Markdown research reports, moved as a tree
    ├── *.md (10)
    ├── baseline_v0/
    └── env/
```

`exports/` is **not** created. See D3.

### 3.1 Old → new, every file

The same table is committed in Phase 2 as `docs/refactor/path-map.tsv`, which
drives the checksum comparison.

| Old path | New path | Note |
|---|---|---|
| `data/README.md` | `app/data/README.md` | path text updated in a *later* commit (D7) |
| `data/edcs_inscriptions.jsonl.gz` | `app/data/raw/edcs_inscriptions.jsonl.gz` | |
| `data/edcs_inscriptions.tsv.gz` | `app/data/raw/edcs_inscriptions.tsv.gz` | **DECIDE D3** |
| `data/edcs_harvest_manifest.json` | `app/data/raw/harvest/edcs_harvest_manifest.json` | |
| `data/edcs_failed_ids.json` | `app/data/raw/harvest/edcs_failed_ids.json` | |
| `data/edcs_lookup.json` | `app/data/reference/edcs_lookup.json` | written by the scraper, but its role is lookup |
| `data/edcs_inscriptions_cleaned.jsonl.gz` | `app/data/processed/edcs_inscriptions_cleaned.jsonl.gz` | |
| `data/derived/abbrev_pairs.tsv.gz` | `app/data/processed/derived/abbrev_pairs.tsv.gz` | |
| `data/derived/v1/abbrev_pairs_v1.tsv.gz` | `app/data/processed/derived/v1/abbrev_pairs_v1.tsv.gz` | |
| `data/derived/v1/phase3_stats.json` | `app/data/processed/derived/v1/phase3_stats.json` | **DECIDE D5**: build stats written next to the dataset by the script that builds it |
| `data/derived/v1/splits/<12 .tsv.gz>` | `app/data/processed/derived/v1/splits/<same>` | |
| `data/derived/v1/splits/SHA256SUMS.txt` | `app/data/processed/derived/v1/splits/SHA256SUMS.txt` | |
| `data/derived/v1/splits/split_manifest.json` | `app/data/processed/derived/v1/splits/split_manifest.json` | |
| `data/derived/v1/splits/split_stats.json` | `app/data/processed/derived/v1/splits/split_stats.json` | **DECIDE D5** |
| `data/derived/phase1_supplement.json` | `app/data/results/analysis/phase1_supplement.json` | |
| `data/derived/phase1_supplement2.json` | `app/data/results/analysis/phase1_supplement2.json` | |
| `data/derived/phase1_supplement3.json` | `app/data/results/analysis/phase1_supplement3.json` | |
| `data/derived/phase2_crossref.json` | `app/data/results/analysis/phase2_crossref.json` | |
| `data/derived/phase2_numeral_fusion.json` | `app/data/results/analysis/phase2_numeral_fusion.json` | |
| `data/derived/phase2_vinculum.json` | `app/data/results/analysis/phase2_vinculum.json` | |
| `data/derived/v1/phase3_diff.json` | `app/data/results/analysis/v1/phase3_diff.json` | |
| `data/derived/v1/phase3f_blockperm.json` | `app/data/results/analysis/v1/phase3f_blockperm.json` | |
| `data/derived/v1/phase3f_signal.json` | `app/data/results/analysis/v1/phase3f_signal.json` | |
| `data/map_layers/**` (14 files) | `app/data/reference/map_layers/**` | tree moved as is |
| `results/cells/*.json` (18) | `app/data/results/cells/*.json` | |
| `results/cells_m2_alpha1e-5/*.json` (9) | `app/data/results/cells_m2_alpha1e-5/*.json` | |
| `results/cells_m2_alpha1e-7/*.json` (9) | `app/data/results/cells_m2_alpha1e-7/*.json` | |
| `reports/**` (14 files) | `app/data/reports/**` | tree moved as is, `baseline_v0/` duplicates kept |
| `img/App video.gif` | `docs/img/App video.gif` | **DECIDE D6** |
| `img/Example export png image for term viator.png` | `docs/img/Example export png image for term viator.png` | **DECIDE D6** |
| `tests/fixtures/**` (54 files) | `app/tests/fixtures/**` | test data, not app data |

Runtime files that are not tracked, with new locations: plain
`edcs_inscriptions.{jsonl,tsv}` → `raw/`; `edcs_checkpoint.json`,
`edcs_index_cache.json` → `raw/harvest/`; `edcs_inscriptions_cleaned.jsonl` →
`processed/`; plain `abbrev_pairs*.tsv` and `splits/*.tsv` → next to their `.gz`.

### 3.2 Decisions on data

- **DECIDE D3 — `exports/`.** The scraper writes the JSONL and the TSV
  together, from the same rows, in the same loop, and compresses both at the
  end. They are the raw scrape in two formats, not a later export.
  **Recommended:** both in `raw/`, no `exports/` folder. The API's export
  endpoints (Phase 3) stream from `raw/` and `processed/`. Alternative: put
  the TSV, or both files, in `exports/`, which is a one-line config change
  either way.
- **DECIDE D4 — `processed/derived/` name.** It is redundant, but it keeps the
  `derived/v1/splits` segment that `app/data/reports/decisions.md` and
  `phase4_splits.md` cite about 40 times, so a reader can still find the files
  from the historical reports. Alternative: `processed/abbrev/`.
- **DECIDE D5 — `phase3_stats.json`, `split_manifest.json`, `split_stats.json`.**
  Recommended: keep them with the dataset they describe, under `processed/`.
  The other phase JSONs are free-standing analysis results, so they go to
  `results/analysis/`. Alternative: every `*.json` that is not a dataset goes
  to `results/`.
- **DECIDE D6 — `img/`.** These are README illustrations, not data or UI
  assets. Recommended: `docs/img/`, with README image links updated.
  Alternative: `app/ui/assets/`.
- **DECIDE D7 — `app/data/README.md` path text.** Moving it is a data move
  (checksummed). Afterwards, in a separate commit, I would update the layout
  and paths it describes, since it is user documentation. The historical
  `reports/*.md` and `CHANGELOG.md` entries are **not** edited. They are
  records of what was true when written, and `path-map.tsv` translates them.

---

## 4. Path plan (all paths live in `app/config/paths.py`)

`ROOT = Path(__file__).resolve().parents[2]` (repo root),
`APP = ROOT / "app"`, `DATA = APP / "data"`.

| Constant (new) | Old name & value | New value |
|---|---|---|
| `ROOT_DIR` | `main.ROOT_DIR`, `streamlit.ROOT_DIR`, `scraper.PROJECT_DIR` = `.` | `.` (unchanged) |
| `DATA_DIR` | `main.DATA_DIR`, `scraper.DATA_DIR`, `streamlit.DATA_DIR` = `data/` | `app/data/` |
| `RAW_DIR` | — (was `DATA_DIR` for `main.get_jsonl_files`) | `app/data/raw/` |
| `HARVEST_DIR` | — | `app/data/raw/harvest/` |
| `PROCESSED_DIR` | — | `app/data/processed/` |
| `REFERENCE_DIR` | — | `app/data/reference/` |
| `RESULTS_DIR` | — (`REPO/"results"` in scripts) | `app/data/results/` |
| `REPORTS_DIR` | — (`REPO/"reports"` in scripts) | `app/data/reports/` |
| `RAW_JSONL` | `scraper.OUTPUT_JSONL` = `data/edcs_inscriptions.jsonl` | `app/data/raw/edcs_inscriptions.jsonl` |
| `RAW_TSV` | `scraper.OUTPUT_TSV` = `data/edcs_inscriptions.tsv` | `app/data/raw/edcs_inscriptions.tsv` |
| `CHECKPOINT_FILE` | `scraper.CHECKPOINT` = `data/edcs_checkpoint.json` | `app/data/raw/harvest/edcs_checkpoint.json` |
| `FAILED_IDS_FILE` | `scraper.FAILED_FILE` = `data/edcs_failed_ids.json` | `app/data/raw/harvest/edcs_failed_ids.json` |
| `INDEX_CACHE_FILE` | `scraper.INDEX_CACHE` = `data/edcs_index_cache.json` | `app/data/raw/harvest/edcs_index_cache.json` |
| `MANIFEST_FILE` | `scraper.MANIFEST` = `data/edcs_harvest_manifest.json` | `app/data/raw/harvest/edcs_harvest_manifest.json` |
| `LOOKUP_FILE` | `scraper.LOOKUP_FILE`, `streamlit.LOOKUP_FILE` = `data/edcs_lookup.json` | `app/data/reference/edcs_lookup.json` |
| `CLEANED_JSONL` | `main.output_jsonl`, `streamlit.CLEANED_JSONL_FILE` = `data/edcs_inscriptions_cleaned.jsonl` | `app/data/processed/edcs_inscriptions_cleaned.jsonl` |
| `CLEANED_JSONL_GZ` | `main.output_gz`, `streamlit.CLEANED_JSONL_GZ` = `…cleaned.jsonl.gz` | `app/data/processed/edcs_inscriptions_cleaned.jsonl.gz` |
| `MAP_LAYERS_DIR` | `streamlit.MAP_LAYERS_DIR` = `data/map_layers/` | `app/data/reference/map_layers/` |
| `DERIVED_DIR` | `REPO/"data"/"derived"` (scripts) | `app/data/processed/derived/` |
| `DERIVED_V1_DIR` | `REPO/"data"/"derived"/"v1"` | `app/data/processed/derived/v1/` |
| `SPLITS_DIR` | `REPO/"data"/"derived"/"v1"/"splits"` | `app/data/processed/derived/v1/splits/` |
| `ANALYSIS_DIR` | `REPO/"data"/"derived"` for `phase*.json` | `app/data/results/analysis/` |
| `ANALYSIS_V1_DIR` | `REPO/"data"/"derived"/"v1"` for `phase3_diff/3f_*.json` | `app/data/results/analysis/v1/` |
| `CELLS_DIR` | `phase5_common.CELLS` = `results/cells/` | `app/data/results/cells/` |
| `SCRIPTS_DIR` | `REPO/"scripts"` | `app/scripts/` |
| `SCRAPER_MODULE` | `main.SRC_DIR/"edcs_scraper.py"` (run by path) | `"app.cli.scrape"` (run with `-m`) |
| `STREAMLIT_APP` | `main.SRC_DIR/"edcs_streamlit_map.py"` | `app/ui/streamlit_app.py` |
| `FIXTURE_DIR` (tests only, stays in `conftest.py` as `Path(__file__).parent / "fixtures"`) | `tests/fixtures/` | `app/tests/fixtures/` |

The error text `"Restore them with: git checkout -- data/map_layers"` becomes
`… -- app/data/reference/map_layers`.

`config/settings.py` holds the non-path settings unchanged:
`EDCS_BASE`, `INDEX_URL`, `LOOKUPS_URL`, `PLACES_URL`, `LANGUAGES_URL`,
`SOURCES_URL`, `MONUMENT_URL`, `IMAGE_URL`, `EPIGCORPUS_VERSION`, `HEADERS`,
`DEFAULT_WORKERS=16`, `CHUNK_SIZE=2000`, `MAX_RETRIES=4`, `BACKOFF_BASE=2.0`,
`ROMAN_BOUNDS`, and the CARTO/Leaflet URLs. Colours, styles and template
strings are presentation, so they stay in `ui/`.

### 4.1 The research scripts (the conflict in the brief)

The brief says to move `scripts/` "unchanged in content" and also says
`config/` is "the only place paths are defined". These conflict. Every script
builds paths as `REPO/"data"/"derived"/…`, `REPO/"reports"`, `REPO/"results"`
with `REPO = parent.parent` (discovery §3.4). After the move, `REPO` would be
`app/`, and with the role-grouped layout none of those paths exist.

**DECIDE D8 — scripts.**
- **(A, recommended) Path-only edits.** Add `app/scripts/_paths.py`, a sibling
  helper of about 10 lines that puts the repo root on `sys.path` and re-exports
  `app.config.paths`. In each of the 26 portable scripts, replace only the
  lines that build a filesystem path, for example
  `RAW = REPO / "data" / "edcs_inscriptions.jsonl"` →
  `RAW = paths.RAW_JSONL`, and `REPO/"data"/"derived"/"phase2_crossref.json"`
  → `paths.ANALYSIS_DIR/"phase2_crossref.json"`. `REPO` itself stays,
  repointed to the repo root, because three scripts print
  `X.relative_to(REPO)` into reports. Every other line stays byte-identical.
  Each script's diff will be reviewable as path-only, and I will list the
  changed line numbers per script in the commit body.
  `importlib` sibling loading (`Path(__file__).parent / "abbrev_probe.py"`,
  `REPO/"scripts"/…`) keeps working, and `REPO/"scripts"` becomes
  `paths.SCRIPTS_DIR`.
- (B) Byte-identical scripts that no longer resolve their inputs. They would
  be documented as historical and "not runnable without editing".
- (C) Byte-identical scripts, giving up the role grouping and keeping
  `app/data/derived/`, `app/reports/`, `app/results/` exactly where `REPO`
  expects them.

The three phase-0 scripts with `/Users/sanoj/...` and `/private/tmp/...`
paths already fail on any machine other than the author's. I propose to move
them **byte-identical** under every option and list them in `unused.md`.

**Lint.** `ruff check .` is red today with 532 findings, all in `scripts/`
(discovery §5.12). To meet "ruff passes" without touching script logic, I add
`app/scripts` to ruff's `exclude`, with a comment pointing at the baseline
count. If you would rather keep linting them, the gate becomes "no **new**
findings", and I record 532 as the baseline.

### 4.2 Other configuration that moves with paths

`pyproject.toml`:
- `testpaths = ["app/tests"]`, plus `pythonpath = ["."]` (D1-A).
- `[tool.ruff] exclude = ["app/data", "app/scripts", "docs/img"]`.
- `per-file-ignores` re-keyed to the new module files that received the
  previously-ignored lines (for example `"app/adapters/harvest_store.py"`,
  `"app/services/scrape.py"`, `"app/ui/streamlit_app.py"`). The same codes
  apply, and no new codes are added. If a split module gets no flagged line,
  it gets no entry. The original comment block stays.

`.gitignore`, rewritten rule for rule:

| Old | New |
|---|---|
| `data/edcs_checkpoint.json` | `app/data/raw/harvest/edcs_checkpoint.json` |
| `data/*.jsonl` | `app/data/raw/*.jsonl`, `app/data/processed/*.jsonl` |
| `data/*.tsv` | `app/data/raw/*.tsv`, `app/data/processed/*.tsv` |
| `data/edcs_inscriptions_cleaned.csv` | `app/data/processed/edcs_inscriptions_cleaned.csv` |
| `!data/*.jsonl.gz`, `!data/*.tsv.gz` | same negations under `app/data/raw/` and `app/data/processed/` |
| `data/edcs_index_cache.json` | `app/data/raw/harvest/edcs_index_cache.json` |
| `data/derived/**/*.tsv`, `data/derived/*.tsv` + negations | `app/data/processed/derived/**/*.tsv`, `app/data/processed/derived/*.tsv` + negations |

`epigcorpus.sh`: the two `"$SCRIPT_DIR/main.py" "$@"` invocations become
`-m app.main "$@"`, run with the working directory already at
`$SCRIPT_DIR`. Nothing else changes.

---

## 5. Checksum plan

1. **Before any move** (Phase 2 step 2): `sha256sum` of every tracked file
   under `data/`, `results/`, `reports/`, `img/` and `tests/fixtures/` (154
   files), sorted by path, written to `docs/refactor/checksums-before.txt` in
   `sha256  path` format.
2. Commit `docs/refactor/path-map.tsv` (old path → new path, one row per file
   in §3.1).
3. **Right after the move commit**, before any edit to a moved file: the same
   `sha256sum` over the new paths → `docs/refactor/checksums-after.txt`.
4. Compare: for each row of `path-map.tsv`, the hash recorded for the old
   path in *before* must equal the hash for the new path in *after*. Also check
   that the file counts match (154 = 154) and that no hash is missing on
   either side. I report the result in `baseline.md` and the commit body. Any
   mismatch stops the work.
5. Again at the end of Phase 2. Only `app/data/README.md` may differ by then
   (D7), and that diff is reported explicitly.

Git LFS: none to check (discovery §4.1). `git lfs ls-files` will be run
again after the move to confirm it is still empty.

## 6. Git plan

- Every file moves with `git mv`. Moves go in **move-only commits** (no content
  edits in the same commit), so `git log --follow` and rename detection see
  100 % similarity.
- When a code file is split, it is first `git mv`'d to the module that keeps
  the **largest** share of its lines (`src/edcs_streamlit_map.py` →
  `app/ui/streamlit_app.py`, `src/edcs_scraper.py` → `app/services/scrape.py`,
  `src/edcs_cleaner.py` → `app/core/cleaning.py`, `main.py` → `app/main.py`).
  Later commits cut code out of it into the other modules, so history follows
  the main body.
- `.gitattributes`: `*.gz binary` is path-independent and stays. The two
  path-bound rules become:

  ```
  app/data/raw/*.jsonl        text eol=lf
  app/data/raw/*.tsv          text eol=lf
  app/data/processed/*.jsonl  text eol=lf
  app/data/processed/*.tsv    text eol=lf
  ```

  They match no tracked file today and none after the move. They only affect
  a plain file force-added later. `git check-attr -a` on representative paths
  is run before and after to show equivalent results.
- Commit sequence (each one green on `pytest`, except the move-only ones,
  which are immediately followed by their path fix):
  1. `docs(refactor): record the baseline and data checksums`: baseline.md, checksums-before.txt, path-map.tsv
  2. `refactor(data): move data into app/data grouped by role`: git mv only
  3. `refactor(data): point code, ignores and attributes at app/data`: paths only, plus checksums-after.txt
  4. `refactor: move source files under app/`: git mv only (src, main.py, tests, scripts, img)
  5. `refactor(config): define every path and setting in app/config`
  6. `refactor(core): extract pure cleaning, parsing and search helpers`
  7. `refactor(models): name the inscription column schemas`
  8. `refactor(adapters): extract EDCS client, harvest store, corpus files and map layers`
  9. `refactor(services): scrape, cleaning, search and map-data use cases behind ports`
  10. `refactor(cli): split argument parsing from the pipeline entry point`
  11. `refactor(ui): make the Streamlit app a client of the services`
  12. `test: move tests under app/tests and enforce the layering rule`
  13. `build: update pytest, ruff and the launcher for the app layout`
  14. `refactor(scripts): resolve research-script paths through app.config` (D8-A only)
  15. `docs: update README and data README for the app layout; add ADR 0001`
  16. `docs(refactor): record the verification results and unused files`
- No push. No PR.

---

## 7. Verification (Phase 2 step 5), with the baseline captured first

| Check | How | Pass condition |
|---|---|---|
| Lint | `uv run ruff check .` | 0 findings (scripts excluded, §4.1) |
| Tests | `uv run pytest` | same pass / xfail / xpass counts as the baseline, same test IDs (modulo path prefix) |
| Layering | `test_layering.py` | green |
| CLI help | `python main.py --help` before, `python -m app.main --help` after; scraper `--help` likewise | identical text (argparse `prog` aside, which I will report) |
| CLI run | `python -m app.main --skip-scrape --skip-map` in a **throwaway `git worktree`** under the scratchpad, never in this checkout (discovery §5.14) | stdout identical except the printed paths; see next row |
| Full-corpus cleaning, byte for byte | same worktree run, before and after | `sha256` of the plain `edcs_inscriptions_cleaned.jsonl` identical, and `sha256` of the **decompressed** `.gz` identical (the gzip header mtime differs by design) |
| Fixture cleaning run, byte for byte | a scratch harness: `clean_data` on a frame built from all 50 fixture monuments' raw text plus 2,000 rows from the committed corpus, serialised with the same `to_json` call | identical sha256 before/after |
| Scraper, byte for byte | a scratch harness that runs `harvest()` against a fake session serving the 50 fixture monuments, with `now_iso` frozen, into a temp dir | identical JSONL, TSV, checkpoint and failed-ids bytes before/after |
| Streamlit starts | `streamlit.testing.v1.AppTest.from_file(...).run(timeout=…)` (in streamlit already) plus `streamlit run --server.headless true` and `GET /_stcore/health` → `ok` | no exception, health ok, same set of widgets |
| PNG unchanged | `build_png_bytes` for the no-query view and one `viator` query, before/after | identical bytes, or reported with a pixel diff if matplotlib embeds metadata |
| Data | checksum comparison, §5 | 154/154 match |

The harnesses live in the scratchpad, not the repo, because they are
verification scaffolding, not features. If you want the scraper harness kept
as a permanent characterisation test, say so. It would be the first test of
`parse_monument`.

---

## 8. API design outline (detailed in Phase 3)

Contract first: `app/api/openapi.yaml` (OpenAPI 3.1), no implementation, no
new dependency. All paths are under `/v1`. Errors use
`application/problem+json` (RFC 9457) with `type`, `title`, `status`,
`detail`, `instance`, and an `errors[]` extension for field validation.
Pagination is cursor-based: `?limit=` (default 50, max 500) and `?cursor=`
(opaque), with the response carrying `next_cursor` and a `Link: rel="next"`
header.

| Resource | Operations | Service it maps to |
|---|---|---|
| Inscriptions | `GET /v1/inscriptions` (list, filter-only); `GET /v1/inscriptions/search` (`q` required, `search_in`); `GET /v1/inscriptions/{record_id}` | `services.search.run_query` / `apply_filters`; get-one via `load_full_cleaned_data` |
| Cleaned variants | `GET /v1/inscriptions/{record_id}/variants`; `GET …/variants/{variant}` with `variant ∈ {raw, conservative, interpretive}` | cleaned-corpus columns produced by `services.cleaning.clean_data` |
| Places & map | `GET /v1/places` (per-place counts, same filters as search); `GET /v1/places/known`; `GET /v1/map/layers/{provinces\|roads\|cities}` (GeoJSON) | `aggregate_places`, `load_known_places`, `services.map_data.*` |
| Exports | `GET /v1/exports/corpus.{jsonl\|tsv}`; `GET /v1/exports/cleaned.jsonl`; `GET /v1/exports/search.tsv?<same filters>` (as downloads, `Content-Disposition: attachment`) | `adapters.corpus_files` via a thin export service; search TSV = the UI's matched-table export |
| Lookups | `GET /v1/lookups`; `GET /v1/lookups/{materials\|categories\|languages\|provinces}`; `GET /v1/facets` (filter values present in the corpus) | `reference/edcs_lookup.json` reader, `short_forms`, `filter_options` |
| Scrape jobs (async) | `POST /v1/scrape-jobs` → `202 Accepted` + `Location`; `GET /v1/scrape-jobs/{id}`; `POST /v1/scrape-jobs/{id}/cancel` → `202`; `409` if a job is already running (one checkpoint, one harvest at a time) | `services.scrape.run_harvest` (body mirrors `--limit`, `--workers`, `--restart`, `--refresh-index`) |

Filters are documented to match the current search exactly: `q` is a
case-insensitive **substring** match (not regex, not tokenised) on the
column chosen by `search_in` (`raw` → `inscription_text`, `interpretive`,
`conservative`). `material`, `province` and `place` are exact-match and
repeatable (OR within a filter). `category` and `language` match on token
overlap (comma-split for `language`, list elements for `category`), so
`language=Latin` matches `"Greek, Latin"`. Different filters combine with
AND.

Auth: an `info`-level section and a `securitySchemes` entry stating that the
API will use OAuth 2.0 / OIDC. Which flows (authorization code + PKCE for
the UI, client credentials for automation, scope for starting scrape jobs)
stays open in a proposed ADR (D9). Operations are not marked as secured yet.

Deliverables: `app/api/openapi.yaml`, `app/api/README.md` (endpoint → service
table), `docs/adr/0002-api-first-design.md`. Python's standard library cannot
parse YAML, and Phase 3 may add no dependencies. I will validate the spec with
a tool run ephemerally outside the project (for example `uvx
openapi-spec-validator`, which does not touch `pyproject.toml` or `uv.lock`)
and say which one I used. If none is reachable, I will report the file as
unvalidated.

---

## 9. Risks

1. **Splitting `@st.cache_data` functions.** If an inner call loses its cache,
   the app reloads 588k rows on every rerun. That is a performance
   regression, not a behaviour change, but a real one. It is mitigated by
   injecting the cached loader (§1.3) and checked with AppTest timing.
2. **Streamlit import path.** Handled by D1-A. The `uv run streamlit` fallback
   path in `launch_streamlit_map` is tested explicitly.
3. **The scraper split is the riskiest code move.** It has no tests today.
   It is mitigated by the byte-for-byte harness run before and after.
4. **mtime-based input selection.** `git mv` and checkout reset mtimes. Only
   one raw candidate exists, so the selection is unchanged, but a local plain
   `edcs_inscriptions.jsonl` left in the *old* `data/` would no longer be seen.
   I will mention it in the README as a migration note. Nothing in this
   checkout is affected.
5. **Accidental data rewrites.** Running the CLI rewrites the tracked cleaned
   `.gz` (mtime in the header). All runs happen in a scratch worktree, and
   `git status` must be clean of `app/data` changes before every commit.
6. **Absolute paths printed to stdout change** because files moved. This is
   the only expected stdout difference, and it is listed.
7. **Prose drift.** About 120 path mentions in docs, reports and the
   CHANGELOG still point at old paths. Only README, `app/data/README.md` and
   `tests/README.md` (moved) are updated. The rest is historical, and
   `path-map.tsv` covers it.
8. **`uv sync` needs network access to PyPI.** If the proxy blocks it,
   baseline and verification stop, and I will report that rather than work
   around it.

## 10. Questions for you

- **D1**: importability. Recommended: A (no packaging change).
- **D2**: root `main.py` shim? Recommended: no.
- **D3**: `exports/` folder. Recommended: none, both scraper outputs in `raw/`.
- **D4**: `processed/derived/` vs `processed/abbrev/`. Recommended: keep `derived`.
- **D5**: dataset-describing JSONs stay with the dataset. Recommended: yes.
- **D6**: `img/` → `docs/img/`. Recommended: yes.
- **D7**: update `app/data/README.md` paths after the checksum commit. Recommended: yes.
- **D8**: research scripts. Recommended: A (path-only edits via `_paths.py`), plus ruff exclusion of `app/scripts`.
- **D9**: there is no `docs/adr/0001` yet, but you asked for `0002`.
  Recommended: I write `0001-layered-app-layout.md` in Phase 2 (records this
  refactor), and in Phase 3 open `0003-api-authentication.md` as *Proposed*
  for the OAuth flow decision.
- Should the scraper harness from §7 become a committed test? Recommended:
  not in this work. Ask again after Phase 2.

On "approved", Phase 2 starts at step 1: `uv sync`, ruff, pytest, CLI help, and
the scratch-worktree baseline runs recorded in `docs/refactor/baseline.md`.

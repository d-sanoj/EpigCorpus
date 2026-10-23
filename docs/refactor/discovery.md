# Phase 0 — Discovery

Read-only survey of the repository at commit `bcdc14e` (branch
`claude/pensive-archimedes-csyx0n`, identical to `main`), taken 2026-10-23.
Nothing was moved or edited to produce this document.

---

## 1. What is in the repository

204 tracked files. No `.github/`, no `docs/adr/`, no `app/`.

| Path | Kind | Notes |
|---|---|---|
| `README.md`, `LICENSE`, `CHANGELOG.md`, `pyproject.toml`, `uv.lock`, `.python-version`, `.gitignore`, `.gitattributes` | repo metadata | stay at root per the brief |
| `epigcorpus.sh` | launcher | `cd`s to its own dir, runs `main.py` via `.venv` or `uv` |
| `main.py` | orchestrator + CLI | scrape (subprocess) → clean (import) → Streamlit (subprocess) |
| `src/edcs_scraper.py` (837 lines) | EDCS client + parsing + checkpointing + export writing + its own CLI | run as a script, never imported |
| `src/edcs_cleaner.py` (264 lines) | pure regex rule sets + `clean_data` (pandas) | the only module imported by others |
| `src/edcs_streamlit_map.py` (1,600 lines) | Streamlit UI, search/filter logic, map-layer loading, Leaflet HTML template, PNG rendering | run via `streamlit run <file>` |
| `scripts/*.py` (29 files) | one-off research scripts (phases 0–5 of the abbreviation study) | not imported by `main.py` or `src/` |
| `tests/` | 2 test modules, `conftest.py`, fixtures (54 files) | offline |
| `docs/` | 4 Markdown documents (audit, cleaning comparison, EDCS API, LatEpig breakage) | |
| `data/`, `results/`, `reports/`, `img/` | data and outputs | inventory in §4 |

### Runtime entry points

| Entry point | How it runs | Arguments |
|---|---|---|
| `./epigcorpus.sh [args]` | `exec python main.py "$@"` | passes everything through |
| `python main.py` | argparse | `--skip-scrape`, `--skip-map` |
| `python src/edcs_scraper.py` | argparse, launched by `main.py` as `[sys.executable, src/edcs_scraper.py]` with `cwd=ROOT_DIR` and **no arguments** | `--limit N`, `--workers N` (default 16), `--restart`, `--refresh-index` |
| `streamlit run src/edcs_streamlit_map.py` | launched by `main.py` with `cwd=ROOT_DIR` | none |
| `python scripts/<name>.py` | ad hoc | various |

`main.py` imports `src.edcs_cleaner` as a namespace package (there is no
`src/__init__.py`). `tests/test_cleaner_baseline.py` instead inserts `src/` on
`sys.path` and imports `edcs_cleaner` as a top-level module. The scraper and
the map are never imported, only executed by path.

---

## 2. What `tests/test_api_contract.py` actually tests

**It tests the EDCS source API, not any API of ours.** Its own docstring says
"These tests do not exercise our code." It loads captured EDCS payloads from
`tests/fixtures/` (captured 2026-08-19, EDCS release `20260807-142626`) and
pins their schema so that an EDCS rebuild turns a test red instead of silently
corrupting a harvest:

| Class | What it pins |
|---|---|
| `TestMonumentPayload` (8 tests) | `/data/monument/{shard}/{id8}.json`: `{"v": 2, "d": {...}}`, keys ⊆ `{g,m,q,p,i,c}`, `i` is a list of dicts with string `t`, dating `d` is a 2-list, citations `q` are ≥3-lists, at least one multi-inscription monument in the sample |
| `TestPlacesIndex` (4) | `places.json` keys `["geo_id","ort","p","coord"]`; `coord` is **[lat, lon]** (Roma check); ranges plausible; coord is a full pair or `None` |
| `TestSearchableIndex` (4 + 20 parametrised) | `searchable.json` keys `["id","g","m","ci","li","d","h"]`; row width; `g` may be null; dating field is `list` or `int` |
| `TestLookups` (4) | `lookups.json` top-level shape, `en` labels on materials, provinces are strings, Britannia present |

None of it imports project code. After the refactor it is a contract test of
the **EDCS adapter's upstream**, so it belongs with the adapter tests
(proposed: `app/tests/adapters/test_edcs_upstream_contract.py`). The name
`test_api_contract.py` will collide in meaning with tests for our own HTTP
API, which is why the rename is proposed in the plan.

`tests/test_cleaner_baseline.py` is a characterisation test of
`edcs_cleaner`: rule counts (27/25), specific input→output pairs, and
`xfail(strict=True)` tests that document known bugs (T05, T06, T08, T09 and
others). Strict xfails mean a behaviour change in the cleaner turns the suite
red, which makes it a good guard for the refactor.

Neither test module exercises `edcs_scraper`, `main.py` or the Streamlit app.
`parse_monument` has no tests, even though the monument fixtures would support
them.

---

## 3. Every path the code reads or writes

### 3.1 Pipeline code (`main.py`, `src/`)

| Constant / literal | Value (relative to repo root) | File | R/W |
|---|---|---|---|
| `ROOT_DIR` | `.` (from `Path(__file__).resolve().parent`) | `main.py:15` | — |
| `SRC_DIR` | `src/` | `main.py:16` | used to locate the two scripts below |
| `DATA_DIR` | `data/` | `main.py:17` | — |
| `SRC_DIR / "edcs_scraper.py"` | `src/edcs_scraper.py` | `main.py:91` | executed |
| `SRC_DIR / "edcs_streamlit_map.py"` | `src/edcs_streamlit_map.py` | `main.py:157` | executed |
| `DATA_DIR.glob("*.jsonl")` + `glob("*.jsonl.gz")`, minus names containing `_cleaned` | `data/*.jsonl[.gz]` (newest mtime wins) | `main.py:81` | R |
| `output_jsonl` | `data/edcs_inscriptions_cleaned.jsonl` | `main.py:136` | W |
| `output_gz` | `data/edcs_inscriptions_cleaned.jsonl.gz` | `main.py:137` | W |
| `SRC_DIR`, `PROJECT_DIR`, `DATA_DIR` | `src/`, `.`, `data/` | `src/edcs_scraper.py:74-76` | — |
| `OUTPUT_JSONL` | `data/edcs_inscriptions.jsonl` (+ `.gz` from `compress_outputs`) | `src/edcs_scraper.py:78` | W |
| `OUTPUT_TSV` | `data/edcs_inscriptions.tsv` (+ `.gz`) | `src/edcs_scraper.py:79` | W |
| `LOOKUP_FILE` | `data/edcs_lookup.json` | `src/edcs_scraper.py:80` | W |
| `CHECKPOINT` | `data/edcs_checkpoint.json` | `src/edcs_scraper.py:81` | R/W/delete |
| `FAILED_FILE` | `data/edcs_failed_ids.json` | `src/edcs_scraper.py:82` | R/W |
| `INDEX_CACHE` | `data/edcs_index_cache.json` | `src/edcs_scraper.py:83` | R/W |
| `MANIFEST` | `data/edcs_harvest_manifest.json` | `src/edcs_scraper.py:84` | R/W |
| `ROOT_DIR`, `DATA_DIR` | `.`, `data/` | `src/edcs_streamlit_map.py:28-29` | — |
| `MAP_LAYERS_DIR` | `data/map_layers/` | `src/edcs_streamlit_map.py:34` | R |
| `map_layer_path("roman_empire_ad_117", "roman_empire_ad_117.shp")` | `data/map_layers/roman_empire_ad_117/roman_empire_ad_117.shp` (+ sidecars) | `src/edcs_streamlit_map.py:240` | R |
| `map_layer_path("ba_roads", "ba_roads.shp")` | `data/map_layers/ba_roads/ba_roads.shp` (+ sidecars) | `:248` | R |
| `map_layer_path("Hanson2016_Cities_OxREP.csv")` | `data/map_layers/Hanson2016_Cities_OxREP.csv` | `:256` | R |
| `CLEANED_JSONL_FILE` | `data/edcs_inscriptions_cleaned.jsonl` | `:35` | R (preferred) |
| `CLEANED_JSONL_GZ` | `data/edcs_inscriptions_cleaned.jsonl.gz` | `:36` | R (fallback) |
| `LOOKUP_FILE` | `data/edcs_lookup.json` | `:37` | R |
| error message text | `"git checkout -- data/map_layers"` | `:231` | (message only) |

Download file names produced by the UI (in-memory, not written to disk):
`edcs_map_{term}.png`, `edcs_search_results.tsv`.

Remote URLs (not filesystem paths, but configuration that belongs in
`config/`): `BASE = https://edcs.hist.uzh.ch` and six derived endpoint URLs in
`src/edcs_scraper.py:87-94`; CARTO tile URLs and Leaflet CDN URLs in
`src/edcs_streamlit_map.py:83-90`.

### 3.2 Tests

| Path | File |
|---|---|
| `Path(__file__).parent / "fixtures"` → `tests/fixtures/` | `tests/conftest.py:8` |
| `Path(__file__).parent.parent / "src"` inserted on `sys.path` | `tests/test_cleaner_baseline.py:16` |
| `testpaths = ["tests"]` | `pyproject.toml` |
| ruff `exclude = ["data", "img"]`, per-file-ignores keyed by `src/edcs_*.py` and `main.py` | `pyproject.toml` |

### 3.3 Launcher

`epigcorpus.sh` resolves `SCRIPT_DIR` to its own directory, `cd`s there, and
references `$SCRIPT_DIR/.venv/bin/python` and `$SCRIPT_DIR/main.py`.

### 3.4 Research scripts (`scripts/`)

All but three compute `REPO = Path(__file__).resolve().parent.parent` and then
build paths as `REPO / "data" / ...`. **Every path below therefore depends on
`scripts/` sitting exactly one level under the directory that holds `data/`,
`reports/` and `results/`.**

| Path (relative to `REPO`) | R/W | Scripts |
|---|---|---|
| `data/edcs_inscriptions.jsonl` (plain; gitignored, must be decompressed first) | R | abbrev_probe, dd_diagnostic, exclusion_audit, phase0_explain_disagreements, phase0_second_implementation, phase0_verify, phase1_supplement, phase1_supplement2, phase1_supplement3, phase2_crossref, phase2_numeral_fusion, phase2_vinculum, phase3f_signal |
| `data/edcs_inscriptions.jsonl.gz` | R | phase0_verify |
| `data/derived/abbrev_pairs.tsv` (plain; gitignored) | W abbrev_probe; R phase0_*, phase2_numeral_fusion, phase3_build_v1, phase3_numeral_evidence |
| `data/derived/phase1_supplement.json` | W phase1_supplement; R phase1_supplement2 |
| `data/derived/phase1_supplement2.json` | W | phase1_supplement2 |
| `data/derived/phase1_supplement3.json` | W | phase1_supplement3 |
| `data/derived/phase2_crossref.json` | W | phase2_crossref |
| `data/derived/phase2_numeral_fusion.json` | W | phase2_numeral_fusion |
| `data/derived/phase2_vinculum.json` | W | phase2_vinculum |
| `data/derived/v1/abbrev_pairs_v1.tsv` (plain; gitignored) | W phase3_build_v1; R phase3_diff, phase3f_blockperm, phase3f_signal, phase4_build_splits, phase4_province_evidence |
| `data/derived/v1/phase3_stats.json` | W | phase3_build_v1 |
| `data/derived/v1/phase3_diff.json` | W | phase3_diff |
| `data/derived/v1/phase3f_blockperm.json` | W | phase3f_blockperm |
| `data/derived/v1/phase3f_signal.json` | W | phase3f_signal |
| `data/derived/v1/splits/*.tsv` + `split_manifest.json` | W | phase4_build_splits |
| `data/derived/v1/splits/*.tsv` | R | phase4_split_stats (plain only) |
| `data/derived/v1/splits/{name}.tsv` then `.tsv.gz` fallback | R | phase5_common (`SPLITS`) |
| `data/derived/v1/splits/split_stats.json` | W | phase4_split_stats |
| `results/cells/` (`mkdir` at import time) and `results/cells/{model}_{cond}_seed{n}.json` | R/W | phase5_common (`CELLS`), used by phase5_m1/m2/m3/eval/diag_AC/m2_alpha/m2_tune |
| `reports/abbrev_probe.md` | W | abbrev_probe |
| `reports/dd_diagnostic.md` | W | dd_diagnostic |
| `reports/exclusion_audit.md` | W | exclusion_audit |
| `reports/exclusion_audit_supplement.md` | W | phase1_supplement |
| `scripts/abbrev_probe.py`, `scripts/phase3_wordlists.py`, `scripts/phase5_*.py` loaded via `importlib` | R | dd_diagnostic, exclusion_audit, phase1_supplement[2,3], phase3_build_v1, phase3f_*, phase4_split_stats, phase5_* (some via `Path(__file__).parent`, some via `REPO/"scripts"`) |

**Absolute paths from the author's machine** (these scripts do not run anywhere
else today, before any move):

| File | Path |
|---|---|
| `scripts/phase0_explain_disagreements.py:4` | `/private/tmp/claude-501/-Users-sanoj-.../scratchpad/p0_second_impl.py` (a scratchpad file that is not in the repo) |
| `scripts/phase0_explain_disagreements.py:6` | `REPO = /Users/sanoj/Documents/Projects/EpigCorpus` |
| `scripts/phase0_second_implementation.py:13` | same `REPO` |
| `scripts/phase0_verify.py:6` | same `REPO` |

No script imports anything from `src/`.

### 3.5 Path references in prose

About 120 lines across `README.md`, `CHANGELOG.md`, `docs/*.md`,
`data/README.md`, `tests/README.md` and `reports/*.md` name paths such as
`src/edcs_cleaner.py` or `data/edcs_inscriptions.jsonl`. These are not code
paths. The reports and CHANGELOG entries are historical records of what was
true when they were written.

---

## 4. Data inventory

### 4.1 Git LFS

**Git LFS is not used, and that is deliberate.** `.gitattributes` says so in
its header comment. `git lfs ls-files` returns nothing, and no tracked file
starts with an LFS pointer header. Large files are committed **gzipped** to
stay under GitHub's 100 MB per-file limit. `.gitattributes` holds three rules:

```
*.gz binary
data/*.jsonl text eol=lf
data/*.tsv   text eol=lf
```

`*.gz` is path-independent. The two `data/*` rules are path-bound, and they
only match plain working copies (gitignored, so not tracked). They matter only
if someone force-adds a plain file. Git LFS is installed in this container
with `--skip` smudge configured, but that has no effect here.

The equivalent path-bound rules live in **`.gitignore`**, which matters more:
`data/*.jsonl`, `data/*.tsv`, `data/edcs_checkpoint.json`,
`data/edcs_index_cache.json`, `data/edcs_inscriptions_cleaned.csv`,
`data/derived/**/*.tsv` and their `!*.gz` negations. All of these must follow
the data when it moves, or plain working copies (540 MB) become committable.

All 100 files under `data/`, `results/`, `reports/` and `img/` are real files.
None of them is a pointer.

### 4.2 Folders

| Folder | Files (direct) | Size |
|---|---|---|
| `data/` | 7 | 231 MB total (incl. subfolders) |
| `data/derived/` | 7 | 117 MB incl. `v1/` |
| `data/derived/v1/` | 5 | 93 MB incl. `splits/` |
| `data/derived/v1/splits/` | 15 | 65 MB |
| `data/map_layers/` | 2 | 6.2 MB incl. subfolders |
| `data/map_layers/ba_roads/` | 6 | 5.8 MB |
| `data/map_layers/roman_empire_ad_117/` | 6 | 248 KB |
| `results/` | 0 | 4.5 MB total |
| `results/cells/` | 18 | 2.2 MB |
| `results/cells_m2_alpha1e-5/` | 9 | 1.2 MB |
| `results/cells_m2_alpha1e-7/` | 9 | 1.1 MB |
| `reports/` | 10 | 584 KB total |
| `reports/baseline_v0/` | 3 | 216 KB |
| `reports/env/` | 1 | 8 KB |
| `img/` | 2 | 1.9 MB |
| `tests/fixtures/` | 4 + 50 in `monuments/` | ~240 KB |

### 4.3 Every data file

Sizes in bytes. "Role" is my classification for the plan.

| Path | Bytes | Role | Written by |
|---|---:|---|---|
| `data/README.md` | 8,285 | doc | hand |
| `data/edcs_inscriptions.jsonl.gz` | 41,224,829 | raw corpus (JSONL) | scraper `compress_outputs` |
| `data/edcs_inscriptions.tsv.gz` | 13,015,690 | raw corpus (TSV) | scraper |
| `data/edcs_inscriptions_cleaned.jsonl.gz` | 55,589,253 | processed corpus | `main.py` |
| `data/edcs_lookup.json` | 3,352,023 | lookup / reference | scraper |
| `data/edcs_harvest_manifest.json` | 431 | harvest metadata | scraper |
| `data/edcs_failed_ids.json` | 29 | harvest metadata | scraper |
| `data/derived/abbrev_pairs.tsv.gz` | 24,388,751 | derived dataset | abbrev_probe (gzip by hand) |
| `data/derived/phase1_supplement.json` | 30,415 | analysis output | phase1_supplement |
| `data/derived/phase1_supplement2.json` | 10,524 | analysis output | phase1_supplement2 |
| `data/derived/phase1_supplement3.json` | 5,518 | analysis output | phase1_supplement3 |
| `data/derived/phase2_crossref.json` | 4,364 | analysis output | phase2_crossref |
| `data/derived/phase2_numeral_fusion.json` | 3,235 | analysis output | phase2_numeral_fusion |
| `data/derived/phase2_vinculum.json` | 3,646 | analysis output | phase2_vinculum |
| `data/derived/v1/abbrev_pairs_v1.tsv.gz` | 29,563,935 | derived dataset | phase3_build_v1 |
| `data/derived/v1/phase3_stats.json` | 35,339 | analysis output | phase3_build_v1 |
| `data/derived/v1/phase3_diff.json` | 12,775 | analysis output | phase3_diff |
| `data/derived/v1/phase3f_blockperm.json` | 4,137 | analysis output | phase3f_blockperm |
| `data/derived/v1/phase3f_signal.json` | 10,152 | analysis output | phase3f_signal |
| `data/derived/v1/splits/SHA256SUMS.txt` | 1,068 | integrity manifest (hashes of the **plain** `.tsv`) | hand |
| `data/derived/v1/splits/split_manifest.json` | 1,452 | split metadata | phase4_build_splits |
| `data/derived/v1/splits/split_stats.json` | 7,022 | analysis output | phase4_split_stats |
| `data/derived/v1/splits/*.tsv.gz` (12 files) | 44,014 – 28,059,324 | derived dataset (frozen ML splits) | phase4_build_splits |
| `data/map_layers/README.md` | 3,247 | doc | hand |
| `data/map_layers/Hanson2016_Cities_OxREP.csv` | 226,452 | reference (third-party) | vendored |
| `data/map_layers/ba_roads/*` (6) | 145 – 4,331,922 | reference (third-party shapefile) | vendored |
| `data/map_layers/roman_empire_ad_117/*` (6) | 145 – 214,636 | reference (third-party shapefile) | vendored |
| `results/cells/*.json` (18) | ~126 KB each | ML results (M1, M2 final) | phase5_m1 / phase5_m2 |
| `results/cells_m2_alpha1e-5/*.json` (9) | ~127 KB each | ML results (superseded M2 run, kept as evidence) | renamed by hand in `a5dd25e` |
| `results/cells_m2_alpha1e-7/*.json` (9) | ~127 KB each | ML results (superseded M2 run) | renamed by hand in `a5dd25e` |
| `reports/abbrev_probe.md` | 46,648 | report | abbrev_probe |
| `reports/dd_diagnostic.md` | 39,043 | report | dd_diagnostic |
| `reports/exclusion_audit.md` | 125,941 | report | exclusion_audit |
| `reports/exclusion_audit_supplement.md` | 21,011 | report | phase1_supplement |
| `reports/baseline_v0/{abbrev_probe,dd_diagnostic,exclusion_audit}.md` | same | frozen copies, **byte-identical** to the three above | hand |
| `reports/decisions.md` | 53,917 | provenance log (D-0001 …) | hand |
| `reports/phase0_reproduction.md` | 6,420 | report | hand |
| `reports/phase3_corrections.md` | 23,348 | report | hand |
| `reports/phase4_splits.md` | 15,662 | report | hand |
| `reports/related_work.md` | 1,917 | notes | hand |
| `reports/vinculum_check.md` | 10,722 | report | hand |
| `reports/env/phase0_environment.md` | 3,148 | environment record | hand |
| `img/App video.gif` | 1,334,379 | README asset | hand |
| `img/Example export png image for term viator.png` | 643,836 | README asset (CHANGELOG notes it is out of date) | hand |

### 4.4 Generated locally, absent from a clone

`data/edcs_inscriptions.jsonl`, `data/edcs_inscriptions.tsv`,
`data/edcs_inscriptions_cleaned.jsonl`, `data/edcs_checkpoint.json`,
`data/edcs_index_cache.json`, `data/derived/abbrev_pairs.tsv`,
`data/derived/v1/abbrev_pairs_v1.tsv`, `data/derived/v1/splits/*.tsv`. None
exists in this checkout.

---

## 5. Observations that matter for the refactor

1. **`main.py` picks its input by mtime across `data/*.jsonl[.gz]`, excluding
   names with `_cleaned`.** If raw and processed files end up in separate
   folders, that glob has to point at the raw folder only. The `_cleaned`
   exclusion then becomes redundant, but it should stay so behaviour is
   identical. After a `git mv` or a fresh clone, mtimes are checkout times.
   Today only one candidate exists (`edcs_inscriptions.jsonl.gz`), so the
   choice is unambiguous either way.
2. **The scraper is executed as a script, not imported.** `main.py` runs
   `python src/edcs_scraper.py` as a subprocess. To keep stdout and exit-code
   behaviour identical, the new layout should keep a subprocess boundary
   (`python -m app.cli.scrape`) rather than calling the harvest in-process.
3. **Streamlit runs a file by path.** `streamlit run` puts the script's own
   directory on `sys.path`, not the repo root, so `app/ui/…` cannot
   `import app.…` unless the root is importable (installed package or
   `PYTHONPATH`). This needs a decision (see plan).
4. **The Streamlit module mixes four layers.** Search/filter logic
   (`apply_filters`, `_token_set`, `aggregate_places`, `describe_query`) is
   pure pandas and can move to `core/` and `services/` unchanged. Map-layer
   loading (geopandas) belongs in `adapters/`. Everything `st.*` stays in `ui/`.
   Many functions carry `@st.cache_data`. Moving them out of `ui/` means
   either keeping the decorator at the UI call site or importing
   streamlit outside the UI (a layering violation). The plan handles this by
   wrapping in `ui/`.
5. **The scraper also mixes layers**: HTTP (`adapters`), pure parsing
   (`format_citation`, `format_dating`, `parse_monument` → `core`), checkpoint,
   manifest and failed-ID files (`adapters/storage`), and orchestration
   (`harvest` → `services`). It uses `os.path` strings, not `Path`. Changing
   that is a logic change, so the string form stays.
6. **Research scripts are tied to the folder layout.** They use
   `REPO/"data"/"derived"/...`, `REPO/"reports"`, `REPO/"results"`. If their
   content stays unchanged and the data is regrouped, they stop resolving.
   This is the main conflict in the brief, and the plan raises it as a
   decision.
7. **Three phase-0 scripts are already non-portable** (absolute
   `/Users/sanoj/...` and `/private/tmp/...` paths). Moving them changes
   nothing about that.
8. **`data/derived/v1/splits/SHA256SUMS.txt` hashes the plain `.tsv`
   files**, which are not committed. It cannot be checked from a clone
   without decompressing first. The refactor's own checksum pass hashes the
   committed bytes, so this file is unaffected.
9. **`reports/baseline_v0/` duplicates three reports byte for byte.** This is
   a deliberate frozen baseline, not an accident, so it is kept.
10. **Ruff per-file-ignores are keyed by path** (`src/edcs_scraper.py`, etc.).
    If those modules are split, the ignores have to follow the code, or ruff
    goes red on lines that did not change.
11. **No `.github/`**, so there is no CI to update.
12. **`ruff check .` is red on `main` today: 532 findings, every one in
    `scripts/`** (E702/E701 semicolon one-liners 371, E501 42, I001 26, E401
    23, F401 14, …). `src/`, `main.py` and `tests/` are clean under their
    per-file-ignores. Because the scripts must move "unchanged in content",
    "ruff passes" can only hold if the scripts are excluded from lint.
13. **`filter_inscriptions` in `src/edcs_streamlit_map.py:403` is never
    called.** It is dead code. It moves unchanged and is listed in `unused.md`.
14. **Running `main.py --skip-scrape --skip-map` overwrites the tracked
    `data/edcs_inscriptions_cleaned.jsonl.gz`.** Gzip embeds an mtime, so
    the bytes change even when the content does not. Any CLI verification
    run must happen in a throwaway worktree, or the data checksums break.
15. **No fixture-level cleaning run exists today.** The brief's
    "fixture cleaning run matches the baseline byte for byte" needs a
    baseline to be generated in Phase 2 step 1, before any code moves. The
    plan proposes one.

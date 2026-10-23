# Phase 2 — Verification after the restructure

Run 2026-10-23 on the restructured tree, against `docs/refactor/baseline.md`.
Every check that writes data ran in a throwaway `git worktree` in the session
scratchpad. This checkout's `app/data` was never written to.

## Summary

| Check | Result |
|---|---|
| `ruff check .` | **All checks passed** (`app/scripts` excluded, see plan §4.1) |
| `pytest` | **95 passed, 16 xfailed**: the baseline's 85 tests with identical IDs (modulo path) and outcomes, plus 26 new layering tests |
| `python -m app.main --help` | **byte-identical** to the baseline `python main.py --help` |
| `python -m app.cli.scrape --help` | identical except argparse's `prog`: `scrape.py` instead of `edcs_scraper.py` |
| Full-corpus cleaning (`python -m app.main --skip-scrape --skip-map`) | **byte-identical**: plain `edcs_inscriptions_cleaned.jsonl` and decompressed `.gz` both `54b9ee378f6d20648ddafd8652d53a2301119f9044f0b474c2419f8506d79699`, same as the baseline and as the committed snapshot |
| Its stdout | identical apart from file paths, except two deprecation warnings that are no longer shown (see below) |
| Fixture harvest, with failures and complete | **byte-identical** JSONL, TSV, both `.gz`, checkpoint, failed ids, lookup, index cache, manifest. Stdout identical apart from paths. Checkpoint removed on completion as before |
| Fixture cleaning run | **byte-identical** output (`81db7815…e1`) and verbose stdout (`31d3412f…5c`) |
| Streamlit probes | **identical** to the baseline on every probe: known places, filter options, short forms, three queries, query descriptions, both PNG exports (byte for byte), both map HTML documents, results TSV |
| AppTest | no exception. Same widgets. Search for `viator` gives "515 inscriptions match. Searched in raw inscription text." |
| Streamlit caching | corpus read once at start, the full table once at the first search, no further reads across a re-search, a mode change and a filter change. The lookup is read twice (short forms, known places) and each map layer once, as before |
| `streamlit run app/ui/streamlit_app.py` via the console script (the `uv run streamlit` fallback path in `main.py`) | server healthy (`/_stcore/health` → `ok`). In headless Chromium the page shows "All known places: 21,999", then "515 inscriptions match" after typing `viator`, with no exception element |
| UI import bootstrap | `runpy.run_path` on the UI file from `/tmp` under `python -I` (repo root not on `sys.path`) imports cleanly |
| Research scripts | `phase5_common` loads `primary_val` (139,591 rows) from the new splits folder. Every directory the scripts read or write exists |
| Layering | `test_layering.py` green. It fails as expected on a deliberate services→adapters import and on a streamlit import in core (probe reverted) |
| Data checksums | **154/154** files hash-identical through `path-map.tsv` (`checksums-after.txt`, taken right after the move). Since then exactly one data file has changed, `app/data/README.md`, which was edited on purpose for decision D7 |
| Git LFS | still none (`git lfs ls-files` empty) |
| `.gitattributes` / `.gitignore` | `check-attr` and `check-ignore` give the same answers on the new paths as on the old ones |

## Differences found, all expected or explained

1. **Printed paths.** Stdout of the scraper and the pipeline shows the new
   absolute paths (`…/app/data/raw/…`). This was planned (plan §2).
2. **Scraper `--help` `prog`.** `usage: scrape.py …` instead of
   `usage: edcs_scraper.py …`. Argparse takes it from the file name. The flags
   and help text are identical.
3. **Two `Pandas4Warning` lines no longer appear on stderr** during cleaning.
   `Pandas4Warning` subclasses `DeprecationWarning`, and Python's default
   filters show those only when they are raised from `__main__`.
   `save_cleaned_data` used to live in the `__main__` script and now lives in
   `app.adapters.corpus_files`, so the warning is filtered out. The output
   files are byte-identical. Restoring the message would mean adding a
   warnings filter, which I did not do without asking.
4. **Order of two startup steps in the scraper.** The SIGTERM handler is now
   installed just before the output directories are created, instead of just
   after. Neither depends on the other, and the harness outputs are
   identical.
5. **The scraper creates `raw/`, `raw/harvest/` and `reference/`** instead of
   the single `data/` directory. This is a necessary consequence of the
   role-grouped layout (plan §2).

## Not verifiable in this environment

- **The Leaflet map's tiles and script.** The page loads Leaflet from
  `unpkg.com` and tiles from `basemaps.cartocdn.com`, and this sandbox's
  network policy rejects both hosts, so the map iframe draws nothing here.
  That was equally true before the move. The HTML handed to the iframe is
  byte-identical to the baseline's.
- **A live harvest against EDCS.** It was not attempted (network policy, and
  load on a third-party server). The harness serves the committed EDCS
  fixtures through the real `requests` session code path instead.
- **`./epigcorpus.sh` end to end.** It `exec`s `python -m app.main "$@"`.
  `python -m app.main --help` and the `--skip-scrape --skip-map` run were
  verified directly. The script's other lines did not change.

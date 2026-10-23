# Questionable files and code

Found during the `app/` restructure. **Nothing here was deleted.** Each item
needs a decision from the maintainer.

## Dead code

| Where | What | Evidence |
|---|---|---|
| `app/core/search.py` `filter_inscriptions` (was `src/edcs_streamlit_map.py:403`) | Never called. It is superseded by `apply_filters`, which also handles the filters, and it returns an empty frame for an empty term where `apply_filters` returns all rows. | `grep -rn "filter_inscriptions(" app` finds only the definition. |
| `app/ui/streamlit_app.py` `REPO_URL`, `REPO_NAME` | Defined, never read. | `grep -n "REPO_URL\|REPO_NAME" app/ui/streamlit_app.py` finds only the assignments. |

## Scripts that cannot run outside the author's machine

These were moved byte-identical. They failed outside the author's machine
before the move too.

| Script | Problem |
|---|---|
| `app/scripts/phase0_explain_disagreements.py` | Reads `/private/tmp/claude-501/.../scratchpad/p0_second_impl.py`, a scratchpad file that was never committed, and hard-codes `REPO = /Users/sanoj/Documents/Projects/EpigCorpus`. |
| `app/scripts/phase0_second_implementation.py` | Hard-codes the same `REPO`. |
| `app/scripts/phase0_verify.py` | Hard-codes the same `REPO`. |

Options: give them the same path-only edit as the other scripts (the
scratchpad dependency of the first one cannot be fixed that way), or mark
them as historical records of phase 0.

## Duplicated data

| Files | Note |
|---|---|
| `app/data/reports/baseline_v0/{abbrev_probe,dd_diagnostic,exclusion_audit}.md` vs `app/data/reports/{same}.md` | Byte-identical today (`sha256` equal). Probably a deliberate frozen baseline. They will diverge once the live reports are regenerated. Keep unless you know otherwise. |

## Stale content (not code)

| Where | What |
|---|---|
| `docs/img/Example export png image for term viator.png` | `CHANGELOG.md` already records it as showing the old white export. |
| `app/main.py --help` | "Skip scraping and only clean the latest data/*.jsonl file." The input folder is now `app/data/raw/`. Text kept identical so the CLI help did not change, so fixing it is your call. |
| `app/adapters/corpus_files.py` `load_latest_data` | Error message "No JSONL file found in 'data/' after running scraper." Same reason. |
| `app/data/processed/derived/v1/splits/SHA256SUMS.txt` | Hashes the **plain** `.tsv` splits, which are not committed. It can only be checked after decompressing. |
| `app/data/README.md`, last caveat | Says the map's bounding box discards ~9.3% of the corpus, including all of Britannia. The bounds were widened since then: today 19,949 rows are dropped, 19,938 of them for having no coordinates. This was already stale before the move and is left for you. |
| `docs/AUDIT.md`, `docs/CLEANING_COMPARISON.md`, `docs/LATEPIG_BREAKAGE.md`, `app/data/reports/*.md`, `CHANGELOG.md` | Cite pre-move paths (`src/…`, `data/…`). These are historical records and were not rewritten. See `docs/refactor/path-map.tsv`. |

## Lint debt carried over

`app/scripts/` is excluded from ruff. It carried 532 findings at the time of
the move (see `docs/refactor/baseline.md`). The remaining per-file ignores in
`pyproject.toml` are the original T19/T38/T40 debt, now named by the module
each line moved to.

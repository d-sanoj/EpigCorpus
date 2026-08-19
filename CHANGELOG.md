# Changelog

All notable changes to EpigCorpus. Per remediation ground rule 2, every change
to `src/edcs_cleaner.py` alters what the corpus *is* and must be recorded here
with the epigraphic convention it implements.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added — corpus snapshots committed, compression automated (2026-08-19)

- Full EDCS harvest committed as gzipped snapshots: **542,854 monuments /
  588,509 inscription rows**, harvested 2026-08-19 against EDCS release
  `20260807-142626`. 39 MB and 12 MB compressed (13.7x and 8.2x).
- **Compression is automatic in both directions.** The scraper compresses on
  completion; `main.py` and the map read plain or gzipped transparently,
  preferring a plain file when present. No user ever runs gzip or gunzip.
- `main.py` also writes `edcs_inscriptions_cleaned.jsonl.gz` alongside the
  plain cleaned corpus.
- `.gitignore` now ships `*.jsonl.gz` / `*.tsv.gz` and ignores the plain working
  copies and the regenerable index cache.
- **Stale Git LFS rules removed from `.gitattributes`.** They marked
  `data/*.json` and `data/*.tsv` as LFS-managed while git-lfs was not installed,
  which would have committed broken pointer files.

### Fixed — scraper could destroy a completed harvest (2026-08-19)

- A completed harvest deletes its checkpoint as the "done" signal, but `main()`
  only looked for a checkpoint. With none present the cursor stayed at 0 and
  `harvest()` opened the output files in `"w"` mode — **silently truncating a
  finished 540 MB corpus on any plain re-run**. The "already complete" branch
  could only ever fire while a checkpoint existed, i.e. when the harvest was
  *not* complete.
- Fixed with two independent guards: `data/edcs_harvest_manifest.json` records
  completion durably, and `harvest()` now refuses to truncate any non-empty
  output file regardless of how it was reached.
- SIGTERM now stops a harvest as cleanly as Ctrl+C. Background jobs launched
  from a non-interactive shell inherit SIGINT set to ignore, so SIGTERM was the
  only way to stop a detached run — and it killed the process mid-chunk, leaving
  output ahead of the checkpoint.

### Changed — base-map layers vendored (2026-08-19)

- `data/lat_epig_support/` renamed to **`data/map_layers/`** (via `git mv`, so
  file history is preserved). The folder never held third-party project data —
  it holds AWMC province and road shapefiles and the Hanson 2016 OXREP cities
  CSV. It was named after whichever repository the files had been mirrored from.
- **Runtime downloads removed.** `edcs_streamlit_map.py` no longer fetches
  shapefiles from `raw.githubusercontent.com` at startup. All 13 layer files are
  committed, so the map now loads them from the repository and raises an
  actionable error if any are missing. This removes the last runtime dependency
  on an external repository. (T30)
- `data/map_layers/README.md` added with per-layer provenance, licence terms
  (AWMC is ODbL-1.0; MIT covers EpigCorpus code only) and attribution, plus a
  note that the AD 117 province layer is a reference frame, not a
  contemporaneous basemap. (partial T28)
- Scraper field comments rewritten to describe each column on its own terms.

### Changed — scraper port to the 2026 EDCS (2026-08-19)

- **`src/edcs_scraper.py` rewritten** for the EDCS release of 2026-08-07.
  The old DataTables harvest against `/api/query` (403 since that release) is
  replaced by the static-file architecture: one request for the full corpus
  index, then one request per monument. See `docs/EDCS_API.md`.
- **Coordinate order corrected.** The new `places.json` returns
  `[latitude, longitude]`; the old API returned `[longitude, latitude]`. The
  original hardcoded `longitude = coord[0]`, which would have transposed the
  entire corpus on port. (T34, partial)
- **Full LatEpig 2.0 field parity.** Added `publication`, `raw_dating`,
  `dating_from`, `dating_to`, `status`, `comment`, `photo`, plus
  `language_codes` and `photo_credits`. `publication` and `raw_dating` use the
  site's own `citationLabel()` and `formatDatingRange()` formats so our columns
  are joinable against EDCS's own rendering. Not portable: `partner_link` and
  `TM Place` were links on the retired PHP pages and no longer exist;
  `extra_text` / `extra_html` were HTML-scraping leftovers.
- **Identifying User-Agent** replaces the spoofed Firefox UA, forged `Referer`
  and `X-Requested-With`. Verified to be served normally by the static
  endpoints. (T26)
- **Failed fetches are recorded, not swallowed.** Retries with exponential
  backoff, honours `Retry-After`, writes unrecoverable ids to
  `data/edcs_failed_ids.json`, and refuses to report a harvest as complete while
  that file is non-empty. (T19, T27, partial)
- **Record-level provenance**: `retrieved_at` and `source_url` on every row. (T17, partial)
- Existing column names and output paths are unchanged, so `main.py`,
  `edcs_cleaner.py` and `edcs_streamlit_map.py` run untouched.

The tasks marked *partial* were completed only insofar as the port required
them; their full scope in Phases 1-3 is unchanged.

### Added — Phase 0 (2026-08-19)

- `docs/AUDIT.md` (T00) — data-flow audit: every record-drop point, every
  mutation point, every user-visible count, with measured loss figures.
- `docs/LATEPIG_BREAKAGE.md` (T01) — reproduction of the LatEpig failure against
  the current EDCS, and of the equivalent EpigCorpus failure.
- `docs/EDCS_API.md` (T02) — characterisation of the EDCS release of
  2026-08-07, which replaced the API this project targets.
- `tests/` (T03) — pytest suite with 50 real monument payloads plus index
  fixtures captured 2026-08-19; runs entirely offline.
- `.github/workflows/ci.yml` (T03) — CI running `ruff check` and `pytest`.
- `pytest` and `ruff` dev dependencies, with a dated per-file lint-debt baseline
  in `pyproject.toml` to be deleted task by task.

### Notes

No pipeline code was changed in Phase 0. Corpus semantics are unaffected.

Two findings correct the remediation brief:

- **T04** — `is_forged` is not dead code. EDCS marks *falsae* with a `*` prefix
  on the citation number (4,719 instances in CIL alone, verified 2026-08-19),
  and the existing check fires on it correctly. See `docs/AUDIT.md` §4 F1.
- **T09** — the brief's example `((sestertium))` parses correctly. The rule
  fails on genuinely nested parentheses, e.g. `(A(uli) f(ilius))`.

**Blocking:** the EDCS release of 2026-08-07 withdrew `/api/query`, which
returns 403. The harvester does not run. Phases 1–7 assume it does.
See `docs/LATEPIG_BREAKAGE.md` §7.

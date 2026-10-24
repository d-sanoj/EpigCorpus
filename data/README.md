# Data directory

Corpus exports, lookup tables, harvest metadata, base-map layers and research
datasets, grouped by role:

| Folder | Role |
|---|---|
| `raw/` | What the scraper fetched, untransformed; `raw/harvest/` holds its state |
| `processed/` | Deterministic transforms of raw: the cleaned corpus and derived datasets |
| `reference/` | Lookup tables and vendored third-party map layers |

Analysis outputs and research reports live in `research/results/` and
`research/reports/`.

Every path is defined in `backend/src/epigcorpus/config/paths.py`, and
`EPIGCORPUS_DATA_DIR` moves the whole directory (the Docker image mounts it at
`/app/data`). Files have moved twice. First into `app/data/`
(`docs/refactor/path-map.tsv`), then to this top-level `data/` and to
`research/` (`docs/refactor/path-map-backend.tsv`). The reports keep their
original wording, so use those tables to follow a path they cite.

**Current snapshot:** harvested 2026-08-19 against EDCS release
`20260807-142626` — 542,854 monuments, 588,509 inscription rows, 0 failed
fetches. See `raw/harvest/edcs_harvest_manifest.json`.

---

## Contents

### Committed to the repository

| File | Size | What it is |
|---|---|---|
| `raw/edcs_inscriptions.jsonl.gz` | ~39 MB | The corpus. One inscription per line. |
| `raw/edcs_inscriptions.tsv.gz` | ~12 MB | Same records, tab-delimited. |
| `reference/edcs_lookup.json` | 3.2 MB | Flattened EDCS code tables used for the harvest: materials, categories, provinces, languages, places, citation sources. |
| `raw/harvest/edcs_harvest_manifest.json` | <1 KB | Completion record: counts, timestamps, index snapshot, version. |
| `raw/harvest/edcs_failed_ids.json` | <1 KB | Monuments that could not be fetched. Empty for a clean harvest. |
| `processed/edcs_inscriptions_cleaned.jsonl.gz` | ~53 MB | Cleaned corpus with conservative and interpretive text columns. |
| `processed/derived/` | ~93 MB | Abbreviation-study datasets and frozen splits (see `reports/phase4_splits.md`). |
| `reference/map_layers/` | 6.2 MB | Base-map layers. See `reference/map_layers/README.md`. |
| `results/` | 4.5 MB | Analysis outputs (`analysis/`) and model cells (`cells*/`). |
| `reports/` | 0.6 MB | Research reports and the decision log. |

### Generated locally, not committed

| File | What it is |
|---|---|
| `raw/edcs_inscriptions.jsonl` / `.tsv` | Uncompressed working copies (~540 MB / ~102 MB), written during a harvest. |
| `processed/edcs_inscriptions_cleaned.jsonl` | Uncompressed cleaned corpus, written by the cleaning step. |
| `raw/harvest/edcs_checkpoint.json` | Resume cursor. Present only mid-harvest; deleted on completion. |
| `raw/harvest/edcs_index_cache.json` | Cached corpus index (~5 MB). Regenerable with `--refresh-index`. |

---

## Compression

The corpus is committed gzipped because the uncompressed JSONL is ~540 MB,
well over GitHub's 100 MB per-file limit. Compression is ~13.7x.

Git LFS is deliberately **not** used: the free tier allows 1 GB of bandwidth per
month, so roughly one and a half clones of a 651 MB corpus would exhaust it for
everyone, reviewers included.

**No manual compression or decompression is ever needed.**

- The scraper writes plain `.jsonl`/`.tsv` during a harvest — appending to a
  gzip stream across a checkpointed resume is not safe — then compresses them
  automatically on completion.
- The pipeline and the API read either form. pandas infers gzip from the
  file extension; the loaders prefer a plain file when one exists and fall back
  to the `.gz` snapshot.
- The server keeps a Parquet copy of the cleaned corpus,
  `processed/edcs_inscriptions_cleaned.parquet`. It reads that copy in about a
  second, where the JSONL takes most of a minute. The copy is tagged with the
  JSONL's name, size and modification time, rebuilt whenever the JSONL
  changes, and never committed.
- `.gitignore` ships only the `.gz` and ignores the plain working copies.

A fresh clone therefore contains only the compressed snapshot and runs
immediately. After you re-harvest, the plain files take precedence locally and
fresh `.gz` copies are regenerated for committing.

---

## Workflow

The pipeline runs through the API: the **Pipeline** panel in the web UI, or
`POST /api/v1/pipeline-runs` with a JSON body.

```text
{}                                   harvest, clean, reload the server's corpus
{"harvest": false}                   clean and reload from existing raw data
{"clean": false}                     harvest only
{"limit": 500}                       smoke test on 500 monuments
{"workers": 8}                       gentler on the server (default 16)
{"refresh_index": true}              re-fetch the index; check if EDCS grew
{"restart": true}                    DELETES existing output, re-harvests
```

Harvesting against a completed corpus is safe: the stage reads the manifest,
reports `already_complete`, and touches nothing. It also refuses to overwrite
any non-empty output file unless `restart` is given.

An interrupted or cancelled harvest resumes from its checkpoint automatically:
start another run. Monuments that failed on an earlier run are retried before
the harvest continues, and a harvest is never reported complete while
`edcs_failed_ids.json` is non-empty.

---

## Schema

28 columns. The first block is the original EpigCorpus schema; the rest were
added with the 2026 API port.

### Identity and provenance

| Column | Notes |
|---|---|
| `record_id` | `EDCS-00000001-0` — monument id plus inscription index |
| `edcs_id` | `EDCS-00000001` |
| `inscription_index` | 0-based; a monument may carry several inscriptions |
| `retrieved_at` | ISO 8601 UTC fetch time |
| `source_url` | Exact URL this record came from |

### Place

| Column | Notes |
|---|---|
| `province` | Resolved from the place index |
| `place` | Findspot / settlement |
| `latitude`, `longitude` | Decimal degrees, WGS 84 |

Coordinates are findspot- or settlement-level, so many inscriptions share
identical points. The source `coord` field is `[latitude, longitude]` — the
reverse of the pre-2026 API, which returned `[longitude, latitude]`.

### Text and description

| Column | Notes |
|---|---|
| `inscription_text` | Raw EDCS text with editorial markup intact |
| `language`, `language_codes` | e.g. `Latin` / `la` |
| `material`, `material_en` | Latin token and English label |
| `category`, `category_en` | Inscription genus / personal status, as lists |
| `status` | Same values as `category_en`, under the conventional name |
| `comment` | Editorial comments. Not available before the 2026 API. |

### Dating

| Column | Notes |
|---|---|
| `not_before`, `not_after` | Integer years; **negative means BC** |
| `dating_from`, `dating_to` | Same values under the conventional names |
| `raw_dating` | Display form: `101 .. 299`, a bare year, or `-` if undated |

Only 37.3% of rows carry any dating.

### Bibliography and images

| Column | Notes |
|---|---|
| `belege` | Citations as a list |
| `publication` | Same citations as one string, e.g. `CIL-06, 00001` |
| `image_urls`, `photo` | Resolved image URLs, pipe-separated |
| `photo_credits` | Attribution where EDCS supplies it |

Citation and dating strings follow EDCS's own `citationLabel()` and
`formatDatingRange()` output, so these columns join against EDCS's rendering.

**A `*` prefix on a citation number marks a forgery** (*falsa*) — e.g.
`CIL-06, *00226`. This is the only forgery marker the API exposes.

### Added by the cleaning stage

`inscription_text_conservative`, `inscription_text_interpretive`,
`is_unreadable`, `is_forged`.

> The cleaning pipeline has known defects that alter corpus semantics — see
> `docs/AUDIT.md` §3. Treat cleaned text as provisional until those are fixed.

---

## Coverage

Measured on the 2026-08-19 snapshot, over 588,509 rows.

| Field | Coverage |
|---|---|
| Inscription text | 100% |
| Language | 100% |
| Province / place | 99.4% |
| Citations | 99.2% |
| Coordinates | 96.6% |
| Material | 86.1% |
| Category | 82.0% |
| Dated | 37.3% |
| Images | 20.3% |
| Comments | 3.1% |

30,503 monuments carry more than one inscription.

Largest provinces: Roma 132,109 · Latium et Campania 50,159 · Africa
proconsularis 37,377 · Hispania citerior 24,874 · Gallia Narbonensis 24,174.

Languages: Latin 540,410 · Greek 26,968 · indistinct 12,416 · Greek and Latin
7,021, plus Iberian, Etruscan, Oscan, Palmyrene and others.

---

## Reading the data

```python
import pandas as pd

# Works on either form — pandas infers gzip from the extension
df = pd.read_json("data/raw/edcs_inscriptions.jsonl.gz", lines=True)
```

In the TSV, list-valued columns (`category`, `category_en`, `belege`, `status`,
`language_codes`) are pipe-separated. In the JSONL they stay as lists. Prefer
the JSONL for analysis — inscription text contains newlines, which the TSV
quotes but which make naive line-counting of that file wrong.

---

## Provenance and licensing

Inscriptions come from the **Epigraphik-Datenbank Clauss / Slaby**,
<https://edcs.hist.uzh.ch/>. The project's MIT licence covers **code only**.
EDCS-derived text carries EDCS's own terms, and redistribution terms for this
corpus are still to be settled (T29).

Base-map layer sources and licences: `reference/map_layers/README.md`.

## Caveats

- **This snapshot is not a citable deposit.** A Zenodo release with a DOI (T21)
  is what a paper should cite.
- **EDCS is actively revised and periodically rebuilt.** This snapshot reflects
  one moment. The harvester has been broken by a rebuild before — see
  `docs/LATEPIG_BREAKAGE.md`.
- **No forgery filtering is applied.** Rows whose citations carry `*` are
  *falsae* and are included.
- **The map's bounding box discards ~9.3% of the corpus**, including all of
  Britannia. That filter lives in the map, not the data — the corpus here is
  complete. See `docs/AUDIT.md` §2.

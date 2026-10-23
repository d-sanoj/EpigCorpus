# Data directory

Corpus exports, lookup tables, harvest metadata and base-map layers used by
EpigCorpus.

**Current snapshot:** harvested 2026-08-19 against EDCS release
`20260807-142626` — 542,854 monuments, 588,509 inscription rows, 0 failed
fetches. See `edcs_harvest_manifest.json`.

---

## Contents

### Committed to the repository

| File | Size | What it is |
|---|---|---|
| `edcs_inscriptions.jsonl.gz` | ~39 MB | The corpus. One inscription per line. |
| `edcs_inscriptions.tsv.gz` | ~12 MB | Same records, tab-delimited. |
| `edcs_lookup.json` | 3.2 MB | Flattened EDCS code tables used for the harvest: materials, categories, provinces, languages, places, citation sources. |
| `edcs_harvest_manifest.json` | <1 KB | Completion record: counts, timestamps, index snapshot, version. |
| `edcs_failed_ids.json` | <1 KB | Monuments that could not be fetched. Empty for a clean harvest. |
| `map_layers/` | 6.2 MB | Base-map layers. See `map_layers/README.md`. |

### Generated locally, not committed

| File | What it is |
|---|---|
| `edcs_inscriptions.jsonl` / `.tsv` | Uncompressed working copies (~540 MB / ~102 MB), written during a harvest. |
| `edcs_inscriptions_cleaned.jsonl` | Cleaned corpus with conservative and interpretive text columns. |
| `edcs_inscriptions_cleaned.jsonl.gz` | Compressed cleaned corpus. |
| `edcs_checkpoint.json` | Resume cursor. Present only mid-harvest; deleted on completion. |
| `edcs_index_cache.json` | Cached corpus index (~5 MB). Regenerable with `--refresh-index`. |

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
- `main.py` and the Streamlit map read either form. pandas infers gzip from the
  file extension; the loaders prefer a plain file when one exists and fall back
  to the `.gz` snapshot.
- `.gitignore` ships only the `.gz` and ignores the plain working copies.

A fresh clone therefore contains only the compressed snapshot and runs
immediately. After you re-harvest, the plain files take precedence locally and
fresh `.gz` copies are regenerated for committing.

---

## Workflow

```bash
python main.py                    # harvest, clean, launch the map
python src/edcs_scraper.py        # harvest only
python main.py --skip-scrape      # clean and launch from existing data
```

Scraper options:

```bash
python src/edcs_scraper.py --limit 500      # smoke test on 500 monuments
python src/edcs_scraper.py --workers 8      # gentler on the server (default 16)
python src/edcs_scraper.py --refresh-index  # re-fetch the index; check if EDCS grew
python src/edcs_scraper.py --restart        # DELETES existing output, re-harvests
```

Running the scraper against a completed harvest is safe: it reads the manifest,
reports the corpus is already complete, and exits without touching anything. It
also refuses to overwrite any non-empty output file unless `--restart` is given.

An interrupted harvest resumes from its checkpoint automatically — just run it
again with no flags. Monuments that failed on an earlier run are retried before
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
df = pd.read_json("data/edcs_inscriptions.jsonl.gz", lines=True)
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

Base-map layer sources and licences: `map_layers/README.md`.

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

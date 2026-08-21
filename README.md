# EpigCorpus

**A benchmark for expanding Latin epigraphic abbreviations — and an audit of the
transcription conventions that distort any dataset derived from EDCS.**

> **How is this different from LatEpig?**
> LatEpig is a *retrieval* tool — it reproducibly executes a search against EDCS
> and exports the matching records. EpigCorpus is a *derived labelled dataset and
> benchmark* — it mines EDCS's editorial parentheses as ground-truth labels for
> abbreviation expansion, and measures the conventions that silently distort any
> such derivation. One retrieves what EDCS holds; the other measures what EDCS's
> conventions do to anything built from it.

EDCS marks the editorial expansion of ancient abbreviations in round
parentheses: the stone reads `D M`, the edition prints `D(is) M(anibus)`. Those
parentheses are free ground-truth labels. From 588,509 records we extract
**1,424,314 (abbreviation, expansion) pairs**.

## Findings

| | |
| --- | --- |
| **The vinculum is not preserved.** A full census of 39,470,885 characters finds U+0305 **twice** and U+0304 **not at all**. EDCS renders the overline as a supplied word — `HS X(milia)` — 65.4% of the time preceded by `HS`. | [vinculum_check.md](reports/vinculum_check.md) |
| **One `\|` stands for ≥8 unrelated signs.** 376 distinct `\|(...)` forms, 16,194 occurrences, with plurality marked by repetition and its own unresolvable class. | [exclusion_audit_supplement.md](reports/exclusion_audit_supplement.md) |
| **Geminatio doubles the FINAL letter**, not the first — `Aug→Augg`, `Imp→Impp`. A leading-letter rule undercounts by 41%. 8,986 pairs corrected. | [phase3_corrections.md](reports/phase3_corrections.md) |
| **Line-break fragmentation.** EDCS breaks words across lines, so `v/` + `ix(it)` yields the spurious pair `ix → ixit`. 9,506 rows. | [phase3_corrections.md](reports/phase3_corrections.md) |
| **The exclusion filter reshapes the corpus**, it does not merely filter it: province TVD 10.9× and century 16× a bootstrap null. | [exclusion_audit_supplement.md](reports/exclusion_audit_supplement.md) |
| **Province conditioning is memorisation.** Worth **+8.3 points** on provinces seen in training and **exactly +0.0000** on provinces withheld. | [phase4_splits.md](reports/phase4_splits.md) |
| **Editorial label noise ≥0.96%**, measured by holding 80 characters of context byte-identical. | `results/editor_consistency.json` |

## Reproduce everything

```bash
./reproduce.sh --fast
```

Regenerates every number and figure from the committed corpus, skipping model
training and using the committed result cells. Drop `--fast` to retrain.
The script verifies the corpus sha256 before doing anything and refuses to run
against a different snapshot.

Outputs: `results/all_results.json` (every number in the paper) and `figures/`.

## Licensing — code and data are separate

- **Code** (`scripts/`, `src/`, `tests/`): MIT, see [LICENSE](LICENSE).
- **Derived data** (`data/derived/`): see [LICENSE-DATA.md](LICENSE-DATA.md).
  **Not released.** Redistribution permission from EDCS has not been granted;
  this blocks release. See [reports/edcs_permission_request.md](reports/edcs_permission_request.md).

## Documentation

- [reports/decisions.md](reports/decisions.md) — 41 logged decisions, each with
  evidence, rejected alternatives, and what would overturn it
- [reports/datasheet.md](reports/datasheet.md) — datasheet, including ten
  stated limitations
- [reports/related_work.md](reports/related_work.md) — verified citations, DOIs
  retrieved not recalled

---

## The underlying pipeline

EpigCorpus is a reproducible Latin epigraphy pipeline for extracting inscriptions from the Epigraphik-Datenbank Clauss / Slaby (EDCS), cleaning inscription text into research-ready variants, and exploring results on an interactive Roman Empire map.

The repository is designed as a practical, end-to-end workflow:
1. Scrape EDCS data incrementally with checkpoint resume.
2. Generate conservative and interpretive cleaned text.
3. Explore and export results through a Streamlit map interface.

## One-Command Launch

Run the full pipeline with a single command:

```bash
./epigcorpus.sh
```

This launcher handles environment checks and starts the project end-to-end.

## Author

- Sanoj Doddapaneni

## Software Citation

Doddapaneni, S. (n.d.). EpigCorpus: A reproducible Latin epigraphy pipeline for extracting, cleaning, and exploring EDCS inscriptions [Computer software]. GitHub.

## Highlights

- Incremental scraping against the current EDCS API with append-only updates.
- One-row-per-inscription data model.
- Structured exports in JSONL and TSV.
- Two cleaning outputs for analysis workflows:
  - Conservative Cleaned Inscriptions
  - Interpretive Cleaned Inscriptions
- Interactive dark-basemap map: one circle per place sized by its inscription
  count, a teal "known places" layer with tangerine search results drawn above
  it, and optional Roman provinces, roads and cities overlays.
- Search across raw and cleaned text.
- TSV download and publication-style PNG map export.

## Example PNG Export (term: viator)

![Example PNG Export for viator](img/Example%20export%20png%20image%20for%20term%20viator.png)

## Repository Layout

```text
EDCS-Analytics/
├── data/
│   ├── edcs_inscriptions.jsonl
│   ├── edcs_inscriptions.tsv
│   ├── edcs_lookup.json
│   ├── edcs_inscriptions_cleaned.jsonl
│   ├── map_layers/
│   │   ├── Hanson2016_Cities_OxREP.csv
│   │   ├── ba_roads/
│   │   └── roman_empire_ad_117/
│   └── README.md
├── src/
│   ├── edcs_scraper.py
│   ├── edcs_cleaner.py
│   └── edcs_streamlit_map.py
├── main.py
├── epigcorpus.sh
├── pyproject.toml
└── README.md
```

## Requirements

- Python 3.13+
- Dependencies managed in pyproject.toml

Core packages:
- geopandas
- matplotlib
- pandas
- requests
- streamlit

## Installation

Using uv (recommended):

```bash
uv sync
```

Using pip:

```bash
pip install geopandas matplotlib pandas requests streamlit
```

## Quick Start

One-command launcher:

```bash
./epigcorpus.sh
```

Recommended for first-time and daily use.

The launcher:
1. Uses local .venv if available.
2. Installs or uses uv when needed.
3. Ensures a compatible Python runtime.
4. Runs the full pipeline.

## Run Modes

Full pipeline:

```bash
python main.py
```

Skip scrape (use latest local JSONL):

```bash
python main.py --skip-scrape
```

Skip map launch:

```bash
python main.py --skip-map
```

Run components directly:

```bash
python src/edcs_scraper.py
streamlit run src/edcs_streamlit_map.py
```

## Data Products

Generated in data/:

- edcs_inscriptions.jsonl
  - canonical machine-readable inscription export
  - one JSON record per line
- edcs_inscriptions.tsv
  - tabular export for spreadsheets and SQL-style workflows
- edcs_lookup.json
  - lookup dictionary used to decode controlled vocabulary fields
- edcs_checkpoint.json
  - temporary resume checkpoint used during scraping
- edcs_inscriptions_cleaned.jsonl
  - cleaned dataset used by the Streamlit app and map search

For detailed data documentation and schema notes, see data/README.md.

## Pipeline Details

### 1) Scraping

Implemented in src/edcs_scraper.py:
- Uses EDCS API endpoint queries.
- Supports resume via checkpoint.
- Appends only unseen records in incremental mode.

### 2) Cleaning

Implemented in src/edcs_cleaner.py:
- Applies a staged text cleaning pipeline.
- Produces conservative and interpretive cleaned variants.

### 3) Interactive Exploration

Implemented in src/edcs_streamlit_map.py:
- Search modes: Raw inscriptions, Interpretive Cleaned Inscriptions, Conservative Cleaned Inscriptions.
- Interactive Leaflet map with layer toggles, hover tooltips and result popups.
- TSV export for result tables.
- Multi-select filters for material, category, language, province and place,
  combinable with the keyword search.
- PNG export in the map's own dark palette, always available: it carries the
  title, the search term, every active filter, the result count, search mode,
  data source and the attribution footer.

## Data and Software References

### Primary Data Sources

1. Clauss, M., Kolb, A., Slaby, W. A., and Woitas, B. Epigraphik-Datenbank Clauss / Slaby (EDCS). Universitat Zurich and Katholische Universitat Eichstatt-Ingolstadt. https://edcs.hist.uzh.ch/ (accessed 2026-06-03).
2. EDCS API endpoint used by this scraper: https://edcs.hist.uzh.ch/api/query (accessed 2026-06-03).
3. Hanson, J. W. (2016). Cities Database (OXREP Databases), Version 1.0. Oxford Roman Economy Project. DOI: https://doi.org/10.5287/bodleian:eqapevAn8. URL: http://oxrep.classics.ox.ac.uk/databases/cities/.

### Historical GIS Layers

1. Ancient World Mapping Center (AWMC). Geodata repository. https://github.com/AWMC/geodata (accessed 2026-06-03).
2. Province boundary layer used: roman_empire_ad_117.
3. Road network layer used: ba_roads.

### Upstream Method Reference

1. Ballsun-Stanton, B., Hermankova, P., and Laurence, R. (2024). LatEpig (Version 2.0) [Computer software]. GitHub. https://github.com/mqAncientHistory/Lat-Epig/. DOI: https://doi.org/10.5281/zenodo.12036539.

### Software Stack

1. Python. https://www.python.org/
2. pandas. https://pandas.pydata.org/
3. GeoPandas. https://geopandas.org/
4. Matplotlib. https://matplotlib.org/
5. Leaflet. https://leafletjs.com/
6. Requests. https://requests.readthedocs.io/
7. Streamlit. https://streamlit.io/

## Reproducibility Notes

- Scraping is incremental and resumable.
- Cleaning is deterministic for a given input dataset.
- Map search and PNG export use the same cleaned source data.
- Search mode naming is synchronized across UI, match summaries, and exports.

## License

MIT License. See LICENSE.

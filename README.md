# EpigCorpus

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

![Example PNG Export for viator](docs/img/Example%20export%20png%20image%20for%20term%20viator.png)

## Repository Layout

```text
EpigCorpus/
├── app/
│   ├── main.py            # pipeline entry: python -m app.main
│   ├── config/            # paths.py and settings.py: the only place paths are defined
│   ├── models/            # the inscription column schema
│   ├── core/              # pure helpers: cleaning rules, EDCS parsing, search
│   ├── adapters/          # EDCS HTTP client, harvest and corpus files, map layers
│   ├── services/          # use cases: scrape, cleaning, search, map data (+ ports.py)
│   ├── api/               # HTTP API contract (OpenAPI), not implemented yet
│   ├── ui/                # the Streamlit map (streamlit_app.py)
│   ├── cli/               # command-line parsing and launchers
│   ├── scripts/           # one-off research scripts (not part of the package)
│   ├── tests/
│   └── data/
│       ├── raw/           # edcs_inscriptions.jsonl.gz / .tsv.gz, harvest/ state
│       ├── processed/     # edcs_inscriptions_cleaned.jsonl.gz, derived/ datasets
│       ├── reference/     # edcs_lookup.json, map_layers/
│       ├── results/       # analysis outputs and model cells
│       └── reports/       # research reports
├── docs/                  # audits, ADRs, API notes, refactor records, img/
├── epigcorpus.sh
├── pyproject.toml
└── README.md
```

Dependencies point one way: the UI, CLI and (future) API call services;
services reach adapters only through the interfaces in
`app/services/ports.py`. See `docs/adr/0001-layered-app-layout.md`.
`app/tests/test_layering.py` enforces it.

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

Run from the repository root. Full pipeline:

```bash
python -m app.main
```

Skip scrape (use latest local JSONL):

```bash
python -m app.main --skip-scrape
```

Skip map launch:

```bash
python -m app.main --skip-map
```

Run components directly:

```bash
python -m app.cli.scrape            # same flags as before: --limit, --workers, --restart, --refresh-index
streamlit run app/ui/streamlit_app.py
```

`python main.py` and `python src/edcs_scraper.py` were replaced by these module
commands when the code moved under `app/`; the flags are unchanged. A plain
`data/edcs_inscriptions.jsonl` left from before the move is no longer read:
move it to `app/data/raw/`.

## Data Products

Generated under app/data/ (paths are defined in app/config/paths.py):

- raw/edcs_inscriptions.jsonl
  - canonical machine-readable inscription export
  - one JSON record per line
- raw/edcs_inscriptions.tsv
  - tabular export for spreadsheets and SQL-style workflows
- reference/edcs_lookup.json
  - lookup dictionary used to decode controlled vocabulary fields
- raw/harvest/edcs_checkpoint.json
  - temporary resume checkpoint used during scraping
- processed/edcs_inscriptions_cleaned.jsonl
  - cleaned dataset used by the Streamlit app and map search

For detailed data documentation and schema notes, see app/data/README.md.

## Pipeline Details

### 1) Scraping

Implemented in app/services/scrape.py (HTTP in app/adapters/edcs_client.py):
- Uses EDCS API endpoint queries.
- Supports resume via checkpoint.
- Appends only unseen records in incremental mode.

### 2) Cleaning

Implemented in app/core/cleaning.py (rules) and app/services/cleaning.py:
- Applies a staged text cleaning pipeline.
- Produces conservative and interpretive cleaned variants.

### 3) Interactive Exploration

Implemented in app/ui/streamlit_app.py, on app/services/search.py:
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

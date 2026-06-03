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

Sanoj Doddapaneni. (2026). EpigCorpus (Version 1.0) [Computer software]. GitHub. https://github.com/d-sanoj/EpigCorpus

## Highlights

- Incremental scraping against the current EDCS API with append-only updates.
- One-row-per-inscription data model.
- Structured exports in JSONL and TSV.
- Two cleaning outputs for analysis workflows:
  - Conservative Cleaned Inscriptions
  - Interpretive Cleaned Inscriptions
- Interactive map with Roman provinces, roads, cities, and inscription points.
- Search across raw and cleaned text.
- TSV download and publication-style PNG map export.

## Demo

### Streamlit Frontend (GIF)

![EpigCorpus Streamlit Frontend](img/App%20video.gif)

### Example PNG Export (term: viator)

![Example PNG Export for viator](img/Example%20export%20png%20image%20for%20term%20viator.png)

## Repository Layout

```text
EDCS-Analytics/
├── data/
│   ├── edcs_inscriptions.jsonl
│   ├── edcs_inscriptions.tsv
│   ├── edcs_lookup.json
│   ├── edcs_inscriptions_cleaned.jsonl
│   ├── lat_epig_support/
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
- folium
- requests
- streamlit

## Installation

Using uv (recommended):

```bash
uv sync
```

Using pip:

```bash
pip install geopandas matplotlib pandas folium requests streamlit
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
- Interactive map with layers and popups.
- TSV export for result tables.
- PNG export with attribution footer.

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
5. Folium. https://python-visualization.github.io/folium/
6. Requests. https://requests.readthedocs.io/
7. Streamlit. https://streamlit.io/

## Reproducibility Notes

- Scraping is incremental and resumable.
- Cleaning is deterministic for a given input dataset.
- Map search and PNG export use the same cleaned source data.
- Search mode naming is synchronized across UI, match summaries, and exports.

## License

MIT License. See LICENSE.

# EpigCorpus (EDCS-Analytics)

EpigCorpus is an updated, end-to-end Latin epigraphy workflow for extracting inscriptions from EDCS, cleaning epigraphic text into analysis-ready variants, and exploring results in an interactive Roman Empire map.

This repository implements a full local pipeline:
1. Scrape data from the current EDCS interface/API.
2. Clean inscriptions with conservative and interpretive text normalization.
3. Launch a Streamlit map for interactive exploration and export.

## Author

- Sanoj Doddapaneni

## Citation

If you use this software, cite:

Sanoj Doddapaneni. (2026). EpigCorpus (Version 1.0) [Computer software]. GitHub. https://github.com/d-sanoj/EpigCorpus

## What This Project Does

- Incremental EDCS scraping with checkpoint resume and append-only updates.
- Per-inscription record modeling (one row per inscription, not only per monument).
- Metadata harmonization (material/category lookup translation where available).
- Dual cleaned text outputs:
	- conservative cleaned inscriptions
	- interpretive cleaned inscriptions
- Interactive map interface with:
	- Roman provinces, roads, cities, inscription points
	- keyword search across raw and cleaned text
	- results table export (TSV)
	- publication-style PNG map export with citation footer

## Repository Structure

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
├── pyproject.toml
└── README.md
```

## Requirements

- Python 3.13+
- Dependencies (from pyproject.toml):
	- geopandas
	- matplotlib
	- pandas
	- folium
	- requests
	- streamlit

## Installation

Recommended (uv):

```bash
uv sync
```

Alternative (pip):

```bash
pip install geopandas matplotlib pandas folium requests streamlit
```

## Running EpigCorpus

Primary entry point (full pipeline):

```bash
python main.py
```

This runs:
1. Scraping (incremental/resumable)
2. Cleaning
3. Streamlit map launch (typically http://localhost:8501)

Optional flags:

```bash
python main.py --skip-scrape
python main.py --skip-map
```

You can also run modules directly if needed:

```bash
python src/edcs_scraper.py
streamlit run src/edcs_streamlit_map.py
```

## Data Outputs

### Scraper outputs

- data/edcs_inscriptions.jsonl
	- one JSON object per line
	- append-safe for incremental scraping
- data/edcs_inscriptions.tsv
	- tab-separated export of inscription rows
- data/edcs_lookup.json
	- lookup dictionaries used for code-to-label translation
- data/edcs_checkpoint.json
	- temporary resume state during scraping

### Cleaner output

- data/edcs_inscriptions_cleaned.jsonl
	- includes cleaned text variants used by the map and table search

## Cleaning Model

The cleaner is separated into src/edcs_cleaner.py and applies a staged text-normalization pipeline designed for epigraphic use.

Generated text views include:
- Raw inscriptions (original extracted text)
- Conservative Cleaned Inscriptions
- Interpretive Cleaned Inscriptions

These variants are exposed directly in the Streamlit search UI and in PNG export titles/metadata.

## Interactive Map and Export

The Streamlit app in src/edcs_streamlit_map.py provides:

- keyword search across raw and cleaned inscription text
- map visualization with provinces, roads, cities, and inscription points
- popup inspection for matching inscriptions
- TSV download of matched records
- PNG export of publication-style map with search metadata and project citation

## Data and Software Citations

This section provides citation-ready references for the datasets and software used in EpigCorpus.

### Primary Data Sources

1. Clauss, M., Kolb, A., Slaby, W. A., and Woitas, B. Epigraphik-Datenbank Clauss / Slaby (EDCS). Universitat Zurich and Katholische Universitat Eichstatt-Ingolstadt. Available at https://edcs.hist.uzh.ch/ (accessed 2026-06-02).
2. EDCS API endpoint used by the scraper: https://edcs.hist.uzh.ch/api/query (accessed 2026-06-02).
3. Hanson, J. W. (2016). Cities Database (OXREP Databases), Version 1.0. Oxford Roman Economy Project. DOI: https://doi.org/10.5287/bodleian:eqapevAn8. URL: http://oxrep.classics.ox.ac.uk/databases/cities/.

### Historical GIS / Basemap Layers

1. Ancient World Mapping Center (AWMC). Geodata repository. https://github.com/AWMC/geodata (accessed 2026-06-02).
2. Province boundary layer used here: roman_empire_ad_117 (AWMC distribution).
3. Road network layer used here: ba_roads (AWMC distribution).
4. As documented in Lat-Epig and AWMC notes, these layers are part of the Barrington Atlas historical GIS ecosystem, with AWMC-distributed derivatives and OpenStreetMap-related licensing context where applicable.

### Upstream Method/Reference Project

1. Ballsun-Stanton, B., Hermankova, P., and Laurence, R. (2024). LatEpig (Version 2.0) [Computer software]. GitHub. https://github.com/mqAncientHistory/Lat-Epig/. DOI: https://doi.org/10.5281/zenodo.12036539.

## Reproducibility Notes

- Scraping is incremental and resumable.
- Cleaned output is regenerated from current local scraped data.
- Map and PNG export are generated from the same cleaned dataset used in interactive search.
- Search labels and output metadata are synchronized across UI and exported figures.

## License

MIT License.

See LICENSE for details.

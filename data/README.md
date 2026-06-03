# Data Directory Documentation

This directory contains raw and cleaned inscription exports, lookup dictionaries, and map-support reference datasets used by EpigCorpus.

## Contents

### Core inscription exports

- edcs_inscriptions.jsonl
  - canonical scraper output (JSON Lines)
  - one inscription record per line
  - append-safe for incremental updates

- edcs_inscriptions.tsv
  - tabular export of the same records
  - suitable for spreadsheets and relational imports

- edcs_inscriptions_cleaned.jsonl
  - cleaned dataset generated from raw exports
  - includes conservative and interpretive cleaned text columns
  - used by the Streamlit search UI and map exports

### Operational and lookup files

- edcs_lookup.json
  - controlled-vocabulary lookup data returned by EDCS
  - includes mappings for material and inscription categories

- edcs_checkpoint.json
  - temporary scrape resume marker
  - created during in-progress runs
  - removed after successful completion

### Map support datasets

- lat_epig_support/Hanson2016_Cities_OxREP.csv
  - Roman city reference dataset used for map plotting

- lat_epig_support/ba_roads/
  - Roman roads shapefile components

- lat_epig_support/roman_empire_ad_117/
  - Roman empire/province boundary shapefile components

## Data Generation Workflow

From project root:

```bash
python main.py
```

Or run individual stages:

```bash
python src/edcs_scraper.py
python main.py --skip-scrape
```

Workflow summary:
1. Scraper pulls records from EDCS API.
2. New records are appended to JSONL/TSV exports.
3. Cleaning stage produces edcs_inscriptions_cleaned.jsonl.
4. Streamlit app reads the cleaned file for map/search/export.

## Format Notes

### JSONL files

- UTF-8 encoded text
- one JSON object per line
- read with pandas:

```python
import pandas as pd
df = pd.read_json("data/edcs_inscriptions.jsonl", lines=True)
```

### TSV file

- tab-delimited text
- consistent fixed schema exported by scraper
- flattened list-style fields for tabular compatibility

## Common Fields

The exports typically include fields such as:
- record_id
- edcs_id
- inscription_index
- province
- place
- latitude
- longitude
- material
- material_en
- not_before
- not_after
- inscription_text
- language
- category
- category_en
- image_urls

The cleaned dataset additionally includes cleaned inscription variants used by search and export.

## Data Provenance and Attribution

- Inscriptions: Epigraphik-Datenbank Clauss / Slaby (EDCS)
  - https://edcs.hist.uzh.ch/
- Cities: Hanson (2016) OXREP Cities Database
  - http://oxrep.classics.ox.ac.uk/databases/cities/
  - DOI: https://doi.org/10.5287/bodleian:eqapevAn8
- Provinces and roads: Ancient World Mapping Center (AWMC) geodata distributions
  - https://github.com/AWMC/geodata

## Practical Notes

- Treat edcs_checkpoint.json as ephemeral state.
- Prefer edcs_inscriptions_cleaned.jsonl for analysis and app search.
- Keep heavy geospatial support files in lat_epig_support for consistent local map rendering.

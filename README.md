# EpigCorpus

EpigCorpus is a reproducible Latin epigraphy pipeline for extracting inscriptions from the Epigraphik-Datenbank Clauss / Slaby (EDCS), cleaning inscription text into research-ready variants, and exploring results on an interactive Roman Empire map.

The repository is designed as a practical, end-to-end workflow:
1. Scrape EDCS data incrementally with checkpoint resume.
2. Generate conservative and interpretive cleaned text.
3. Explore and export results through an HTTP API and the web map built on it.

## Run it

The map's background is CARTO's dark basemap, and CARTO's tiles need a free
API key: get one at https://carto.com/basemaps/apikey (no account needed).
Then:

```bash
cp .env.example .env             # and put the key in it: EPIGCORPUS_CARTO_KEY=...
```

With Docker (nothing else to install):

```bash
docker compose up --build        # http://localhost:8000
```

Or locally, with [uv](https://docs.astral.sh/uv/) and Node 22:

```bash
cd frontend && npm ci && npm run build && cd ..          # the web UI, once
cd backend && uv sync
export EPIGCORPUS_CARTO_KEY=...                         # your CARTO key
uv run uvicorn epigcorpus.api.server:app --port 8000     # http://127.0.0.1:8000
```

The server answers from the cleaned corpus committed in `data/`. The first
start converts it to a Parquet snapshot, which takes about a minute. After
that it starts in a few seconds.

To harvest from the EDCS, clean, and load the new corpus, use the
**Pipeline** panel in the web UI, or the API:

```bash
curl -X POST localhost:8000/api/v1/pipeline-runs -H 'content-type: application/json' -d '{}'
```

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
- Interactive dark map on CARTO's label-free dark basemap, set up as on the
  original Streamlit map. When zoomed out, known places are grouped into a
  density grid; zoomed in, there is one solid circle per place, sized by its
  inscription count. Stacked bottom to top: land, Roman provinces with their names (the
  base, on by default), teal known places, roads, yellow cities, and orange
  search results on top. No place names on the background.
- Search across raw and cleaned text.
- TSV download and publication-style PNG map export.
- A versioned HTTP API (`/api/v1`, OpenAPI 3.1) for search, records, cleaned
  variants, places (also as GeoJSON), downloads, lookups and pipeline runs.

## Example PNG Export (term: viator)

![Example PNG Export for viator](docs/img/Example%20export%20png%20image%20for%20term%20viator.png)

## Repository Layout

```text
EpigCorpus/
├── backend/               # the epigcorpus Python package (uv project)
│   ├── src/epigcorpus/
│   │   ├── config/        # paths (overridable by environment) and settings
│   │   ├── models/        # the inscription column schema
│   │   ├── core/          # pure rules: cleaning, EDCS parsing, search semantics
│   │   ├── adapters/      # EDCS HTTP client, corpus and harvest files, map layers
│   │   ├── services/      # use cases: harvest, clean, catalog, pipeline runs (+ ports.py)
│   │   └── api/           # FastAPI under /api/v1 and its contract, openapi.yaml
│   └── tests/
├── frontend/              # React + TypeScript + Vite + Tailwind + Leaflet
│   ├── src/api/           # typed client; schema.d.ts is generated from openapi.yaml
│   ├── src/features/      # search (sidebar), map, results (table), pipeline
│   └── e2e/               # Playwright tests against the running app
├── data/                  # raw/, processed/, reference/ (see data/README.md)
├── research/              # the abbreviation study: scripts/, reports/, results/
├── docs/                  # ADRs, API and data notes, refactor records
├── Dockerfile, docker-compose.yml
└── .github/workflows/ci.yml
```

Dependencies point one way. The web UI calls the API, and the API calls
services. Services reach adapters only through the interfaces in
`services/ports.py`; `backend/tests/test_layering.py` enforces this. See
`docs/adr/0001-layered-app-layout.md` and
`docs/adr/0005-backend-frontend-split.md`.

## Development

```bash
# backend
cd backend
uv run pytest                        # unit, contract and layering tests
uv run ruff check src tests

# frontend: Vite on :5173, forwarding /api to the backend on :8000
cd frontend
npm run dev
npm test                             # vitest
npm run typecheck
npm run gen:api                      # regenerate API types after editing openapi.yaml
npm run e2e                          # Playwright, against a server on :8000 (E2E_BASE_URL)
```

Research scripts import the package:
`uv run --project backend python research/scripts/<name>.py`.

## Data Products

Under `data/` (paths are defined in `backend/src/epigcorpus/config/paths.py`):

- `raw/edcs_inscriptions.jsonl[.gz]`, `raw/edcs_inscriptions.tsv[.gz]`: the
  harvested corpus, one inscription per row.
- `raw/harvest/`: the harvest's checkpoint, manifest and failure list.
- `reference/edcs_lookup.json`: the EDCS code tables; `reference/map_layers/`:
  the vendored map layers.
- `processed/edcs_inscriptions_cleaned.jsonl[.gz]`: the cleaned corpus, which
  the API serves. Its Parquet snapshot
  (`processed/edcs_inscriptions_cleaned.parquet`) is rebuilt automatically
  and not committed.

See data/README.md for schemas.

## Pipeline Details

A pipeline run (`POST /api/v1/pipeline-runs`) has three stages:

1. **Harvest**: `services/scrape.py`, over `adapters/edcs_client.py`.
   It is incremental and resumable from a checkpoint. A cancelled run resumes
   where it stopped.
2. **Clean**: `core/cleaning.py` holds the rules and `services/cleaning.py`
   applies them. Each inscription gets a conservative and an interpretive
   variant.
3. **Reload**: the running server loads the new cleaned corpus and swaps it
   in without a restart.

The web UI is a client of the API:

- Search modes: Raw inscriptions, Interpretive Cleaned Inscriptions,
  Conservative Cleaned Inscriptions.
- Filters for language, province, place, category and material, combinable
  with the keyword.
- A Leaflet map with layer toggles, tooltips and result popups.
- A results table, a page at a time.
- TSV export of the results, and a PNG export of the map.
- The search is kept in the address bar, so it can be shared as a link.

## Data and Software References

### Primary Data Sources

1. Clauss, M., Kolb, A., Slaby, W. A., and Woitas, B. Epigraphik-Datenbank Clauss / Slaby (EDCS). Universitat Zurich and Katholische Universitat Eichstatt-Ingolstadt. https://edcs.hist.uzh.ch/ (accessed 2026-06-03).
2. EDCS static data endpoints used by the harvest: https://edcs.hist.uzh.ch/data/ (see docs/EDCS_API.md).
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
7. FastAPI. https://fastapi.tiangolo.com/
8. React. https://react.dev/
9. Vite. https://vite.dev/
10. Tailwind CSS. https://tailwindcss.com/

## Reproducibility Notes

- Scraping is incremental and resumable.
- Cleaning is deterministic for a given input dataset.
- Map search and PNG export use the same cleaned source data.
- Search mode naming is synchronized across UI, match summaries, and exports.

## License

MIT License. See LICENSE.

# ADR 0005: A backend package and a React frontend; the pipeline runs through the API

- **Status:** Accepted
- **Date:** 2026-10-24
- **Supersedes:** ADR 0004, point 2 (the plain-JS UI with no build step), and
  the command line kept by ADR 0001
- **Relates to:** ADR 0001 (layered layout), ADR 0002 (API-first design)

## Context

The maintainer asked for these changes:

- a `backend/` (FastAPI) and `frontend/` (React, TypeScript, Vite, Tailwind,
  Leaflet) split;
- a sidebar / map / table layout;
- endpoints to run the pipeline, list inscriptions, get map GeoJSON and
  download results;
- no command line: "I will only run with API";
- a working service.

Measured before deciding, on the full corpus (588,509 rows):

- **Server start.** Startup read the cleaned JSONL twice, which took about
  70 s and peaked at about 8 GB.
- **Queries.** A keyword search over the 568,560-row search frame took about
  90 ms in pandas, and a filter took about 15 ms.

## Decision

1. **Layout.** The package is `backend/src/epigcorpus`, installed by uv, with
   no `sys.path` changes. The UI is `frontend/`. The data is `data/` at the
   repository root, and the research scripts, reports and results are in
   `research/`. All paths come from `epigcorpus.config.paths`; the
   `EPIGCORPUS_DATA_DIR`, `EPIGCORPUS_RESEARCH_DIR` and
   `EPIGCORPUS_STATIC_DIR` environment variables override them.
2. **Parquet, not DuckDB.** The recommendation that preceded this ADR
   proposed DuckDB over Parquet. The measurements above showed that the cost
   was loading, not querying. So the cleaned corpus gets a Parquet snapshot
   instead:
   - It is written next to the JSONL and tagged with the JSONL's name, size
     and modification time, and rebuilt whenever the JSONL changes.
   - It reads back identical to the JSONL: `assert_frame_equal` with exact
     values on the full corpus, with list columns restored as lists.
   - The search frame is cut from the loaded corpus rather than read again.
   - A warm start now takes about 5 s and 1.6 GB.
   - The search code stays on the pandas core, whose results the earlier
     baselines verified, and DuckDB is not a dependency.
3. **The API is under `/api/v1`.** Under `/v1` it would share an origin with
   the UI's client-side routes. The requested endpoints map onto the contract
   as follows:

   | Requested | In the contract |
   |---|---|
   | `POST /api/pipeline/run` | `POST /api/v1/pipeline-runs`, plus `GET` to poll and list and `POST …/cancel` |
   | `GET /api/inscriptions` | `GET /api/v1/inscriptions` and `/inscriptions/search` |
   | `GET /api/map/geojson` | `GET /api/v1/places.geojson` (a query's places) and `/places/known.geojson` |
   | `GET /api/inscriptions/download` | `GET /api/v1/exports/search.tsv` |

4. **The pipeline runs in the server.** A run has three stages: harvest,
   clean, reload.
   - The reload builds and warms a new catalog while the old one answers,
     then swaps it in (`CatalogSlot`). Each request takes the catalog once, so
     it never mixes two corpora. Cursors are bound to the corpus fingerprint,
     so a cursor from before the swap is a `400`.
   - The command line and `epigcorpus.sh` are deleted.
   - Trade-off: a harvest takes hours and now lives in the server process.
     If the server stops, the next run resumes from the checkpoint, as Ctrl+C
     did before. There is one run at a time and the registry is in memory.
     ADR 0003's authentication should protect `POST /pipeline-runs` before
     the server is exposed beyond localhost.
5. **The React UI keeps the map imperative.** The Leaflet map is a TypeScript
   class (`features/map/epigMap.ts`), ported line by line: same layers,
   colours, sizes, shared-canvas stacking, province labels and decluttering.
   The layer stacking depends on draw order within one canvas, which a
   declarative wrapper such as react-leaflet would fight. React owns
   everything around it: the sidebar, the layer toggles, the legend, the
   table and the pipeline panel.
6. **Types are generated from the contract.** `openapi-typescript` writes
   `frontend/src/api/schema.d.ts`. CI regenerates it and fails on any
   difference, so a contract change reaches the UI's type check.
7. **Deployment.**
   - One image: Node builds the UI, then a Python image serves it and the API.
   - `docker-compose.yml` mounts `data/`.
   - In development, Vite proxies `/api` to uvicorn.
   - CI runs these jobs: backend lint and tests; frontend type check, unit
     tests and build; Playwright end-to-end against the real server and
     committed corpus; and a Docker build.

## Consequences

- One command starts everything (`docker compose up --build`). Locally it is
  two steps: build the UI once, then start uvicorn.
- Leaflet is bundled from npm, so the UI no longer needs unpkg.
- The UI build needs Node 22. The runtime does not.
- Cleaning the full corpus inside the server needs about 8 GB of memory in
  total.

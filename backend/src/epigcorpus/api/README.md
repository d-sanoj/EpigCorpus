# EpigCorpus HTTP API

`openapi.yaml` (OpenAPI 3.1) is the contract. It was written before the code
(`docs/adr/0002-api-first-design.md`). The code is tested against it, and the
server returns it at `/api/v1/openapi.yaml`. FastAPI's own generated schema is
switched off, so there is one description of the API, not two. The frontend
generates its TypeScript types from this file (`npm run gen:api` in
`frontend/`).

```bash
cd backend
uv run uvicorn epigcorpus.api.server:app --port 8000
curl -s 'http://127.0.0.1:8000/api/v1/places?q=viator&limit=1' | python -m json.tool
```

The server loads the corpus in a background thread at startup. That takes
about 5 seconds from the Parquet snapshot in `data/processed/`. The first
start, or the first after the cleaned corpus changes, rebuilds the snapshot
from the JSONL and takes about a minute. `/api/v1/health` reports `ready`, and
requests that need the corpus wait for it.

When `frontend/dist` exists (after `npm run build`), the server also serves
the web UI at `/`. Unknown non-API paths fall back to `index.html` for the
client-side router. Unknown `/api` paths stay `404` problems.

## Layout

```
backend/src/epigcorpus/api/
├── openapi.yaml       the contract
├── server.py          composition root: real adapters → services → app
├── app.py             create_app(deps): routes under /api/v1, problem handlers, the UI at /
├── spa.py             static files with an index.html fallback
├── deps.py            ApiDeps (catalogs, exports, runs), the per-request catalog, query parameters
├── problems.py        RFC 9457 problem+json for every error, FastAPI's own included
├── pagination.py      opaque cursors bound to the query and the corpus snapshot
├── serialize.py       pandas rows → the contract's JSON (null, int years, ISO times, GeoJSON)
└── routes/            one module per resource: meta, inscriptions, places, map,
                       exports, lookups, pipeline_runs
```

`epigcorpus.api` imports only `config`, `models` and `services`, plus FastAPI.
`server.py` is the one module that also imports adapters, and
`backend/tests/test_layering.py` enforces both rules.

## Operation → service

All paths start with `/api/v1`.

| Operation | Service call |
|---|---|
| `GET /health` | `Catalog.is_loaded()` |
| `GET /inscriptions` | `Catalog.rows("", "Raw inscriptions", selections)`, i.e. `search.apply_filters` over the search frame |
| `GET /inscriptions/search` | `Catalog.rows(q, mode, selections)` |
| `GET /inscriptions/{record_id}` | `Catalog.inscription(record_id)`, from the full cleaned corpus indexed by `record_id` |
| `GET /inscriptions/{record_id}/variants[/{variant}]` | Same row: `inscription_text`, `inscription_text_conservative`, `inscription_text_interpretive`, `is_unreadable`, `is_forged` |
| `GET /places` | `Catalog.run_query(q, mode, selections)` → places, example items, `describe_query` title and subtitle |
| `GET /places.geojson` | Same as `/places`, all of it as one GeoJSON FeatureCollection (what the map draws) |
| `GET /places/known` | `Catalog.known_places()` → `search.load_known_places` |
| `GET /places/known.geojson` | Same as `/places/known`, as GeoJSON |
| `GET /map/layers/{provinces,roads,cities}` | `Catalog.provinces_geojson()`, `roads_geojson()`, `cities_points()` → `services.map_data`. Serialised once per corpus snapshot, with an ETag |
| `GET /map/layers/land` | `Catalog.land_geojson()`: Natural Earth land, the map's background |
| `GET /map/layers/province-labels` | `Catalog.province_labels()`: each EDCS province at the median of its places, coloured like the shading beneath |
| `GET /exports/{corpus.jsonl,corpus.tsv,cleaned.jsonl}` | `corpus_files.export_file(name)`. The stored `.gz` is sent as is when the client accepts gzip, otherwise decompressed on the fly |
| `GET /exports/search.tsv` | `Catalog.rows` → `Catalog.results_table` → `search.results_tsv` |
| `GET /exports/map.png` | `Catalog.export_png` → `services.map_png.build_png_bytes` |
| `GET /lookups`, `/lookups/{table}` | `Catalog.lookup()` → `corpus_files.read_lookup()` |
| `GET /facets` | `Catalog.filter_options()` and `Catalog.short_forms()` |
| `POST /pipeline-runs` | `PipelineRuns.start` → a thread running harvest (`run_harvest`), clean (`run_cleaning`), reload (`CatalogSlot.reload`) |
| `GET /pipeline-runs`, `/pipeline-runs/{run_id}` | `PipelineRuns.list`, `PipelineRuns.get` |
| `POST /pipeline-runs/{run_id}/cancel` | `PipelineRuns.cancel` → the harvest's `should_stop` hook; later stages are skipped |

`search_in` maps to the services' mode labels (`raw` → "Raw inscriptions",
`interpretive` → "Interpretive Cleaned Inscriptions", `conservative` →
"Conservative Cleaned Inscriptions"). The filters map onto corpus columns:
`material` → `material_en`, `category` → `category_en`; `language`,
`province` and `place` keep their names.

`Catalog` (`services/catalog.py`) is the process-wide read model. It loads the
cleaned corpus once and cuts the search frame from it. It also loads filter
options, short forms, known places and the map layers once, and keeps the last
64 queries and 16 PNGs. `CatalogSlot` holds the current catalog. A pipeline
run's reload builds and warms a new catalog while the old one keeps
answering, then swaps it in. Each request takes the catalog once, when it
starts (`deps.get_catalog`), so a request never mixes two corpora.

## The pipeline

The command line is gone. The pipeline runs only through the API:

```bash
# harvest (resumable), clean, reload: the old `scrape → clean`, plus the reload
curl -si -X POST localhost:8000/api/v1/pipeline-runs -H 'content-type: application/json' -d '{}'
# re-clean what is on disk and reload, no network
curl -si -X POST localhost:8000/api/v1/pipeline-runs -H 'content-type: application/json' -d '{"harvest": false}'
curl -s localhost:8000/api/v1/pipeline-runs/<id>        # poll: status, stages, harvest progress
curl -si -X POST localhost:8000/api/v1/pipeline-runs/<id>/cancel
```

A full harvest takes hours and runs inside the server process. If the server
stops mid-harvest, the next run resumes from the checkpoint. Cleaning the
full corpus runs in a child process, chunk by chunk, and a reload drops the old
catalog before loading the new one. A full clean-and-reload run peaks at about
3 GB, checked in a container limited to 4 GB. If the machine still runs out,
only the child dies, and the run fails with a message that says so.

## How the open questions from the design were settled

| Question | What the implementation does |
|---|---|
| Cancelling a harvest | `harvest()` takes an optional `should_stop` hook, polled between chunks. It stops through the same flush-and-checkpoint path as Ctrl+C did, so a cancelled harvest resumes. |
| Harvest progress | An optional `on_progress` hook, called after every chunk, shown on the run as `progress`. |
| Failure without `sys.exit` | `run_harvest` raises `HarvestError`; the run fails with a `harvest-unavailable` problem. |
| Outcome | `run_harvest` returns `complete`, `partial`, `incomplete`, `interrupted` or `already_complete`, shown as the harvest stage's `result`. |
| Refreshing what is served | A run's `reload` stage swaps the new corpus in. No restart is needed. |
| The bounding box | **Kept.** Search, list and places use the map's search frame, 568,560 of 588,509 rows; rows without coordinates are left out. Reading one record by id uses the full corpus. |
| `retrieved_at` as epoch milliseconds | The pipeline now writes it as ISO 8601 (`2026-08-19T16:32:11Z`), as the scraper does. The API answers ISO 8601 for both forms, since a corpus cleaned earlier still holds epoch milliseconds. |
| Search TSV per monument | **Kept**, and documented. |
| Cursor stability | Cursors carry an offset and a fingerprint of the query and the corpus snapshot. A stale cursor, including one from before a reload, is a `400`. |
| Authentication | Declared, **not enforced**. See `docs/adr/0003-api-authentication.md`. Starting a pipeline run is the first thing to protect before exposing the server. |

## Known limits

- **One process.** The run registry lives in memory, and a restart forgets
  finished runs. Run one server process, not several workers.
- **Province names are placed from the corpus, not drawn from the province
  layer.** The AWMC polygons carry no names and don't map one-to-one onto
  EDCS provinces; one shape covers Syria, Galatia and Pontus. Each name sits
  at the median of its province's places.

## Tests

`backend/tests/api/test_contract.py` runs against a real cleaned corpus built
offline from the 50 EDCS monument fixtures. It checks:

- the routed operations equal the contract's, and every response validates
  against its declared schema;
- search, places and GeoJSON match the services exactly, and pages join up;
- problem types, gzip negotiation, and the TSV and PNG downloads;
- the pipeline lifecycle: stages, conflict, progress, cancelling, failure,
  and the catalog swap;
- the single-page-app fallback.

`test_corpus_snapshot.py` checks that the Parquet snapshot reads back
identical to the JSONL.

```bash
cd backend && uv run pytest
```

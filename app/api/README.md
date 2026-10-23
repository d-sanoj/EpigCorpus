# EpigCorpus HTTP API

`openapi.yaml` (OpenAPI 3.1) is the contract. It was written before the code
(`docs/adr/0002-api-first-design.md`). The code is tested against it, and the
server returns it at `/v1/openapi.yaml`. FastAPI's own generated schema is
switched off, so there is one description of the API, not two.

```bash
python -m app.cli.serve                 # http://127.0.0.1:8000/  (--host, --port)
curl -s 'http://127.0.0.1:8000/v1/places?q=viator&limit=1' | python -m json.tool
```

The server loads the corpus in a background thread at startup, which takes
about 20 seconds for the full snapshot. `/v1/health` reports `ready`, and
requests that need the corpus wait for it. The web map at `/`
(`app/ui/static`) is a client of these routes, and nothing else.

## Layout

```
app/api/
├── openapi.yaml       the contract
├── server.py          composition root: real adapters → services → app
├── app.py             create_app(deps): routes, problem handlers, the static UI at /
├── deps.py            ApiDeps (catalog, exports, jobs), shared query parameters
├── problems.py        RFC 9457 problem+json for every error, FastAPI's own included
├── pagination.py      opaque cursors bound to the query and the corpus snapshot
├── serialize.py       pandas rows → the contract's JSON (null, int years, ISO times)
└── routes/            one module per resource: meta, inscriptions, places, map,
                       exports, lookups, jobs
```

`app/api` imports only `config`, `models` and `services`, plus FastAPI.
`server.py` is the one module that also imports adapters, and
`app/tests/test_layering.py` enforces both rules.

## Operation → service

| Operation | Service call |
|---|---|
| `GET /v1/health` | `Catalog.is_loaded()` |
| `GET /v1/inscriptions` | `Catalog.rows("", "Raw inscriptions", selections)`, i.e. `search.apply_filters` over the search frame |
| `GET /v1/inscriptions/search` | `Catalog.rows(q, mode, selections)` |
| `GET /v1/inscriptions/{record_id}` | `Catalog.inscription(record_id)`, from the full cleaned corpus indexed by `record_id` |
| `GET /v1/inscriptions/{record_id}/variants[/{variant}]` | same row: `inscription_text`, `inscription_text_conservative`, `inscription_text_interpretive`, `is_unreadable`, `is_forged` |
| `GET /v1/places` | `Catalog.run_query(q, mode, selections)` → places, example items, `describe_query` title and subtitle |
| `GET /v1/places/known` | `Catalog.known_places()` → `search.load_known_places` |
| `GET /v1/map/layers/{provinces,roads,cities}` | `Catalog.provinces_geojson()`, `roads_geojson()`, `cities_points()` → `services.map_data`. Serialised once, with an ETag |
| `GET /v1/exports/{corpus.jsonl,corpus.tsv,cleaned.jsonl}` | `corpus_files.export_file(name)`. The stored `.gz` is sent as is when the client accepts gzip, otherwise decompressed on the fly |
| `GET /v1/exports/search.tsv` | `Catalog.rows` → `Catalog.results_table` → `search.results_tsv` (the old map's "Download TSV") |
| `GET /v1/exports/map.png` | `Catalog.export_png` → `services.map_png.build_png_bytes` (the old map's "Download as PNG", byte-identical) |
| `GET /v1/lookups`, `/v1/lookups/{table}` | `Catalog.lookup()` → `corpus_files.read_lookup()` |
| `GET /v1/facets` | `Catalog.filter_options()` and `Catalog.short_forms()` |
| `POST /v1/scrape-jobs` | `ScrapeJobs.start` → `services.scrape.run_harvest` in a thread |
| `GET /v1/scrape-jobs/{job_id}` | `ScrapeJobs.get` |
| `POST /v1/scrape-jobs/{job_id}/cancel` | `ScrapeJobs.cancel` → the harvest's `should_stop` hook |

`search_in` maps to the services' mode labels (`raw` → "Raw inscriptions",
`interpretive` → "Interpretive Cleaned Inscriptions", `conservative` →
"Conservative Cleaned Inscriptions"). The filters map onto corpus columns:
`material` → `material_en`, `category` → `category_en`; `language`,
`province` and `place` keep their names.

`Catalog` (`app/services/catalog.py`) is the process-wide read model that
replaced the Streamlit app's `st.cache_data`. It loads the search frame, the
full corpus, filter options, short forms, known places and the map layers
once, and keeps the last 64 queries and 16 PNGs.

## How the open questions from the design were settled

| Question (design-stage README) | What the implementation does |
|---|---|
| Cancelling a scrape job | `harvest()` gained an optional `should_stop` hook, polled between chunks. It stops through the same flush-checkpoint path as Ctrl+C, so a cancelled harvest resumes. |
| Scrape-job progress | An optional `on_progress` hook, called after every chunk. |
| Failure without `sys.exit` | `run_harvest` raises `HarvestError`. The CLI catches it and still exits 1, after the same log lines (checked against the original). |
| Outcome | `run_harvest` returns `complete`, `partial`, `incomplete`, `interrupted` or `already_complete`. |
| The bounding box | **Kept as documented.** Search, list and places use the map's search frame, 568,560 of 588,509 rows: rows without coordinates are left out. Reading one record by id uses the full corpus. Still yours to revisit. |
| `retrieved_at` as epoch milliseconds | **Converted in the API** to ISO 8601. The cleaned corpus on disk is unchanged. |
| Search TSV per monument | **Kept** (927 rows for 515 `viator` matches) and documented. |
| Cursor stability | Cursors carry an offset and a fingerprint of the query and the corpus snapshot (`fingerprint()`: name, size and mtime of the cleaned file). A stale cursor is a `400`. |
| Authentication | Declared, **not enforced**. See `docs/adr/0003-api-authentication.md`. |

## Additions made while implementing

The contract was extended where the web map needed something the
design-stage draft lacked. Each addition is in `openapi.yaml`:

- `GET /v1/health`, so the page can tell "loading" from "broken".
- `GET /v1/exports/map.png`, the PNG export.
- `summary` on both place pages (match and place counts; the query in words).
- `limit` up to 25,000 on the place pages, so the map takes every place in one
  request.
- GeoJSON features may have a `null` geometry; one simplified road does.

## Known limits

- **One process.** The scrape-job registry lives in memory. A restart forgets
  finished jobs, though a harvest that was running resumes from its
  checkpoint at the next start. Run one server process, not several workers.
- **A harvest does not refresh what is served.** A scrape job writes the raw
  corpus. Cleaning (`python -m app.main --skip-scrape --skip-map`) and a
  server restart are still needed before search sees new data, as before.
- **The map needs the internet** for Leaflet (unpkg) and the CARTO basemap
  tiles, as the Streamlit map did. Search, results and downloads work without
  them.

## Tests

`app/tests/api/test_contract.py` runs against a real cleaned corpus built
offline from the 50 EDCS monument fixtures. It checks:

- the routed operations equal the contract's;
- every response validates against its declared schema;
- search and places match the services exactly, and pages join up;
- problem types, gzip negotiation, and the TSV and PNG bytes;
- the scrape-job lifecycle (start, conflict, progress, cancel, finished).

```bash
uv run pytest app/tests/api
```

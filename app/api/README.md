# EpigCorpus HTTP API: contract

`openapi.yaml` (OpenAPI 3.1) is the contract. **No endpoint is implemented**,
and no web framework is installed. The contract came first on purpose (see
`docs/adr/0002-api-first-design.md`). This file maps the contract onto the
services that already exist, and lists the gaps an implementation will have
to close.

## Where the API sits

```
HTTP request ─▶ app/api (routing, validation, pagination, problem+json)
                    │  calls only services (enforced by app/tests/test_layering.py)
                    ▼
              app/services ──▶ app/core, app/models
                    │ ports.py
                    ▼
              app/adapters (files, EDCS HTTP, map layers), bound by the API's
              composition root, as app/main.py, app/cli/scrape.py and
              app/ui/streamlit_app.py do today
```

The API layer may import `config`, `models` and `services`. Like the UI, its
entry module will be a composition root that binds `app.adapters.corpus_files`,
`map_layers`, `edcs_client` and `harvest_store` into the services. The
search frame is loaded once per process and reused, as the UI's
`st.cache_data` does today.

## Operation → service

| Operation | Service call | Notes |
|---|---|---|
| `GET /v1/inscriptions` | `services.search.run_query("", mode, selections, load_all)` → rows | Filters only. `load_all` = `load_inscriptions(corpus_files)` bound and memoised. |
| `GET /v1/inscriptions/search` | `services.search.run_query(q, mode, selections, load_all)` → rows | `search_in` → mode label via `SEARCH_COLUMNS` (`raw` → "Raw inscriptions", `interpretive` → "Interpretive Cleaned Inscriptions", `conservative` → "Conservative Cleaned Inscriptions"). |
| `GET /v1/inscriptions/{record_id}` | `services.search.load_full_cleaned_data(corpus_files)` → row by `record_id` | Full corpus, not the bounded frame. Needs an index by `record_id` for speed (new code, no new behaviour). |
| `GET /v1/inscriptions/{record_id}/variants[/{variant}]` | same row; columns `inscription_text`, `inscription_text_conservative`, `inscription_text_interpretive`, `is_unreadable`, `is_forged` | Produced by `services.cleaning.clean_data` / `core.cleaning`. Nothing is re-cleaned per request. |
| `GET /v1/places` | `services.search.run_query(...)` → places (`aggregate_places` + example items) | Response field `examples` = the service's `items`. |
| `GET /v1/places/known` | `services.search.load_known_places(load_all, corpus_files)` | |
| `GET /v1/map/layers/provinces` | `services.map_data.provinces_geojson(map_layers.load_provinces)` | Static; ETag = hash of the vendored files. |
| `GET /v1/map/layers/roads` | `services.map_data.roads_geojson(map_layers.load_roads)` | |
| `GET /v1/map/layers/cities` | `services.map_data.cities_points(map_layers.load_cities)` | `[lat, lon, label]` triples become `{latitude, longitude, label}`. |
| `GET /v1/exports/corpus.jsonl`, `corpus.tsv` | stream the scraper's output: `config.paths.RAW_JSONL` / `RAW_TSV`, or the committed `.gz` | Needs a small export service that resolves the plain-or-gz file the way `adapters.corpus_files.cleaned_corpus_path` does. |
| `GET /v1/exports/cleaned.jsonl` | `adapters.corpus_files.cleaned_corpus_path()` through the same export service | |
| `GET /v1/exports/search.tsv` | `run_query` → `services.search.matched_results(rows, full)` → `services.search.results_tsv(table)` | Same bytes as the map's "Download TSV". |
| `GET /v1/lookups`, `/v1/lookups/{table}` | `adapters.corpus_files.read_lookup()` through a lookup service | `None` (no harvest yet) → `503`. |
| `GET /v1/facets` | `services.search.filter_options(load_all)` and `services.search.short_forms(corpus_files)` | API names drop the `_en` suffix: `material`, `category`. |
| `POST /v1/scrape-jobs` | `services.scrape.run_harvest(limit, workers, restart, refresh_index, client=edcs_client, store=harvest_store, log=…)` in a background worker | One job at a time → `409`. |
| `GET /v1/scrape-jobs/{id}` | the job registry (new) | Progress needs a hook, see gaps. |
| `POST /v1/scrape-jobs/{id}/cancel` | signal the running `harvest()` | Needs a hook, see gaps. |

Filter parameters map onto the service's `selections` tuple:
`material` → `material_en`, `category` → `category_en`, `language` →
`language`, `province` → `province`, `place` → `place`.

## Gaps an implementation must close

These are listed so the contract is not mistaken for a promise the services
already keep. Each needs a decision or a small, deliberate change when the
endpoints are built. **None was made in this restructure**, because each
would change behaviour.

1. **Scrape-job cancellation.** `harvest()` stops cleanly only on
   `KeyboardInterrupt` (Ctrl+C or SIGTERM). A worker thread cannot receive
   either, so cancellation needs a stop hook, for example a `should_stop`
   callable checked between chunks that takes the same flush-and-checkpoint
   path.
2. **Scrape-job progress.** Progress exists only as `log()` lines. The
   `ScrapeJobProgress` fields need either a progress port, called where the
   per-chunk log line is written, or parsing of those lines.
3. **Scrape-job failure.** `run_harvest` calls `sys.exit(1)` when the index or
   lookup fetch fails. The worker must catch `SystemExit` and map it to
   status `failed`, or the service should raise a domain error, which would
   change the CLI's code path.
4. **Outcome.** `run_harvest` returns `None` and reports the outcome only in
   log lines. `complete`, `incomplete`, `partial` and `already_complete` need
   it to return a result.
5. **The bounding box.** Search, list and places inherit the map's
   search frame, which keeps 568,560 of 588,509 rows. It drops the 19,938
   rows with no coordinates, then the 11 rows outside `ROMAN_BOUNDS`
   (latitude 20–62, longitude −12–50; widened since `docs/AUDIT.md` D1, so
   Britannia is now inside). Dropping rows without coordinates is right for a
   map and questionable for a search API. **Decide** before
   implementing whether the API searches the bounded frame (as documented
   now) or the full corpus.
6. **`retrieved_at` in the cleaned corpus is epoch milliseconds**, not ISO 8601.
   pandas parsed the scraper's ISO strings as dates and wrote them back as
   integers (the `Pandas4Warning` about the `'epoch'` date format). The
   contract says `date-time`, so the API must convert, or the cleaning step's
   `to_json` call should get `date_format="iso"`. That would change the
   committed cleaned corpus, so it needs its own decision.
7. **Search TSV granularity.** `matched_results` selects by monument
   (`edcs_id`), so the export holds every inscription on a matching
   monument (927 rows for 515 `viator` matches). The contract documents this
   as current behaviour; whether it is intended is a question for you.
8. **Cursor stability.** Cursors encode an offset plus a fingerprint of the
   query and of the corpus snapshot. Rows come back in corpus order and
   places in ascending count order, so pages are stable for a given snapshot.
   A new harvest or cleaning run invalidates outstanding cursors (`400`).
9. **Authentication** is declared, not enforced. Flows and scopes are open in
   `docs/adr/0003-api-authentication.md`.

## Examples

Every example in `openapi.yaml` uses real data from the committed snapshot:
EDCS-00000001-0 and EDCS-00000144-0, the 515 `viator` matches (131 in Roma),
4,606 Latin inscriptions from Noricum, 21,999 known places, 1,388 cities, and
the first vertices of the first province and road. The exceptions are the
scrape-job examples, which describe a job that has never run. Their counts are
plausible, not observed. Cursor strings and ETags are illustrative.

## Validating the contract

No project dependency was added. Validation runs in throwaway environments:

```bash
uvx openapi-spec-validator app/api/openapi.yaml        # OpenAPI 3.1 structure
```

Each request and response example was also checked against its own schema
with `jsonschema` (Draft 2020-12), 109 examples in all, in a `uvx`
environment.

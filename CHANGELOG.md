# Changelog

All notable changes to EpigCorpus. Per remediation ground rule 2, every change
to `src/edcs_cleaner.py` alters what the corpus *is* and must be recorded here
with the epigraphic convention it implements.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed — CARTO basemap again, with a built-in fallback (2026-10-24)

- **CARTO tiles.** The map is drawn on CARTO's `dark_nolabels` tiles again,
  in Web Mercator. The equal-area projection added earlier today is removed.
- **Fallback.** The map switches to Natural Earth land from the API in
  either of two cases:
  - four tiles fail to load;
  - CARTO answers with its "API KEY REQUIRED" image. That is detected as the
    same non-flat picture served for three different tiles; real tiles
    differ, apart from flat open sea.

  So the background is never broken or black. All three cases (tiles load,
  key image, offline) were checked in the browser.
- **Fallback land fixed.** Natural Earth's Afro-Eurasia polygon crosses the
  antimeridian at the Bering Strait and was drawn as huge diagonal slivers.
  Polygons that cross ±180° are now split there, and Antarctica is left out.
- **No pan limit.** The view no longer has a limit, which made panning feel
  stuck.
- **Unchanged.** The grouping of known places when zoomed out stays.

### Changed — the cleaned corpus writes `retrieved_at` as ISO 8601 (2026-10-24)

- **The change.** Cleaning wrote `retrieved_at` as epoch milliseconds
  (`1787157131000`), pandas' default. That turned the scraper's ISO
  timestamp into a bare number, contradicted `data/README.md`, and is
  deprecated in pandas 3 (`Pandas4Warning`). It is now written as ISO 8601
  in whole seconds, `2026-08-19T16:32:11Z`, exactly as the scraper records
  it.
- **Verified on the full corpus.** All 588,509 records are identical
  except for that field, whose ISO value equals the old epoch value. The
  new cleaned JSONL's sha256 is `36e4d626…8d92d3`; the old one was
  `54b9ee37…79699`.
- **Existing corpora.** The committed snapshot keeps the epoch form until
  the next pipeline run. Both forms read back as timestamps, and the API
  answers ISO 8601 for either.
- **Tests.** The test suite now fails on any `Pandas4Warning`.

### Changed — an equal-area map with no edges; known places grouped when zoomed out (2026-10-24)

- **Background.** The map's land is now the whole world from Natural Earth
  1:50m. It was a rectangle cut at longitude −35…80 and latitude 0…75, whose
  straight edges showed as boxes and bands when zoomed out.
- **Projection.** The map is drawn in a Lambert azimuthal equal-area
  projection centred on the Mediterranean (40°N 15°E), replacing Web
  Mercator.
  - Regions keep their true relative size: Britannia and Germania are no
    longer inflated next to Africa and Aegyptus.
  - Tile basemaps such as CARTO only exist in Web Mercator. The map draws
    only vector data, so it can use the projection that suits it.
  - Zoom levels keep the old scale at 40°N.
  - The view is held to the Roman world with a margin.
- **Known places.** While zoomed out (below zoom 7), the 22,000 known places
  are grouped into a grid: one dot per area, larger and brighter with more
  inscriptions. Hovering shows the area's places and inscriptions, and a
  click zooms in. From zoom 7 places are drawn one by one, at their
  default-zoom size. Search results are always drawn one by one.
- **Legend.** The legend can be collapsed.
- **Compression.** API responses are gzip-compressed, which cuts the land
  layer from 1.2 MB to 0.4 MB.

### Fixed — pipeline runs no longer exhaust memory ("Load failed") (2026-10-24)

- **Symptom.** A pipeline run's clean stage could kill the server in a
  Docker container with the usual 4–8 GB allowance. The browser then showed
  "Load failed".
- **Cause.** Cleaning parsed the whole raw JSONL at once (6.5 GB peak) next
  to the corpus the server already held. The reload then held the old and
  new corpus side by side.
- **Fix.**
  - JSONL files are read in chunks of 50,000 rows. The frames are identical,
    and the peak drops from 6.5 GB to 1.5 GB.
  - Cleaning streams chunk by chunk into temporary files, which replace the
    corpus only when complete. The output is byte-identical (sha256
    `54b9ee37…79699`). A failed run leaves the previous corpus untouched.
  - The clean stage runs in a child process, which also refreshes the
    Parquet snapshot. Its memory is freed when it ends. If memory still runs
    out, only the child dies, and the run fails with a message that says so.
  - The reload drops the old catalog before loading the new one.
  - In a container limited to 4 GB, a full clean-and-reload run now succeeds
    and peaks at 3.1 GB.
- **UI.** The pipeline panel keeps polling through a lost connection, and
  explains when a restarted server has forgotten a run.

### Changed — backend/frontend split; the pipeline runs through the API (2026-10-24)

- **Layout.** The code is the installable package `backend/src/epigcorpus`,
  the web UI is `frontend/`, and data and research material moved to the root
  `data/` and `research/`. Every file moved with its history and an unchanged
  checksum (`docs/refactor/path-map-backend.tsv`). See
  `docs/adr/0005-backend-frontend-split.md`.
- **No command line.** `epigcorpus.sh`, `python -m app.main` and
  `app.cli.*` are removed. Start the server with
  `uv run uvicorn epigcorpus.api.server:app` (from `backend/`) or
  `docker compose up`.
- **Pipeline runs.** `POST /api/v1/pipeline-runs` harvests, cleans and then
  reloads the corpus into the running server, with no restart. Runs can be
  polled, listed and cancelled. `/v1/scrape-jobs` is gone. A clean-only run
  on the full corpus writes a cleaned JSONL byte-identical to the baseline
  (sha256 `54b9ee37…79699`).
- **API under `/api/v1`** (was `/v1`). New: `GET /api/v1/places.geojson` and
  `/places/known.geojson`, the map's places as GeoJSON.
- **Faster start.** The server reads the cleaned corpus from a Parquet
  snapshot, rebuilt automatically when the JSONL changes. It now starts in
  about 5 s and 1.6 GB, down from about 70 s and 8 GB. Search results are
  unchanged: the snapshot reads back identical to the JSONL.
- **React web UI.** It is built with TypeScript, Vite, Tailwind and Leaflet,
  in a sidebar / map / table layout, with a pipeline panel. It has the same
  map, filters, downloads and shareable links. Leaflet is bundled, so the
  UI no longer needs unpkg.
- **Docker and CI.** A multi-stage `Dockerfile` and `docker-compose.yml`,
  plus GitHub Actions. CI runs backend lint and tests; frontend type check,
  unit tests and build; Playwright end-to-end; and a Docker build.

### Changed — known places can be shown under search results (2026-10-23)

- During a search the "Known places" layer is no longer disabled: it starts
  off, so the results stand out, and can be switched on to show every known
  place underneath roads, cities and results. The choice is remembered for
  later searches; with no search, known places show as before.
- Known places load once in the background, so the layer is also available
  when the page is opened from a search link.

### Changed — map redesign: solid markers, muted provinces, fixed stacking (2026-10-23)

- Known places, cities and search results are solid circles with a thin
  dark outline (they were translucent rings), so neighbouring and
  overlapping places stay apart when zoomed in. New colours: teal places,
  yellow cities, orange results; roads light grey.
- The provinces are the base layer and start switched on, in a muted
  eight-colour palette so the markers read over them; their names use a
  lighter tint of the same colour.
- Fixed stacking, bottom to top: land, provinces, known places, roads,
  cities, search results (province names as text above). The order holds
  whichever layer loads or is switched on first; hovering still reaches
  places under the roads. The layer control lists the layers in that
  order.
- Search results never draw smaller than a minimum size, so single-match
  places stay findable among the cities.
- The PNG export keeps its own print palette; it is unchanged.

### Changed — basemap without labels, province names in province colours (2026-10-23)

- CARTO's basemaps now answer without an API key with an "API KEY REQUIRED"
  tile. The web map no longer uses map tiles at all: the background is
  Natural Earth land outlines, vendored in
  `app/data/reference/map_layers/natural_earth_land_50m.geojson` and served
  at `/v1/map/layers/land`. No key, no tile service, no place names. The
  "Place names" toggle is gone. (Esri's dark grey tiles were tried first and
  showed as a black background.)
- The "Roman provinces" layer now writes each EDCS province's name, in the
  colour of the shading beneath it (`/v1/map/layers/province-labels`).
  Overlapping names are hidden, larger provinces first; more appear as you
  zoom in.
- Unused CARTO and Leaflet constants left in `app/config/settings.py` by the
  Streamlit app are removed.

### Added — the HTTP API (2026-10-23)

- `app/api` implements the v1 contract in `app/api/openapi.yaml`: search,
  records, cleaned variants, places, map layers, JSONL/TSV/PNG downloads,
  lookups, facets, health, and asynchronous scrape jobs (start, progress,
  cancel). Run it with `python -m app.cli.serve`.
- The harvest gained optional stop and progress hooks and returns its
  outcome; the command line behaves exactly as before.
- New dependencies: fastapi, uvicorn (dev: httpx, pyyaml, jsonschema).

### Changed — Streamlit replaced by a web UI on the API (2026-10-23)

- The Streamlit app and the `streamlit` dependency are gone. The map is now
  `app/ui/static`, served by the API at `/`, with the same controls, map,
  summary, results and downloads; see
  `docs/adr/0004-replace-streamlit-with-an-api-client-ui.md`.
- `python -m app.main` step 3 launches the web app; `--skip-map` skips it.
- The PNG export is byte-identical; it now renders in
  `app/services/map_png.py`.
- The results table pages 100 rows at a time; Download TSV is unchanged.

### Changed — code and data moved under `app/`, in layers (2026-10-23)

- No change to corpus semantics, cleaning rules, CLI flags, output file
  names or formats. The full-corpus cleaning output, a fixture harvest and
  the map's queries and PNG exports were checked byte for byte against the
  previous layout (`docs/refactor/verification.md`).
- Code: `src/edcs_cleaner.py` is now `app/core/cleaning.py` (rules) and
  `app/services/cleaning.py` (`clean_data`); `src/edcs_scraper.py` is split
  across `app/adapters/edcs_client.py`, `app/adapters/harvest_store.py`,
  `app/core/edcs_parsing.py`, `app/services/scrape.py` and
  `app/cli/scrape.py`; `src/edcs_streamlit_map.py` is `app/ui/streamlit_app.py`
  on top of `app/services/search.py`, `app/services/map_data.py` and
  `app/adapters/map_layers.py`. See `docs/adr/0001-layered-app-layout.md`.
- Commands: `python main.py` → `python -m app.main`;
  `python src/edcs_scraper.py` → `python -m app.cli.scrape`;
  `streamlit run app/ui/streamlit_app.py`. `./epigcorpus.sh` is unchanged.
- Data: `data/`, `results/` and `reports/` moved to `app/data/{raw,processed,
  reference,results,reports}/`, every file hash-identical; mapping in
  `docs/refactor/path-map.tsv`. Every path is defined in
  `app/config/paths.py`.
- Tests moved to `app/tests/`; `tests/test_api_contract.py`, which pins the
  upstream EDCS schema, is now `app/tests/adapters/test_edcs_upstream_contract.py`.
  `app/tests/test_layering.py` enforces the dependency rule.

### Changed — queries described in words rather than key/value pairs (2026-08-19)

- `describe_query` phrases a query as a title and a single subtitle line, used
  by both the PNG export and the on-screen match summary:
  `Inscriptions matching "viator"` /
  `In Latin, on stone or marble, classified as tomb inscriptions, from Noricum
  and Dalmatia — searched in raw inscription text`.
- Only the filters actually in use are named. Locations read with "and" because
  the map really does show all of them; attributes read with "or" because an
  inscription matches just one of them.
- The subtitle is guaranteed to be one line. If the sentence runs long, the
  wordiest selections collapse into counts one at a time (`from 7 provinces`),
  so short selections stay spelled out for as long as they fit; the search-mode
  clause shortens next, and only then is the line trimmed on a word boundary.

### Changed — search controls laid out in three equal rows (2026-08-19)

- The controls sit in three equal-width rows: *Search in / Keyword / Language*,
  then *Province / Place / Category*, then *Material / Search / Download as
  PNG*.
- **`st.form` is gone.** A download button cannot live inside a form, and the
  PNG button shares the last row with Search. Widget changes therefore rerun
  the script, so `run_query` is now cached on the query itself rather than
  re-filtering half a million rows on every rerun. Pressing Enter in the
  keyword box still searches, via the input's `on_change`.
- **The PNG button is live at all times.** With no query it exports the
  full corpus map — every known place, teal, sized by inscription count — and
  with a query it exports the results in tangerine. Its title and footer
  describe whichever is on screen.
- **The PNG now uses the map's palette instead of white paper**: the dark
  background, light-grey roads, yellow city rings and the same teal/tangerine
  markers, with light type throughout. Everything the export carried before is
  kept — title, search term, result count, search mode, data source and the
  citation footer — and the active filters are listed alongside them. The map
  area shrinks as the footer grows, so extra filter lines cannot overdraw
  Aegyptus.
- `img/Example export png image for term viator.png` in the README still shows
  the old white export and is now out of date.
- Dropdown options show their short form on a dimmed second line, as one
  selectable item: `bronze / aes`, `curse tablets / defixiones`, `Greek / gr`.
  The short forms are the Latin tokens and language codes in
  `edcs_lookup.json`; province and place have none and stay single-line.

### Added — faceted filters on the map search (2026-08-19)

- Keyword search gained five multi-select filters: **material, category,
  language, province and place**, each accepting any number of values. Place
  carries roughly 15,000 options, so it leans on the widget's type-to-search.
- `category_en` holds a list per row and `language` arrives comma-joined
  (`Greek, Latin`), so both are filtered on token overlap: selecting `Latin`
  matches a bilingual inscription. Material, province and place match exactly.
- The keyword box no longer defaults to `viator`; it starts empty and is
  optional. A query is any keyword, any filter, or both.
- **The teal known-places layer now appears only when nothing is filtered.**
  Once a query runs, the corpus-wide layer is dropped from the payload
  entirely and its checkbox is disabled, leaving only the tangerine results.
- Every search widget carries an explicit `key` and no `default`/`value`.
  Passing a default that changes between runs churns the widget identity, which
  silently restored a filter the user had just cleared.
- `load_all_inscriptions` takes its column set as an argument so the set lands
  in the cache key. Adding a column to `REQUIRED_COLS` previously left a
  running app serving a cached frame without it, which is a `KeyError` at the
  first use.
- The PNG export's title and footer are composed by the caller, since a query
  can now be a keyword, a set of filters, or both.

### Changed — interactive map rebuilt on a dark basemap (2026-08-19)

- `src/edcs_streamlit_map.py` now renders Leaflet directly instead of Folium.
  Styling follows the folium cartography tutorial at
  https://nbviewer.org/github/vincentropy/python_cartography_tutorial/blob/master/part1_basic_folium_maps.ipynb:
  CARTO `dark_matter` basemap, flat teal `#0A8A9F` and tangerine `#E37222`
  circle markers, and magnitude carried by circle size rather than by hue.
- **One circle per place, not one dot per inscription.** Places are aggregated
  and sized continuously by their inscription count. The tutorial sizes circles
  linearly (`radius = count / 20`), which suits tens of trips per bike station;
  a single place here can hold thousands of inscriptions, so the radius is
  continuous on a log scale instead. Radii also scale with the zoom level.
- Circles are drawn smallest-first so the largest sit on top, and fills are held
  at low alpha: with 22,000 overlapping places a flat colour at full opacity
  turns Europe into a solid block, whereas low alpha lets density read as
  brightness on the dark basemap.
- The map is now always on screen. The teal "known places" layer covers the
  whole corpus — plus places `edcs_lookup.json` knows that hold no inscription
  here, drawn as small grey dots — and a search overlays tangerine result
  circles in a pane above it.
- Controls: layer checkboxes (known places / search results / place names), a
  size legend, a reset-view button, hover tooltips, and result popups listing up
  to eight matching inscriptions per place. All chrome is dark, since white
  Leaflet panels glare against `dark_matter`.
- "Place names" toggles the basemap between CARTO `dark_all` and
  `dark_nolabels`.
- Roman provinces, roads and cities are kept as optional overlays, switched off
  by default, so the vendored layers stay reachable. Cities carry their own
  hover tooltip naming the site (`Taucheira / Tawqrah, Creta et Cyrenaica`,
  from the ancient and modern toponyms and province in the Hanson 2016 data).
- **Fixed: switching on roads or cities killed every hover tooltip.** Leaflet
  binds mouse events to each canvas element, so only the topmost interactive
  canvas hit-tests and one laid over the markers swallows their hovers — the
  same reason the search-results layer blocked hovering known places
  underneath it. Everything hoverable now shares a single canvas, where
  stacking and hit priority both follow draw order (places, then cities, then
  results), and the two non-interactive layers sit in panes with
  `pointer-events: none` so events fall straight through them.
- **`ROMAN_BOUNDS` widened to `(20, 62, -12, 50)`.** The old
  `(25, 50, -10, 45)` box silently dropped Britannia, northern Germania and the
  eastern frontier from the map and from every result count derived from it.
- PNG export uses the same tangerine markers and continuous sizing, with
  print-safe substitutes for the two colours that only work on a dark
  background, and its footer now reports matches *and* the number of places they
  fall in.
- `folium` removed from the dependencies; nothing imports it any more.

### Added — corpus snapshots committed, compression automated (2026-08-19)

- Full EDCS harvest committed as gzipped snapshots: **542,854 monuments /
  588,509 inscription rows**, harvested 2026-08-19 against EDCS release
  `20260807-142626`. 39 MB and 12 MB compressed (13.7x and 8.2x).
- **Compression is automatic in both directions.** The scraper compresses on
  completion; `main.py` and the map read plain or gzipped transparently,
  preferring a plain file when present. No user ever runs gzip or gunzip.
- `main.py` also writes `edcs_inscriptions_cleaned.jsonl.gz` alongside the
  plain cleaned corpus.
- `.gitignore` now ships `*.jsonl.gz` / `*.tsv.gz` and ignores the plain working
  copies and the regenerable index cache.
- **Stale Git LFS rules removed from `.gitattributes`.** They marked
  `data/*.json` and `data/*.tsv` as LFS-managed while git-lfs was not installed,
  which would have committed broken pointer files.

### Fixed — scraper could destroy a completed harvest (2026-08-19)

- A completed harvest deletes its checkpoint as the "done" signal, but `main()`
  only looked for a checkpoint. With none present the cursor stayed at 0 and
  `harvest()` opened the output files in `"w"` mode — **silently truncating a
  finished 540 MB corpus on any plain re-run**. The "already complete" branch
  could only ever fire while a checkpoint existed, i.e. when the harvest was
  *not* complete.
- Fixed with two independent guards: `data/edcs_harvest_manifest.json` records
  completion durably, and `harvest()` now refuses to truncate any non-empty
  output file regardless of how it was reached.
- SIGTERM now stops a harvest as cleanly as Ctrl+C. Background jobs launched
  from a non-interactive shell inherit SIGINT set to ignore, so SIGTERM was the
  only way to stop a detached run — and it killed the process mid-chunk, leaving
  output ahead of the checkpoint.

### Changed — base-map layers vendored (2026-08-19)

- `data/lat_epig_support/` renamed to **`data/map_layers/`** (via `git mv`, so
  file history is preserved). The folder never held third-party project data —
  it holds AWMC province and road shapefiles and the Hanson 2016 OXREP cities
  CSV. It was named after whichever repository the files had been mirrored from.
- **Runtime downloads removed.** `edcs_streamlit_map.py` no longer fetches
  shapefiles from `raw.githubusercontent.com` at startup. All 13 layer files are
  committed, so the map now loads them from the repository and raises an
  actionable error if any are missing. This removes the last runtime dependency
  on an external repository. (T30)
- `data/map_layers/README.md` added with per-layer provenance, licence terms
  (AWMC is ODbL-1.0; MIT covers EpigCorpus code only) and attribution, plus a
  note that the AD 117 province layer is a reference frame, not a
  contemporaneous basemap. (partial T28)
- Scraper field comments rewritten to describe each column on its own terms.

### Changed — scraper port to the 2026 EDCS (2026-08-19)

- **`src/edcs_scraper.py` rewritten** for the EDCS release of 2026-08-07.
  The old DataTables harvest against `/api/query` (403 since that release) is
  replaced by the static-file architecture: one request for the full corpus
  index, then one request per monument. See `docs/EDCS_API.md`.
- **Coordinate order corrected.** The new `places.json` returns
  `[latitude, longitude]`; the old API returned `[longitude, latitude]`. The
  original hardcoded `longitude = coord[0]`, which would have transposed the
  entire corpus on port. (T34, partial)
- **Full LatEpig 2.0 field parity.** Added `publication`, `raw_dating`,
  `dating_from`, `dating_to`, `status`, `comment`, `photo`, plus
  `language_codes` and `photo_credits`. `publication` and `raw_dating` use the
  site's own `citationLabel()` and `formatDatingRange()` formats so our columns
  are joinable against EDCS's own rendering. Not portable: `partner_link` and
  `TM Place` were links on the retired PHP pages and no longer exist;
  `extra_text` / `extra_html` were HTML-scraping leftovers.
- **Identifying User-Agent** replaces the spoofed Firefox UA, forged `Referer`
  and `X-Requested-With`. Verified to be served normally by the static
  endpoints. (T26)
- **Failed fetches are recorded, not swallowed.** Retries with exponential
  backoff, honours `Retry-After`, writes unrecoverable ids to
  `data/edcs_failed_ids.json`, and refuses to report a harvest as complete while
  that file is non-empty. (T19, T27, partial)
- **Record-level provenance**: `retrieved_at` and `source_url` on every row. (T17, partial)
- Existing column names and output paths are unchanged, so `main.py`,
  `edcs_cleaner.py` and `edcs_streamlit_map.py` run untouched.

The tasks marked *partial* were completed only insofar as the port required
them; their full scope in Phases 1-3 is unchanged.

### Added — Phase 0 (2026-08-19)

- `docs/AUDIT.md` (T00) — data-flow audit: every record-drop point, every
  mutation point, every user-visible count, with measured loss figures.
- `docs/LATEPIG_BREAKAGE.md` (T01) — reproduction of the LatEpig failure against
  the current EDCS, and of the equivalent EpigCorpus failure.
- `docs/EDCS_API.md` (T02) — characterisation of the EDCS release of
  2026-08-07, which replaced the API this project targets.
- `tests/` (T03) — pytest suite with 50 real monument payloads plus index
  fixtures captured 2026-08-19; runs entirely offline.
- `.github/workflows/ci.yml` (T03) — CI running `ruff check` and `pytest`.
- `pytest` and `ruff` dev dependencies, with a dated per-file lint-debt baseline
  in `pyproject.toml` to be deleted task by task.

### Notes

No pipeline code was changed in Phase 0. Corpus semantics are unaffected.

Two findings correct the remediation brief:

- **T04** — `is_forged` is not dead code. EDCS marks *falsae* with a `*` prefix
  on the citation number (4,719 instances in CIL alone, verified 2026-08-19),
  and the existing check fires on it correctly. See `docs/AUDIT.md` §4 F1.
- **T09** — the brief's example `((sestertium))` parses correctly. The rule
  fails on genuinely nested parentheses, e.g. `(A(uli) f(ilius))`.

**Blocking:** the EDCS release of 2026-08-07 withdrew `/api/query`, which
returns 403. The harvester does not run. Phases 1–7 assume it does.
See `docs/LATEPIG_BREAKAGE.md` §7.

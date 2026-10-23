# ADR 0004 — Replace the Streamlit app with a web UI on the API

- **Status:** Accepted
- **Date:** 2026-10-23
- **Supersedes:** the "UI frozen" constraint of ADR 0001, and the "later" in ADR 0002's "the UI will later become a client of this API"
- **Relates to:** ADR 0002 (API-first design)

## Context

The maintainer asked for the API to be implemented, for Streamlit to be
removed completely, and for a basic UI with the same features. After ADR 0001
the Streamlit app was a thin shell over the services, but it still owned
three things: the caching (`st.cache_data`), the PNG renderer, and the
Leaflet map. It also kept the whole corpus in memory per Streamlit server.

## Decision

1. **The Streamlit app is deleted and `streamlit` leaves the dependencies.**
   What it owned moved down a layer, verbatim:
   - caching became `app/services/catalog.py` (`Catalog`);
   - the PNG renderer became `app/services/map_png.py`, byte-identical
     output;
   - the export's title, footer and file name moved to `Catalog.export_png`.
2. **The UI is static HTML, CSS and JavaScript in `app/ui/static`**, served
   by the API at `/`, and calls only `/v1`. There is no build step, no
   framework and no npm dependency. Leaflet still comes from unpkg, as it did.
3. **Same features, same behaviour**, item by item:
   - the search modes, the keyword with Enter, and the five multi-select
     filters with their dimmed short forms (place has type-to-search);
   - Search;
   - Download as PNG, reflecting the map;
   - the summary sentence and the info and no-match notices;
   - the map: known places versus results, fit to the results, popups,
     layer toggles, provinces, roads, cities, place names, reset and legend;
   - the results table and Download TSV.

   The map code is the Streamlit template's, ported. One addition: the
   submitted query is kept in the address bar.
4. **`python -m app.main` keeps its flags.** Step 3 launches the web app
   instead of Streamlit.

## Consequences

- The UI and every other client get identical search behaviour by
  construction, because the UI has no other way in.
- One server process holds the corpus once, and the page loads in about 20
  seconds after start instead of on each Streamlit session.
- The results table is paged, 100 rows at a time, instead of showing every
  column of every matched monument at once. The full table is still the TSV
  download, unchanged.
- The UI has no automated browser test in the suite. It was checked by hand in
  headless Chromium (see the commit that added it), and the API beneath it is
  covered by the contract tests.
- `streamlit run …` no longer exists; `python -m app.cli.serve` replaces it.

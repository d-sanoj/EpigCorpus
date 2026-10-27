# EpigCorpus frontend

The web UI is built with React, TypeScript, Vite, Tailwind and Leaflet. It is
a client of the API in `backend/`, or — in the static build — of the corpus
files described in `docs/static-site.md`.

```bash
npm ci
npm run dev        # http://localhost:5173, forwarding /api to the backend on :8000
npm run build      # dist/, which the backend serves at /
npm test           # vitest unit tests
npm run typecheck
npm run gen:api    # regenerate src/api/schema.d.ts from the backend's openapi.yaml
npm run e2e        # Playwright; needs a server with dist/ on :8000 (or E2E_BASE_URL)
```

With `VITE_DATA_MODE=static` the same build answers its own queries and drops
the things that need a server: `VITE_BASE` sets the path it is served from
(`/EpigCorpus/` on Pages). See [../docs/static-site.md](../docs/static-site.md).

## Layout

```
src/
├── api/              client.ts (typed fetch), query.ts (the query and its URL forms),
│                     schema.d.ts (generated, do not edit),
│                     static/ (the serverless build: the corpus container, the
│                     query port, the text worker, the /api/v1 paths it answers)
├── components/ui.tsx buttons, segmented controls, badges, popover dismissal
├── features/
│   ├── search/       SearchPanel, MultiSelect: keyword, where to search, filters
│   ├── map/          epigMap.ts (the Leaflet map), MapView (controls, layers, legend)
│   ├── results/      ResultsDrawer: the bar over the map that opens into the table
│   └── pipeline/     usePipeline (follows runs), PipelineSheet (the side panel)
├── lib/              format.ts, theme.ts (light, dark or system; remembered)
└── App.tsx           top bar, layout, the current query, loading the corpus
```

## Design

- **Themes.** Light by default, with dark mode; the theme switch in the top
  bar offers light, dark or the system's setting. Every colour is a token in
  `index.css` (`--bg`, `--surface`, `--line`, `--fg`, `--accent`, and so
  on), defined once per theme. The map follows the theme: OpenFreeMap
  "positron" or "dark" underneath, and each palette's marker colours.
- **The map gets the room.** The search panel can be hidden from the top
  bar. The legend and the results table start collapsed, and the layer list
  opens from the map's controls.
- **Markers.** They are the same solid dots, sized by inscription count:
  teal for known places, terracotta for search results, ochre for cities.

The map is plain Leaflet inside a React component. Its background is an
OpenFreeMap vector style drawn by MapLibre GL through
`@maplibre/maplibre-gl-leaflet` (no API key). Places, roads, cities and
results share one canvas, and their stacking follows draw order, so the map
code stays imperative. See `docs/adr/0005-backend-frontend-split.md`.

# EpigCorpus frontend

The web UI is built with React, TypeScript, Vite, Tailwind and Leaflet. It is
a client of the API in `backend/` and nothing else.

```bash
npm ci
npm run dev        # http://localhost:5173, forwarding /api to the backend on :8000
npm run build      # dist/, which the backend serves at /
npm test           # vitest unit tests
npm run typecheck
npm run gen:api    # regenerate src/api/schema.d.ts from the backend's openapi.yaml
npm run e2e        # Playwright; needs a server with dist/ on :8000 (or E2E_BASE_URL)
```

## Layout

```
src/
├── api/              client.ts (typed fetch), query.ts (the query and its URL forms),
│                     schema.d.ts (generated, do not edit)
├── features/
│   ├── search/       Sidebar, MultiSelect: search mode, keyword, filters, downloads
│   ├── map/          epigMap.ts (the Leaflet map), MapView (toggles, legend)
│   ├── results/      ResultsTable: the matches, 100 at a time
│   └── pipeline/     PipelinePanel: start, follow and cancel pipeline runs
├── lib/format.ts
└── App.tsx           layout, the current query, loading and reloading the corpus
```

The map is plain Leaflet inside a React component. Places, roads, cities and
results share one canvas, and their stacking follows draw order, so the map
code stays imperative. See `docs/adr/0005-backend-frontend-split.md`.

# EpigCorpus backend

The `epigcorpus` Python package covers the EDCS harvest, the cleaning, the
catalog the API serves, and the HTTP API. See the repository README for the
whole picture, and `src/epigcorpus/api/README.md` for the API.

```bash
cd backend
uv sync                                                  # creates backend/.venv
uv run uvicorn epigcorpus.api.server:app --port 8000     # API at /api/v1, web UI at /
uv run pytest                                            # tests
uv run ruff check src tests                              # lint
```

The web UI is served from `../frontend/dist` once it is built (`npm run build`
in `frontend/`). Without it the API still runs.

## Configuration

| Variable | Default | What it moves |
|---|---|---|
| `EPIGCORPUS_DATA_DIR` | `<repo>/data` | the corpus, harvest state, lookups, map layers |
| `EPIGCORPUS_RESEARCH_DIR` | `<repo>/research` | research outputs (scripts only) |
| `EPIGCORPUS_STATIC_DIR` | `<repo>/frontend/dist` | the built web UI |
| `EPIGCORPUS_CARTO_KEY` | none | CARTO basemap API key (free at https://carto.com/basemaps/apikey); without it the map's tiles say "API KEY REQUIRED" |

## Layers

`config` and `models` sit at the bottom. `core` holds pure rules. `adapters`
handles files and HTTP. `services` holds the use cases, which reach adapters
only through `services/ports.py`. `api` is FastAPI, and `api/server.py` is
the composition root, the one module that wires adapters into services.
`tests/test_layering.py` enforces this.

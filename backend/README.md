# EpigCorpus backend

The `epigcorpus` Python package: EDCS harvest, cleaning, the search index and
the HTTP API. See the repository README for the whole picture.

```bash
cd backend
uv sync                                                  # creates backend/.venv
uv run uvicorn epigcorpus.api.server:app --port 8000     # API at /api/v1, web app at /
uv run pytest                                            # tests
uv run ruff check .                                      # lint
```

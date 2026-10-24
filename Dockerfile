# EpigCorpus: one image with the API and the built web UI.
#
#   docker compose up --build        # see docker-compose.yml
#
# The corpus is not baked in: mount the repository's data/ at /app/data.

# ─── frontend: build the static app ─────────────────────────────────────────
FROM node:22-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# ─── backend: the Python package and its locked dependencies ────────────────
FROM python:3.13-slim AS app
RUN pip install --no-cache-dir uv==0.11.32
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg \
    EPIGCORPUS_DATA_DIR=/app/data \
    EPIGCORPUS_RESEARCH_DIR=/app/research \
    EPIGCORPUS_STATIC_DIR=/app/frontend/dist
WORKDIR /app/backend
# Dependencies first, so code changes don't reinstall them.
COPY backend/pyproject.toml backend/uv.lock backend/.python-version ./
RUN uv sync --frozen --no-dev --no-install-project
COPY backend/ ./
RUN uv sync --frozen --no-dev
COPY --from=frontend /app/frontend/dist /app/frontend/dist

# Runs as root so it can write to a bind-mounted data/ owned by any host
# user (the Parquet snapshot, pipeline output).
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health', timeout=4)"
CMD ["uvicorn", "epigcorpus.api.server:app", "--host", "0.0.0.0", "--port", "8000"]

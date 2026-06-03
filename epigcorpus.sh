#!/usr/bin/env bash

set -euo pipefail

MIN_PY="3.13"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting EpigCorpus pipeline..."

version_ge() {
  # Returns success if $1 >= $2 for dot-separated numeric versions.
  [[ "$(printf '%s\n' "$2" "$1" | sort -V | tail -n1)" == "$1" ]]
}

python_version() {
  "$1" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
}

ensure_uv() {
  if command -v uv >/dev/null 2>&1; then
    return 0
  fi

  echo "uv not found. Attempting automatic install..."

  if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl is required to install uv automatically."
    echo "Install uv manually: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
  fi

  curl -LsSf https://astral.sh/uv/install.sh | sh

  # Typical install path from uv installer.
  export PATH="$HOME/.local/bin:$PATH"

  if ! command -v uv >/dev/null 2>&1; then
    echo "Error: uv installation did not complete successfully."
    echo "Install uv manually: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
  fi
}

if [[ -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
  echo "Using local virtual environment: .venv"

  VENV_PY="$SCRIPT_DIR/.venv/bin/python"
  VENV_VER="$(python_version "$VENV_PY")"
  if ! version_ge "$VENV_VER" "$MIN_PY"; then
    echo "Warning: .venv Python version is $VENV_VER, but >= $MIN_PY is recommended."
  fi

  exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/main.py" "$@"
fi

ensure_uv

echo "No .venv found. Bootstrapping environment with uv..."
uv python install "$MIN_PY"
uv sync --python "$MIN_PY"
exec uv run --python "$MIN_PY" python "$SCRIPT_DIR/main.py" "$@"

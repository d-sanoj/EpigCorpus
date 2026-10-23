"""Paths for the research scripts in this folder, from app/config/paths.py.

The scripts run by file path (python app/scripts/<name>.py), which puts this
folder on sys.path but not the repository root, so the root is added here
before app.config.paths is re-exported. In the app/ restructure the scripts
changed only in their path lines and in importing this module.
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.config.paths import *  # noqa: E402,F403

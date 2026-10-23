"""Turn pandas rows into the JSON shapes in openapi.yaml.

pandas reads missing values as NaN, years as floats once a column has a gap,
lists as lists, and the cleaned corpus's `retrieved_at` as a timestamp (it was
written as epoch milliseconds). The API answers with null, integers, lists and
ISO 8601 strings.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from app.models.inscription import REQUIRED_COLS

SUMMARY_FIELDS = [
    "record_id",
    "edcs_id",
    "province",
    "place",
    "latitude",
    "longitude",
    "not_before",
    "not_after",
    "language",
    "material_en",
    "category_en",
    "inscription_text",
    "inscription_text_interpretive",
    "inscription_text_conservative",
]
assert set(SUMMARY_FIELDS) == set(REQUIRED_COLS)

FULL_FIELDS = SUMMARY_FIELDS + [
    "inscription_index",
    "material",
    "dating_from",
    "dating_to",
    "raw_dating",
    "language_codes",
    "category",
    "status",
    "evidence",
    "publication",
    "image_urls",
    "photo",
    "photo_credits",
    "comment",
    "retrieved_at",
    "source_url",
    "is_unreadable",
    "is_forged",
]

LIST_FIELDS = {"category_en", "category", "status", "language_codes", "evidence"}
INT_FIELDS = {"not_before", "not_after", "dating_from", "dating_to", "inscription_index"}
BOOL_FIELDS = {"is_unreadable", "is_forged"}


def _missing(value) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    return isinstance(value, float) and math.isnan(value)


def value(name: str, raw):
    """One cell as JSON."""
    if name in LIST_FIELDS:
        if isinstance(raw, (list, tuple, np.ndarray)):
            return [str(item) for item in raw]
        return []
    if isinstance(raw, (list, tuple, np.ndarray)):
        return list(raw)
    if _missing(raw):
        return None
    if name in INT_FIELDS:
        return int(raw)
    if name in BOOL_FIELDS:
        return bool(raw)
    if name == "retrieved_at":
        return _iso(raw)
    if isinstance(raw, np.integer):
        return int(raw)
    if isinstance(raw, np.floating):
        return float(raw)
    if isinstance(raw, np.bool_):
        return bool(raw)
    return raw


def _iso(raw) -> str:
    if isinstance(raw, pd.Timestamp):
        stamp = raw.tz_localize("UTC") if raw.tzinfo is None else raw.tz_convert("UTC")
        return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(raw, (int, float, np.integer, np.floating)):
        return datetime.fromtimestamp(float(raw) / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(raw)


def record(row, fields: list[str]) -> dict:
    """A row (Series or namedtuple-like mapping) as a dict of the given fields."""
    return {name: value(name, row.get(name, None)) for name in fields}


def summaries(frame: pd.DataFrame) -> list[dict]:
    return [record(row, SUMMARY_FIELDS) for _, row in frame.iterrows()]


def place(row, with_examples: bool) -> dict:
    out = {
        "place": str(row["place"]),
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "count": int(row["count"]),
    }
    if with_examples:
        items = row.get("items") if hasattr(row, "get") else None
        out["examples"] = [str(item) for item in (items or [])]
    return out


def places(frame: pd.DataFrame, with_examples: bool) -> list[dict]:
    return [place(row, with_examples) for _, row in frame.iterrows()]

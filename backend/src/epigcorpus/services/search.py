"""Search use cases over the cleaned corpus: load, filter options, query, known places.

Nothing here caches. epigcorpus.services.catalog.Catalog memoises these functions
and passes its memoised loader back in as `load_all`, so nested loads (filter
options, known places and queries all reading the search frame) share one copy.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from epigcorpus.config.settings import ROMAN_BOUNDS
from epigcorpus.core.search import (  # noqa: F401  (re-exported for the UI and a future API)
    FILTER_LABELS,
    SEARCH_COLUMNS,
    _normalize_search_mode,
    _place_result_items,
    _token_set,
    aggregate_places,
    apply_filters,
    describe_query,
)
from epigcorpus.models.inscription import MULTI_VALUE_FILTERS, REQUIRED_COLS
from epigcorpus.services.ports import CleanedCorpusReader

# The (possibly cached) loader of the search frame, i.e. load_inscriptions
# with its reader already bound.
InscriptionLoader = Callable[[], pd.DataFrame]


def load_inscriptions(
    reader: CleanedCorpusReader, columns: tuple[str, ...] = tuple(REQUIRED_COLS)
) -> pd.DataFrame:
    """Load JSONL with only needed columns - minimal memory footprint.

    Rows without coordinates, or outside ROMAN_BOUNDS, are dropped: this is
    the frame every search runs on.
    """
    # Load JSONL and keep only required columns
    data = reader.read_cleaned_corpus()

    # Select only columns we need
    cols_to_keep = [c for c in columns if c in data.columns]
    data = data[cols_to_keep]

    # Fast numeric conversion and bounds check
    data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
    data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")

    # Drop invalid coords
    data = data.dropna(subset=["latitude", "longitude"])

    # Quick Roman bounds filter
    min_lat, max_lat, min_lon, max_lon = ROMAN_BOUNDS
    data = data[
        (data["latitude"] >= min_lat) & (data["latitude"] <= max_lat) &
        (data["longitude"] >= min_lon) & (data["longitude"] <= max_lon)
    ]

    return data.reset_index(drop=True)


def load_full_cleaned_data(reader: CleanedCorpusReader) -> pd.DataFrame:
    """Load full cleaned dataset with all columns for detailed result table."""
    return reader.read_cleaned_corpus()


def short_forms(reader: CleanedCorpusReader) -> dict[str, dict[str, str]]:
    """Map each English filter value to its abbreviated form, where one exists.

    EDCS records materials and categories under a Latin token (`aes`,
    `tituli sepulcrales`) and languages under an ISO-ish code, alongside the
    English label the corpus columns carry. The dropdowns show both.
    """
    forms: dict[str, dict[str, str]] = {"material_en": {}, "category_en": {}, "language": {}}
    lookup = reader.read_lookup()
    if lookup is None:
        return forms

    for source, column, short_key in (
        ("materials", "material_en", "token"),
        ("categories", "category_en", "token"),
        ("languages", "language", "code"),
    ):
        for entry in (lookup.get(source) or {}).values():
            english = str(entry.get("en") or "").strip()
            short = str(entry.get(short_key) or "").strip()
            if english and short and short not in {english, "?"}:
                forms[column][english] = short
    return forms


def filter_options(load_all: InscriptionLoader) -> dict[str, list[str]]:
    """Every value each filter can take, taken from the corpus itself."""
    data = load_all()
    options: dict[str, list[str]] = {}
    for column in ("material_en", "province", "place"):
        values = data[column].dropna().astype(str).str.strip()
        options[column] = sorted({v for v in values if v and v != "?"})
    for column in MULTI_VALUE_FILTERS:
        collected: set[str] = set()
        for value in data[column]:
            collected |= _token_set(value)
        options[column] = sorted(collected)
    return options


def load_known_places(load_all: InscriptionLoader, reader: CleanedCorpusReader) -> pd.DataFrame:
    """Every place EDCS knows, carrying this corpus' monument count.

    Places that the lookup table knows but that have no inscription in the
    corpus are kept with a count of 0, which is how EDCS renders them: a small
    grey dot rather than a graduated purple one.
    """
    known = aggregate_places(load_all())

    lookup = reader.read_lookup()
    if lookup is None:
        return known

    seen = {
        (round(lat, 4), round(lon, 4))
        for lat, lon in zip(known["latitude"], known["longitude"], strict=True)
    }
    min_lat, max_lat, min_lon, max_lon = ROMAN_BOUNDS
    extra: list[dict[str, object]] = []
    for entry in (lookup.get("places") or {}).values():
        try:
            lat = float(entry.get("lat"))
            lon = float(entry.get("lon"))
        except (TypeError, ValueError):
            continue
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue
        key = (round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        extra.append(
            {
                "place": str(entry.get("ort") or "?").strip() or "?",
                "latitude": round(lat, 5),
                "longitude": round(lon, 5),
                "count": 0,
            }
        )

    if not extra:
        return known
    combined = pd.concat([known, pd.DataFrame(extra)], ignore_index=True)
    # Re-sort after the zero-count places are appended, or they would paint over
    # every graduated circle on the map.
    return combined.sort_values("count").reset_index(drop=True)


def run_query(
    term: str,
    search_mode: str,
    selections: tuple[tuple[str, tuple[str, ...]], ...],
    load_all: InscriptionLoader,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filtered rows and their per-place aggregate.

    The UI caches this on the query itself: its search controls are not wrapped
    in `st.form` - a download button cannot live inside one - so every widget
    change reruns the script, and the cache keeps those reruns from
    re-filtering half a million rows.
    """
    filters = {column: list(values) for column, values in selections}
    search_column = SEARCH_COLUMNS[search_mode]
    filtered = apply_filters(load_all(), search_column, term, filters)
    if filtered.empty:
        return filtered, pd.DataFrame(columns=["place", "latitude", "longitude", "count", "items"])

    places = aggregate_places(filtered)
    examples = _place_result_items(filtered, search_column)
    places["items"] = [
        examples.get((round(lat, 5), round(lon, 5)), [])
        for lat, lon in zip(places["latitude"], places["longitude"], strict=True)
    ]
    return filtered, places


def matched_results(filtered: pd.DataFrame, full_data: pd.DataFrame) -> pd.DataFrame:
    """Every column of the cleaned corpus for the monuments a query matched."""
    id_col = "edcs_id" if "edcs_id" in filtered.columns and "edcs_id" in full_data.columns else "record_id"
    matched_ids = filtered[id_col].dropna().astype(str).unique().tolist()

    full_data_keyed = full_data.copy()
    full_data_keyed[id_col] = full_data_keyed[id_col].astype(str)
    return full_data_keyed[full_data_keyed[id_col].isin(matched_ids)]


def results_tsv(table: pd.DataFrame) -> bytes:
    """The results table as the TSV download the UI offers."""
    return table.to_csv(sep="\t", index=False).encode("utf-8")

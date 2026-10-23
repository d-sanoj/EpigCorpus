"""Pure search, filtering and place aggregation over the inscription frame.

No I/O and no Streamlit: everything here takes a DataFrame and returns one,
so the UI, the CLI and a future HTTP API share the same search behaviour.

Search semantics: the keyword is a case-insensitive substring match (not a
regex) on the column chosen by the search mode. material_en, province and
place match exactly; category_en and language match on token overlap. Values
within one filter are alternatives; different filters must all hold.
"""

from __future__ import annotations

import pandas as pd

from app.models.inscription import MULTI_VALUE_FILTERS


def _token_set(value: object) -> set[str]:
    """Split a cell into the set of values it carries."""
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    if value is None or isinstance(value, float):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def apply_filters(
    data: pd.DataFrame,
    search_column: str,
    term: str,
    selections: dict[str, list[str]],
) -> pd.DataFrame:
    """Narrow the corpus by the selected filters and, last, the keyword."""
    result = data
    for column in ("material_en", "province", "place"):
        chosen = selections.get(column) or []
        if chosen:
            result = result[result[column].isin(chosen)]
    for column in MULTI_VALUE_FILTERS:
        chosen = set(selections.get(column) or [])
        if chosen:
            result = result[result[column].map(lambda v, want=chosen: bool(want & _token_set(v)))]
    if term.strip():
        haystack = result[search_column].fillna("").astype(str)
        result = result[haystack.str.contains(term, case=False, na=False, regex=False)]
    return result.reset_index(drop=True)


def filter_inscriptions(data: pd.DataFrame, search_column: str, term: str) -> pd.DataFrame:
    """Fast string filtering without GeoDataFrame overhead."""
    if not term.strip():
        return data.iloc[:0]  # Empty dataframe

    search_text = data[search_column].fillna("").astype(str)
    matches = search_text.str.contains(term, case=False, na=False, regex=False)
    return data[matches].reset_index(drop=True)


SEARCH_COLUMNS = {
    "Raw inscriptions": "inscription_text",
    "Interpretive Cleaned Inscriptions": "inscription_text_interpretive",
    "Conservative Cleaned Inscriptions": "inscription_text_conservative",
}


FILTER_LABELS = {
    "material_en": "Material",
    "category_en": "Category",
    "language": "Language",
    "province": "Province",
    "place": "Place",
}


def _normalize_search_mode(label: str | None) -> str:
    legacy_map = {
        "Raw inscription text": "Raw inscriptions",
        "Interpretive cleaned text": "Interpretive Cleaned Inscriptions",
        "Conservative cleaned text": "Conservative Cleaned Inscriptions",
    }
    normalized = legacy_map.get(label or "", label or "")
    valid_modes = {
        "Raw inscriptions",
        "Interpretive Cleaned Inscriptions",
        "Conservative Cleaned Inscriptions",
    }
    return normalized if normalized in valid_modes else "Raw inscriptions"


# --------------------------------------------------------------------------
# Place aggregation
#
# EDCS draws one circle per place scaled by how many monuments sit there, not
# one circle per monument. Everything below turns inscription rows into that
# per-place view.
# --------------------------------------------------------------------------


# Inscriptions listed inside a search-result popup before it is truncated.
POPUP_ITEM_LIMIT = 8


def aggregate_places(inscriptions: pd.DataFrame) -> pd.DataFrame:
    """Collapse inscription rows into one row per place with a monument count."""
    if inscriptions.empty:
        return pd.DataFrame(columns=["place", "latitude", "longitude", "count"])

    frame = inscriptions[["place", "latitude", "longitude"]].copy()
    frame["place"] = frame["place"].fillna("?").astype(str).str.strip().replace("", "?")
    # Round before grouping so float noise in the source data cannot split one
    # place across several near-identical coordinates.
    frame["latitude"] = frame["latitude"].round(5)
    frame["longitude"] = frame["longitude"].round(5)

    grouped = (
        frame.groupby(["place", "latitude", "longitude"], sort=False)
        .size()
        .reset_index(name="count")
    )
    # Ascending: callers draw in row order, so the largest, darkest circles are
    # painted last and end up on top instead of buried under the small ones.
    return grouped.sort_values("count").reset_index(drop=True)


def _place_result_items(inscriptions: pd.DataFrame, search_column: str) -> dict[tuple[float, float], list[str]]:
    """Collect a few example inscriptions per place for the result popups."""
    items: dict[tuple[float, float], list[str]] = {}
    counts: dict[tuple[float, float], int] = {}
    for row in inscriptions.itertuples(index=False):
        key = (round(float(row.latitude), 5), round(float(row.longitude), 5))
        counts[key] = counts.get(key, 0) + 1
        bucket = items.setdefault(key, [])
        if len(bucket) >= POPUP_ITEM_LIMIT:
            continue
        identifier = getattr(row, "edcs_id", None) or getattr(row, "record_id", "")
        text = getattr(row, search_column, "") or ""
        text = " ".join(str(text).split())
        if len(text) > 160:
            text = text[:157] + "…"
        bucket.append(f"{identifier} · {text}" if text else str(identifier))
    return items


# How each filter reads in a sentence: the preposition that introduces it, the
# conjunction between values, and the noun used once the list grows too long to
# spell out. Locations take "and" because the map really does show all of them;
# attributes take "or" because a single inscription matches just one of them.
FILTER_PHRASING = {
    "language": ("in", "or", "languages"),
    "material_en": ("on", "or", "materials"),
    "category_en": ("classified as", "or", "categories"),
    "province": ("from", "and", "provinces"),
    "place": ("at", "and", "places"),
}


# Beyond this many values a filter is summarised as a count rather than listed.
PHRASE_LIST_LIMIT = 3


# A subtitle longer than this is condensed so it always fits on one line.
# At 10pt across the 11.8in figure this is about three quarters of the width.
SUBTITLE_WIDTH = 132


SEARCH_MODE_PHRASING = {
    "Raw inscriptions": "raw inscription text",
    "Interpretive Cleaned Inscriptions": "interpretive cleaned text",
    "Conservative Cleaned Inscriptions": "conservative cleaned text",
}


# Used once a query has enough filters that the full wording will not fit.
SEARCH_MODE_PHRASING_SHORT = {
    "Raw inscriptions": "raw text",
    "Interpretive Cleaned Inscriptions": "interpretive text",
    "Conservative Cleaned Inscriptions": "conservative text",
}


def _join_values(values: list[str], conjunction: str) -> str:
    """`a`, `a or b`, `a, b or c`."""
    if len(values) == 1:
        return values[0]
    return f"{', '.join(values[:-1])} {conjunction} {values[-1]}"


def _clause(column: str, chosen: list[str], condensed: bool) -> str:
    """One filter as a phrase, either spelled out or summarised as a count."""
    preposition, conjunction, plural = FILTER_PHRASING[column]
    if condensed or len(chosen) > PHRASE_LIST_LIMIT:
        return f"{preposition} {len(chosen)} {plural}"
    return f"{preposition} {_join_values(chosen, conjunction)}"


def describe_query(
    term: str, search_mode: str, selections: dict[str, list[str]]
) -> tuple[str, str]:
    """Phrase a query as a title and a one-line subtitle.

    Only the filters actually in use are named. If the sentence runs past the
    single line it is given, the longest selections collapse into counts one at
    a time ("from 7 provinces"), so short selections stay spelled out for as
    long as they fit.
    """
    active = [column for column in FILTER_PHRASING if selections.get(column)]

    if term:
        title = f'Inscriptions matching "{term}"'
    elif active:
        title = "Inscriptions across the Roman Empire"
    else:
        return (
            "All known places in the EDCS corpus",
            "Every place EDCS records, sized by inscription count",
        )

    condensed: set[str] = set()

    def compose(short_mode: bool) -> str:
        parts = [_clause(column, selections[column], column in condensed) for column in active]
        sentence = ", ".join(parts)
        if term:
            phrasing = SEARCH_MODE_PHRASING_SHORT if short_mode else SEARCH_MODE_PHRASING
            searched = f"searched in {phrasing.get(search_mode, 'raw inscription text')}"
            sentence = f"{sentence} \u2014 {searched}" if sentence else searched
        return sentence[:1].upper() + sentence[1:] if sentence else ""

    subtitle = compose(short_mode=False)

    # Collapse the wordiest selections first; a single-value filter has nothing
    # to gain from collapsing, so it is never touched.
    savings = {
        column: len(_clause(column, selections[column], False))
        - len(_clause(column, selections[column], True))
        for column in active
        if len(selections[column]) > 1
    }
    for column in sorted(savings, key=savings.get, reverse=True):
        if len(subtitle) <= SUBTITLE_WIDTH:
            break
        condensed.add(column)
        subtitle = compose(short_mode=False)

    if len(subtitle) > SUBTITLE_WIDTH:
        subtitle = compose(short_mode=True)
    if len(subtitle) > SUBTITLE_WIDTH:
        # Trim on a word boundary rather than mid-word.
        subtitle = subtitle[: SUBTITLE_WIDTH - 1].rsplit(" ", 1)[0].rstrip(" ,\u2014") + "\u2026"
    return title, subtitle

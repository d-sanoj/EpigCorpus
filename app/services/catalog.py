"""The read model behind the API: the cleaned corpus, its queries and the map.

`Catalog` memoises what the Streamlit app used to keep in `st.cache_data`:
the search frame, the full corpus, filter options, short forms, known places,
the map layers and recent queries. Each is loaded once per process and shared
by every request, so nested loads (filter options and known places reading the
search frame; the GeoJSON and the PNG reading the layers) reuse one copy.

The query and summary logic is the Streamlit app's, moved here unchanged:
`run_query`, `describe_query`, and the PNG export's title, footer, colours and
file name.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from app.services import map_data, map_png, search
from app.services.ports import CleanedCorpusReader, MapLayerSource

Selections = tuple[tuple[str, tuple[str, ...]], ...]

# The search modes, in the order the map's selectbox listed them.
SEARCH_MODES = tuple(search.SEARCH_COLUMNS)
DATA_SOURCE = "Data source: Epigraphik-Datenbank Clauss / Slaby"


def selections_from(filters: dict[str, list[str]]) -> Selections:
    """The hashable form run_query takes: one (column, values) pair per filter.

    Pairs follow FILTER_LABELS order, as the map built them.
    """
    return tuple((column, tuple(filters.get(column) or ())) for column in search.FILTER_LABELS)


def is_query(term: str, selections: Selections) -> bool:
    """Any keyword or any selected filter counts as a query, as on the map."""
    return bool(term) or any(values for _, values in selections)


@dataclass(frozen=True)
class QueryResult:
    rows: pd.DataFrame
    places: pd.DataFrame
    title: str
    subtitle: str


@dataclass(frozen=True)
class PngExport:
    content: bytes
    filename: str


class _Lru:
    """A small thread-safe least-recently-used cache."""

    def __init__(self, size: int):
        self._size = size
        self._items: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key, compute: Callable[[], object]):
        with self._lock:
            if key in self._items:
                self._items.move_to_end(key)
                return self._items[key]
        value = compute()
        with self._lock:
            self._items[key] = value
            self._items.move_to_end(key)
            while len(self._items) > self._size:
                self._items.popitem(last=False)
        return value


class Catalog:
    def __init__(
        self, reader: CleanedCorpusReader, layers: MapLayerSource, query_cache_size: int = 64
    ):
        self._reader = reader
        self._layers = layers
        self._memo: dict[str, object] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._queries = _Lru(query_cache_size)
        self._rows = _Lru(query_cache_size)
        self._pngs = _Lru(16)

    # ─── memoisation ───────────────────────────────────────────────────────────

    def _once(self, key: str, compute: Callable[[], object]):
        """Compute a value once per process; concurrent callers wait for the first."""
        if key in self._memo:
            return self._memo[key]
        with self._locks_guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            if key not in self._memo:
                self._memo[key] = compute()
        return self._memo[key]

    def is_loaded(self) -> bool:
        return "inscriptions" in self._memo and "full" in self._memo

    def warm(self) -> None:
        """Load everything a first page view needs."""
        self.inscriptions()
        self.full_by_id()
        self.filter_options()
        self.short_forms()
        self.known_places()

    def fingerprint(self) -> str:
        """Identifies the corpus snapshot; cursors are bound to it."""
        return self._once("fingerprint", self._reader.fingerprint)

    # ─── corpus ────────────────────────────────────────────────────────────────

    def inscriptions(self) -> pd.DataFrame:
        """The search frame: REQUIRED_COLS, rows with coordinates inside ROMAN_BOUNDS."""
        return self._once("inscriptions", lambda: search.load_inscriptions(self._reader))

    def full(self) -> pd.DataFrame:
        """Every row and column of the cleaned corpus."""
        return self._once("full", lambda: search.load_full_cleaned_data(self._reader))

    def full_by_id(self) -> pd.DataFrame:
        return self._once("full_by_id", lambda: self.full().set_index("record_id", drop=False))

    def inscription(self, record_id: str) -> pd.Series | None:
        table = self.full_by_id()
        if record_id not in table.index:
            return None
        return table.loc[record_id]

    def lookup(self) -> dict | None:
        return self._once("lookup", self._reader.read_lookup)

    def short_forms(self) -> dict[str, dict[str, str]]:
        return self._once("short_forms", lambda: search.short_forms(self._reader))

    def filter_options(self) -> dict[str, list[str]]:
        return self._once("filter_options", lambda: search.filter_options(self.inscriptions))

    def known_places(self) -> pd.DataFrame:
        return self._once(
            "known_places", lambda: search.load_known_places(self.inscriptions, self._reader)
        )

    # ─── queries ───────────────────────────────────────────────────────────────

    def run_query(self, term: str, search_mode: str, selections: Selections) -> QueryResult:
        """Matching rows, their per-place aggregate and the query in words. Cached."""

        def compute() -> QueryResult:
            rows, places = search.run_query(term, search_mode, selections, self.inscriptions)
            title, subtitle = search.describe_query(
                term, search_mode, {column: list(values) for column, values in selections}
            )
            return QueryResult(rows, places, title, subtitle)

        return self._queries.get((term, search_mode, selections), compute)

    def rows(self, term: str, search_mode: str, selections: Selections) -> pd.DataFrame:
        """Only the matching rows, without the per-place aggregate. Cached.

        The same rows, in the same order, as run_query returns: both are
        apply_filters over the search frame.
        """
        column = search.SEARCH_COLUMNS[search_mode]
        return self._rows.get(
            (term, search_mode, selections),
            lambda: search.apply_filters(
                self.inscriptions(), column, term,
                {name: list(values) for name, values in selections},
            ),
        )

    def results_table(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Every column for the monuments a query matched (the TSV download)."""
        return search.matched_results(rows, self.full())

    # ─── map layers ────────────────────────────────────────────────────────────

    def provinces(self):
        return self._once("provinces", self._layers.load_provinces)

    def roads(self):
        return self._once("roads", self._layers.load_roads)

    def cities(self):
        return self._once("cities", self._layers.load_cities)

    def provinces_geojson(self) -> dict:
        return self._once("provinces_geojson", lambda: map_data.provinces_geojson(self.provinces))

    def roads_geojson(self) -> dict:
        return self._once("roads_geojson", lambda: map_data.roads_geojson(self.roads))

    def cities_points(self) -> list[list]:
        return self._once("cities_points", lambda: map_data.cities_points(self.cities))

    # ─── PNG export ────────────────────────────────────────────────────────────

    def export_png(self, term: str, search_mode: str, selections: Selections) -> PngExport:
        """The map's "Download as PNG": the query's results, or every known place.

        Title, subtitle, footer, colours and file name are composed exactly as
        the Streamlit app composed them.
        """

        def compute() -> PngExport:
            filters = {column: list(values) for column, values in selections}
            if is_query(term, selections):
                result = self.run_query(term, search_mode, selections)
                png_places = result.places
                heading, subtitle = result.title, result.subtitle
                footer_lines = (
                    f"Search term: {term}" if term else "Search term: (none)",
                    f"Results: {len(result.rows):,} across {len(png_places):,} places"
                    f" | Search mode: {search_mode}",
                    DATA_SOURCE,
                )
                png_colors = (
                    map_png.SEARCH_COLOR, map_png.PRINT_SEARCH_EDGE, "matching inscriptions"
                )
                file_stem = term.replace(" ", "_").lower() or "filtered"
            else:
                png_places = self.known_places()
                heading, subtitle = search.describe_query("", search_mode, filters)
                footer_lines = (
                    f"All known places: {len(png_places):,}",
                    f"Inscriptions plotted: {int(png_places['count'].sum()):,}",
                    DATA_SOURCE,
                )
                png_colors = (map_png.KNOWN_COLOR, map_png.PRINT_KNOWN_EDGE, "inscriptions")
                file_stem = "all_known_places"
            content = map_png.build_png_bytes(
                png_places[["place", "latitude", "longitude", "count"]],
                heading,
                subtitle,
                footer_lines,
                *png_colors,
                load_provinces=self.provinces,
                load_roads=self.roads,
                load_cities=self.cities,
            )
            return PngExport(content, f"edcs_map_{file_stem}.png")

        return self._pngs.get((term, search_mode, selections), compute)

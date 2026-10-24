"""The publication-style PNG export of the map, rendered with matplotlib.

Moved verbatim from the former Streamlit app. The PNG uses the web map's dark
palette and symbology; the colours below are mirrored in the web UI's
frontend/src/map/.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from io import BytesIO

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")  # no display: the PNG is rendered on a server

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

PNG_CITATION = (
    "Sanoj Doddapaneni. (2026). EpigCorpus (Version 1.0) "
    "[Computer software]. GitHub. https://github.com/d-sanoj/EpigCorpus"
)

# The tutorial's two-colour scheme: teal for one state, tangerine for the other,
# with magnitude carried by circle size rather than by hue.
KNOWN_COLOR = "#0A8A9F"   # teal
SEARCH_COLOR = "#E37222"  # tangerine
# The PNG prints on white paper, so the two colours that only work on the dark
# basemap get print-safe counterparts.
PRINT_SEARCH_EDGE = "#ffd9b8"
PRINT_KNOWN_EDGE = "#b8f1fb"
MAP_BACKGROUND = "#0b0f17"

# The tutorial sizes circles straight off the value (radius = count / 20). That
# is fine for a few hundred bike stations spanning tens of trips; here a single
# place can hold thousands of inscriptions, so the same idea is kept - size is
# continuous in the count, no binning - on a log scale that stays legible.
RADIUS_BASE = 1.2
RADIUS_SCALE = 1.9
EMPTY_RADIUS = 1.5
# Representative counts drawn in the size legend.
LEGEND_STOPS = [1, 10, 100, 1000]


def radius_for_count(count: int) -> float:
    """Circle radius at the default zoom, continuous in the inscription count."""
    if count <= 0:
        return EMPTY_RADIUS
    return RADIUS_BASE + RADIUS_SCALE * math.log10(count + 1)


def _print_province_palette() -> list[str]:
    """Province fills for the dark export.

    Deliberately cool and muted: the markers are warm and the city rings violet,
    so the provinces stay in a blue-green band, light enough to separate
    neighbours but never bright enough to compete with the data.
    """
    return [
        "#4a6fa5",
        "#3f8f7f",
        "#5b6bab",
        "#4f8f5a",
        "#3b7f9c",
        "#6b6fa8",
        "#4d8f8a",
        "#5a7fb0",
    ]


# Export-only colours, spaced in hue so no two layers read as the same thing:
# violet city rings, muted slate roads, warm markers.
PRINT_CITY_COLOR = "#c084fc"
PRINT_ROAD_COLOR = "#64748b"


# The layers a PNG can draw, as the web map's layer menu names them.
LAYERS = frozenset({"provinces", "roads", "cities", "known", "results"})


def build_png_bytes(
    result_places: pd.DataFrame,
    heading: str,
    subtitle: str,
    footer_lines: tuple[str, ...],
    marker_color: str = SEARCH_COLOR,
    marker_edge: str = PRINT_SEARCH_EDGE,
    marker_label: str = "matching inscriptions",
    *,
    load_provinces: Callable[[], gpd.GeoDataFrame],
    load_roads: Callable[[], gpd.GeoDataFrame],
    load_cities: Callable[[], gpd.GeoDataFrame],
    show_provinces: bool = True,
    show_roads: bool = True,
    show_cities: bool = True,
    background_places: pd.DataFrame | None = None,
) -> bytes:
    """Publication-style PNG using the same symbology as the web map.

    `heading`, `subtitle` and `footer_lines` are built by the caller because a
    query can now be a keyword, a set of filters, or both. The subtitle names
    every active filter directly under the title. The layer loaders are
    passed in so callers can share their cached layers with the web map.

    The `show_*` flags and `background_places` (known places drawn in teal
    under a query's results) follow the layers switched on in the web map.
    The frame is always the empire's, whichever layers are drawn.
    """
    subtitle_lines = [subtitle] if subtitle.strip() else []
    provinces = load_provinces().reset_index(drop=True).copy()
    roads = load_roads()
    cities = load_cities()

    provinces_proj = provinces.to_crs(epsg=3857)
    roads_proj = roads.to_crs(epsg=3857)
    cities_proj = cities.to_crs(epsg=3857)

    province_colors = _print_province_palette()
    provinces_proj["_color"] = [
        province_colors[i % len(province_colors)] for i in range(len(provinces_proj))
    ]

    # The export carries the same palette as the interactive map rather than
    # sitting on white paper.
    fig, ax = plt.subplots(figsize=(11.8, 8.6), dpi=180)
    fig.patch.set_facecolor(MAP_BACKGROUND)
    ax.set_facecolor(MAP_BACKGROUND)
    # The footer grows a line per active filter, so the map area has to yield
    # room for it rather than being overdrawn.
    footer_bottom = 0.05
    map_bottom = footer_bottom + 0.021 * len(footer_lines) + 0.02
    map_top = 0.88 - 0.022 * len(subtitle_lines)
    fig.subplots_adjust(left=0.06, right=0.94, top=map_top, bottom=map_bottom)

    if show_provinces:
        provinces_proj.plot(
            ax=ax,
            color=provinces_proj["_color"],
            edgecolor="#cbd5e1",
            linewidth=0.4,
            alpha=0.3,
            zorder=1,
        )
    if show_roads:
        roads_proj.plot(ax=ax, color=PRINT_ROAD_COLOR, linewidth=0.35, alpha=0.75, zorder=2)
    if show_cities:
        cities_proj.plot(
            ax=ax,
            color="none",
            markersize=4,
            edgecolor=PRINT_CITY_COLOR,
            linewidth=0.5,
            alpha=0.85,
            zorder=3,
        )

    if background_places is not None and not background_places.empty:
        known_geo = gpd.GeoDataFrame(
            background_places.copy(),
            geometry=gpd.points_from_xy(
                background_places["longitude"], background_places["latitude"]
            ),
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        # Context under the results: smaller and fainter, so the results read.
        known_sizes = [(radius_for_count(int(c)) * 1.1) ** 2 / 4 for c in known_geo["count"]]
        known_geo.plot(
            ax=ax,
            color=KNOWN_COLOR,
            markersize=known_sizes,
            alpha=0.35,
            edgecolor=PRINT_KNOWN_EDGE,
            linewidth=0.25,
            zorder=3.5,
        )

    if not result_places.empty:
        places_geo = gpd.GeoDataFrame(
            result_places.copy(),
            geometry=gpd.points_from_xy(result_places["longitude"], result_places["latitude"]),
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        # Sorted ascending by aggregate_places, so the largest circles are drawn
        # last and stay on top. Size is continuous in the count, as on the web map.
        sizes = [(radius_for_count(int(c)) * 1.9) ** 2 / 4 for c in places_geo["count"]]
        places_geo.plot(
            ax=ax,
            color=marker_color,
            markersize=sizes,
            alpha=0.92,
            edgecolor=marker_edge,
            linewidth=0.35,
            zorder=4,
        )

    minx, miny, maxx, maxy = provinces_proj.total_bounds
    x_pad = (maxx - minx) * 0.025
    y_pad = (maxy - miny) * 0.04
    ax.set_xlim(minx - x_pad, maxx + x_pad)
    ax.set_ylim(miny - y_pad, maxy + y_pad)
    ax.set_axis_off()
    ax.set_aspect("equal", adjustable="box")
    ax.set_anchor("C")

    fig.suptitle(heading, fontsize=17, fontfamily="serif", y=0.955, color="#e5e7eb")
    if subtitle_lines:
        fig.text(
            0.5,
            0.925,
            "\n".join(subtitle_lines),
            ha="center",
            va="top",
            fontsize=10,
            color="#9fb3c8",
        )

    legend_handles = []
    if show_roads:
        legend_handles.append(Line2D([0], [0], color=PRINT_ROAD_COLOR, lw=1.4, label="Roads"))
    if show_cities:
        legend_handles.append(
            Line2D(
                [0], [0], marker="o", color="none", markerfacecolor="none",
                markeredgecolor=PRINT_CITY_COLOR, markersize=5, label="Cities",
            )
        )
    if background_places is not None and not background_places.empty:
        legend_handles.append(
            Line2D(
                [0], [0], marker="o", color="none", markerfacecolor=KNOWN_COLOR,
                markeredgecolor=PRINT_KNOWN_EDGE, markersize=5, alpha=0.7,
                label="Known places",
            )
        )
    legend_handles += [
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=marker_color,
            markeredgecolor=marker_edge,
            markersize=max(4.0, radius_for_count(stop) * 1.1),
            label=f"{stop:,} {marker_label}",
        )
        for stop in LEGEND_STOPS
        if not result_places.empty
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(0.965, 0.965),
        frameon=True,
        fontsize=9,
        borderaxespad=0.0,
        labelcolor="#e5e7eb",
    )
    legend.get_frame().set_facecolor("#111827")
    legend.get_frame().set_edgecolor("#94a3b8")
    legend.get_frame().set_alpha(0.92)

    fig.text(
        0.5,
        footer_bottom,
        "\n".join(footer_lines),
        ha="center",
        va="bottom",
        fontsize=8,
        color="#cbd5e1",
    )
    fig.text(
        0.5,
        0.018,
        PNG_CITATION,
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#94a3b8",
    )

    buffer = BytesIO()
    fig.savefig(buffer, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()

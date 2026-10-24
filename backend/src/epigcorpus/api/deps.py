"""What the routes depend on: the wired services and the shared query parameters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Annotated

from fastapi import Depends, Query, Request

from epigcorpus.services.catalog import Catalog, CatalogSlot, Selections, selections_from
from epigcorpus.services.pipeline import PipelineRuns
from epigcorpus.services.ports import ExportSource


@dataclass(frozen=True)
class ApiDeps:
    catalogs: CatalogSlot
    exports: ExportSource
    runs: PipelineRuns
    spec_path: Path
    static_dir: Path | None = None
    # CARTO basemap API key (EPIGCORPUS_CARTO_KEY); None when not configured.
    carto_key: str | None = None


def get_deps(request: Request) -> ApiDeps:
    return request.app.state.deps


Deps = Annotated[ApiDeps, Depends(get_deps)]


def get_catalog(deps: Deps) -> Catalog:
    """The catalog for one request: fixed when the request starts, so a reload
    swapping in a new corpus mid-request cannot mix rows from both."""
    return deps.catalogs.current


CatalogDep = Annotated[Catalog, Depends(get_catalog)]


class SearchIn(StrEnum):
    raw = "raw"
    interpretive = "interpretive"
    conservative = "conservative"


# search_in -> the search-mode label the services (and the old map) use.
SEARCH_MODES = {
    SearchIn.raw: "Raw inscriptions",
    SearchIn.interpretive: "Interpretive Cleaned Inscriptions",
    SearchIn.conservative: "Conservative Cleaned Inscriptions",
}

Limit = Annotated[int, Query(ge=1, le=500, description="Page size.")]
PlacesLimit = Annotated[int, Query(ge=1, le=25000, description="Page size (places are small).")]
Cursor = Annotated[str | None, Query(description="Opaque cursor from page.next_cursor.")]


@dataclass(frozen=True)
class Filters:
    material: list[str]
    category: list[str]
    language: list[str]
    province: list[str]
    place: list[str]

    def selections(self) -> Selections:
        """API names -> the corpus columns the search services filter on."""
        return selections_from(
            {
                "material_en": self.material,
                "category_en": self.category,
                "language": self.language,
                "province": self.province,
                "place": self.place,
            }
        )

    def any(self) -> bool:
        return any((self.material, self.category, self.language, self.province, self.place))


def get_filters(
    material: Annotated[list[str], Query()] = [],  # noqa: B006 - FastAPI copies defaults
    category: Annotated[list[str], Query()] = [],  # noqa: B006
    language: Annotated[list[str], Query()] = [],  # noqa: B006
    province: Annotated[list[str], Query()] = [],  # noqa: B006
    place: Annotated[list[str], Query()] = [],  # noqa: B006
) -> Filters:
    return Filters(material, category, language, province, place)


FilterParams = Annotated[Filters, Depends(get_filters)]
QueryText = Annotated[str, Query(min_length=1, max_length=200)]
OptionalQueryText = Annotated[str | None, Query(min_length=1, max_length=200)]
SearchInParam = Annotated[SearchIn, Query()]

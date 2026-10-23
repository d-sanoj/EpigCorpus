# Base-map layers

Third-party geospatial layers used to draw the EpigCorpus map: Roman provinces,
Roman roads, and Roman cities.

These files are **vendored** — committed to the repository rather than fetched
at runtime — so the map renders offline and reproducibly, and no figure depends
on an external host staying online. Earlier versions of the map downloaded them
on every cold start from a third-party GitHub mirror; that dependency is gone.

They are **not** EpigCorpus data and are **not** covered by the project's MIT
licence, which applies to code only. Each carries its own terms.

---

## Layers

| Layer | Files | Features | Source |
|---|---|---|---|
| Roman provinces, AD 117 | `roman_empire_ad_117/` | 111 polygons | Ancient World Mapping Center (AWMC), UNC Chapel Hill |
| Roman roads | `ba_roads/` | 3,166 linestrings | AWMC, digitised from the *Barrington Atlas* |
| Cities | `Hanson2016_Cities_OxREP.csv` | 1,388 points | Hanson 2016, Oxford Roman Economy Project |

Each shapefile ships the full component set (`.shp`, `.shx`, `.dbf`, `.prj`,
plus `.sbn`/`.sbx` indexes). All are read as EPSG:4326.

The cities CSV is **ISO-8859-1**, not UTF-8, and its coordinate columns are
named `Longitude (X)` and `Latitude (Y)`.

---

## Licences and attribution

**AWMC — provinces and roads.** Ancient World Mapping Center, University of
North Carolina at Chapel Hill. <http://awmc.unc.edu> · current distribution
<https://github.com/AWMC/geodata>, licensed **ODbL-1.0**.

> AWMC has since reorganised its repository, and the directory layout and layer
> names vendored here correspond to an earlier release — the current repository
> exposes `roman_empire_ce_117_extent` and a GeoJSON roads file instead. That
> mismatch is part of why these are pinned in-tree rather than re-fetched: the
> upstream paths this project was built against no longer resolve.

**Hanson 2016 — cities.** Hanson, J. W. (2016). *An Urban Geography of the
Roman World, 100 BC to AD 300*. Archaeopress. Published via the Oxford Roman
Economy Project, <http://oxrep.classics.ox.ac.uk/databases/cities/> ·
DOI [10.5287/bodleian:eqapevAn8](https://doi.org/10.5287/bodleian:eqapevAn8).
Cite Hanson 2016 when reusing.

---

## Usage

The map loads these through `map_layer_path()` in `src/edcs_streamlit_map.py`,
which raises an actionable error if a file is missing. Restore them with:

```bash
git checkout -- data/map_layers
```

---

## Cartographic caveats

Anything published from these layers should state the following.

- **The province layer is a fixed AD 117 frame.** The corpus spans roughly
  500 BC to AD 700, so for most inscriptions these boundaries are a *reference
  frame, not a contemporaneous basemap*. A date filter is planned (T13).
- **PNG export currently uses EPSG:3857.** Area distortion grows with latitude
  across a map spanning ~24–56°N, so apparent density is not comparable between
  north and south. An equal-area projection is planned (T12).
- **Province fill colours are assigned by row index**, so adjacent provinces can
  share a colour, and the current palette is not colour-blind safe (T16).
- **Points are findspot-level**, so many inscriptions overlap exactly. The map
  shows presence, not density (T14).

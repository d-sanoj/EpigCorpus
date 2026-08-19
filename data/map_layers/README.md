# Base-map layers

Third-party geospatial layers used to draw the EpigCorpus map. They are
**vendored** here — committed to the repository rather than downloaded at
runtime — so the map renders offline and reproducibly, and so no figure depends
on an external repository staying online.

These files are **not** EpigCorpus data and are **not** covered by the project's
MIT licence, which applies to code only. Each carries its own terms.

| Layer | Files | Source | Terms |
|---|---|---|---|
| Roman provinces, AD 117 | `roman_empire_ad_117/*` | Ancient World Mapping Center (AWMC), University of North Carolina at Chapel Hill — political shading shapefiles | ODbL-1.0 (per the AWMC `geodata` repository) |
| Roman roads (Barrington Atlas) | `ba_roads/*` | Ancient World Mapping Center (AWMC), UNC Chapel Hill | ODbL-1.0 |
| Cities | `Hanson2016_Cities_OxREP.csv` | J. W. Hanson (2016), *Cities Database*, Oxford Roman Economy Project (OXREP) | OXREP terms — cite Hanson 2016 |

## Attribution

- **AWMC** — Ancient World Mapping Center, <http://awmc.unc.edu>. Current
  distribution: <https://github.com/AWMC/geodata>. Note that AWMC has since
  reorganised its repository; the `roman_empire_ad_117` and `ba_roads` layouts
  vendored here correspond to an earlier release, which is part of why they are
  pinned in-tree rather than re-fetched.
- **Hanson 2016** — Hanson, J. W. (2016). *An Urban Geography of the Roman
  World, 100 BC to AD 300*. Archaeopress. Cities database published via the
  Oxford Roman Economy Project, <http://oxrep.classics.ox.ac.uk>.

## Caveat on the province layer

`roman_empire_ad_117` is a snapshot of the empire at a single moment. The corpus
it is drawn under spans roughly 500 BC – AD 700, so the provincial boundaries are
a **reference frame, not a contemporaneous basemap** for most inscriptions. Any
published figure should say so.

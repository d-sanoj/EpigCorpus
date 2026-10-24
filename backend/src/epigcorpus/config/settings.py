"""Settings: the EDCS endpoints and HTTP behaviour, and fixed spatial bounds.

Paths live in epigcorpus.config.paths. Presentation (colours, styles, the basemap)
lives with the web UI in frontend/.
"""

# ─── EDCS API ─────────────────────────────────────────────────────────────────
EDCS_BASE = "https://edcs.hist.uzh.ch"
INDEX_URL = f"{EDCS_BASE}/data/indexes/searchable.json"
LOOKUPS_URL = f"{EDCS_BASE}/data/indexes/lookups.json"
PLACES_URL = f"{EDCS_BASE}/data/indexes/places.json"
LANGUAGES_URL = f"{EDCS_BASE}/data/indexes/languages.json"
SOURCES_URL = f"{EDCS_BASE}/data/indexes/sources.json"
MONUMENT_URL = f"{EDCS_BASE}/data/monument/{{shard}}/{{id8}}.json"
IMAGE_URL = f"{EDCS_BASE}/bilder/{{prefix}}/{{filename}}"

EPIGCORPUS_VERSION = "0.2.0"

# An identifying User-Agent. Verified 2026-08-19: the static /data/ endpoints
# serve this without complaint (300/300 and 600/600 in survey runs), so there is
# no access reason to spoof a browser. The old scraper's forged Firefox UA,
# Referer and X-Requested-With bought nothing — six header variants all got the
# same 403 from the withdrawn /api/query.
HEADERS = {
    "User-Agent": (
        f"EpigCorpus/{EPIGCORPUS_VERSION} "
        "(+https://github.com/d-sanoj/EpigCorpus; contact.sanoj.d@gmail.com)"
    ),
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}

DEFAULT_WORKERS = 16
CHUNK_SIZE = 2000
MAX_RETRIES = 4
BACKOFF_BASE = 2.0


# ─── Spatial bounds ───────────────────────────────────────────────────────────
# Roman Empire bounds. Wide enough for the whole empire as EDCS draws it:
# Britannia in the north-west through Aegyptus and Mesopotamia in the south-east.
ROMAN_BOUNDS = (20.0, 62.0, -12.0, 50.0)  # (min_lat, max_lat, min_lon, max_lon)

# The web map's projection: Lambert azimuthal equal-area centred on the
# Mediterranean, so areas are true across the empire (no Mercator stretch).
# The frontend's map uses the same centre (frontend/src/features/map/crs.ts).
MAP_CENTER = (40.0, 15.0)  # (lat, lon)
# Land is drawn out to this angular distance from the centre (about 11,000 km),
# far past any view the map allows; beyond it the projection degenerates.
LAND_RADIUS_DEGREES = 100.0

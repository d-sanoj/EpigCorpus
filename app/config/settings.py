"""Settings: the EDCS endpoints and HTTP behaviour, and fixed spatial bounds.

Paths live in app.config.paths. Presentation (colours, styles, templates)
stays with the UI.
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


# ─── Basemap tiles and Leaflet ────────────────────────────────────────────────
# CARTO basemap. "light_all" carries place labels; "light_nolabels" is the same
# cartography without them, which is how the "Place names" toggle works.
# CARTO dark basemap ("dark_matter"), the backdrop used throughout the folium
# cartography tutorial this styling follows:
# https://nbviewer.org/github/vincentropy/python_cartography_tutorial/blob/master/part1_basic_folium_maps.ipynb
# "dark_all" carries place labels, "dark_nolabels" is the same cartography
# without them, which is how the "Place names" toggle works.
CARTO_LABELLED = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
CARTO_PLAIN = "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
CARTO_ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
    '&copy; <a href="https://carto.com/attributions">CARTO</a>'
)
LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
LEAFLET_JS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"

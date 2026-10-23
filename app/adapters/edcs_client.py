"""HTTP client for the EDCS static-file API (see docs/EDCS_API.md)."""

import json
import os
import time

import requests

from app.adapters.console import log
from app.config import paths
from app.config.settings import (
    BACKOFF_BASE,
    EPIGCORPUS_VERSION,
    HEADERS,
    INDEX_URL,
    LANGUAGES_URL,
    LOOKUPS_URL,
    MAX_RETRIES,
    PLACES_URL,
    SOURCES_URL,
)
from app.core.edcs_parsing import now_iso

# Defined in app.config.paths; the client works on os.path strings.
INDEX_CACHE = os.fspath(paths.INDEX_CACHE_FILE)


# ─── HTTP ─────────────────────────────────────────────────────────────────────


def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount("https://", adapter)
    return session


def get_json(session, url, timeout=300):
    """GET with exponential backoff. Honours Retry-After when the server sends it."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                wait = float(response.headers.get("Retry-After", BACKOFF_BASE**attempt))
                time.sleep(wait)
                last_error = f"HTTP {response.status_code}"
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE**attempt)
    raise RuntimeError(f"{url} failed after {MAX_RETRIES} attempts — {last_error}")


# ─── LOOKUPS ──────────────────────────────────────────────────────────────────


def build_lookup(session):
    """Fetch and flatten every code table into a single lookup dict."""
    log("[+] Fetching lookup tables...")
    lookups = get_json(session, LOOKUPS_URL)["d"]
    languages = get_json(session, LANGUAGES_URL)["d"]
    places_raw = get_json(session, PLACES_URL)
    sources_raw = get_json(session, SOURCES_URL)["d"]

    # materials: index = id, entry = [token, {de,en,...}]; slot 0 is null
    materials = {}
    for idx, entry in enumerate(lookups.get("materials", [])):
        if entry:
            materials[idx] = {"token": entry[0], "en": entry[1].get("en", entry[0])}

    # categories: [bit, token, {de,en,...}], keyed by bit
    categories = {}
    for entry in lookups.get("categories", []):
        if isinstance(entry, list) and len(entry) >= 3:
            categories[entry[0]] = {"token": entry[1], "en": entry[2].get("en", entry[1])}

    provinces = lookups.get("provinces", [])

    langs = {}
    for entry in languages:
        if isinstance(entry, dict):
            langs[entry.get("sprache_id")] = {
                "en": entry.get("names", {}).get("en", ""),
                "code": (entry.get("kuerzel") or "").strip(),
            }

    # places: [geo_id, ort, province_index, [lat, lon]]
    # NOTE: coord is [latitude, longitude] here. The OLD API returned
    # [longitude, latitude]. Getting this backwards transposes the whole corpus.
    places = {}
    for row in places_raw.get("d", []):
        coord = row[3] if len(row) > 3 else None
        province_idx = row[2] if len(row) > 2 else None
        places[row[0]] = {
            "ort": row[1] or "",
            "province": (
                provinces[province_idx]
                if isinstance(province_idx, int) and 0 <= province_idx < len(provinces)
                else ""
            ),
            "lat": coord[0] if coord and len(coord) == 2 else "",
            "lon": coord[1] if coord and len(coord) == 2 else "",
        }

    # sources: index = id, entry = [token, description, count]
    sources = {}
    for idx, entry in enumerate(sources_raw):
        if entry:
            sources[idx] = entry[0]

    lookup = {
        "materials": materials,
        "categories": categories,
        "provinces": provinces,
        "languages": langs,
        "places": places,
        "sources": sources,
        "_meta": {
            "retrieved_at": now_iso(),
            "epigcorpus_version": EPIGCORPUS_VERSION,
        },
    }
    log(
        f"    materials={len(materials)} categories={len(categories)} "
        f"provinces={len(provinces)} languages={len(langs)} "
        f"places={len(places)} sources={len(sources)}"
    )
    return lookup


# ─── INDEX ────────────────────────────────────────────────────────────────────


def fetch_index(session, use_cache=True):
    """Fetch the full corpus index. One request, ~19 MB, no pagination."""
    if use_cache and os.path.exists(INDEX_CACHE):
        with open(INDEX_CACHE, encoding="utf-8") as handle:
            cached = json.load(handle)
        log(f"[+] Using cached index: {len(cached['ids']):,} monuments "
            f"(fetched {cached['fetched_at']})")
        return cached["ids"], cached["fetched_at"]

    log("[+] Fetching corpus index (~19 MB)...")
    started = time.time()
    payload = get_json(session, INDEX_URL, timeout=600)
    ids = [row[0] for row in payload.get("d", [])]
    fetched_at = now_iso()
    log(f"[+] Index: {len(ids):,} monuments in {time.time() - started:.1f}s")

    with open(INDEX_CACHE, "w", encoding="utf-8") as handle:
        json.dump({"fetched_at": fetched_at, "count": len(ids), "ids": ids}, handle)
    return ids, fetched_at

"""The corpus as files, for a deployment that has no server behind it.

The hosted build (frontend built with VITE_DATA_MODE=static) cannot run FastAPI,
so everything the API computes per request splits in two:

    precomputed here          facets, known places, the map layers, the spec
    computed in the browser   the query itself: filters, keyword, per-place
                              aggregation, paging, the results table

The precomputed half is produced by the same Catalog the API answers from, so
the two cannot drift. The container's rows are the API's search frame, in its
order: the same rows the server would search, with the same missing-value
rules.

File names are fixed, and `manifest.json` carries the corpus fingerprint plus
the byte length of each text blob. The browser fetches with `no-cache`, so it
revalidates rather than trusts a stale copy, and it checks the fingerprint and
the lengths before using what it gets: artifacts from two snapshots can never
be mixed. The manifest is written last, so a half-written build is visible as
one.

Usage (from backend/):

    uv run python -m epigcorpus.export.static_site --out ../frontend/public/data

The container layout is documented in the frontend that reads it
(frontend/src/api/static/container.ts). Keep the two in step: the format
version is in the header, and the browser refuses anything it does not know.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import struct
import sys
from collections.abc import Callable, Iterable
from pathlib import Path

import numpy as np
import pandas as pd

from epigcorpus.adapters import corpus_files, map_layers
from epigcorpus.api.deps import SearchIn
from epigcorpus.api.routes.lookups import FACET_NAMES
from epigcorpus.api.serialize import place_collection
from epigcorpus.config.paths import API_SPEC
from epigcorpus.core.search import _token_set
from epigcorpus.services.catalog import Catalog

FORMAT = "epigcorpus-static-1"

# The search modes the UI offers, and the corpus column each searches.
TEXT_COLUMNS = {
    SearchIn.raw.value: "inscription_text",
    SearchIn.conservative.value: "inscription_text_conservative",
    SearchIn.interpretive.value: "inscription_text_interpretive",
}

# Missing years, as an i32. The API answers null for a year the corpus lacks;
# the browser maps this back to null. Real years never come near it.
MISSING_YEAR = -(2**31)

RECORD_ID = re.compile(r"^EDCS-(\d{8})-(\d+)$")

NP_DTYPES = {"u16": np.uint16, "u32": np.uint32, "i32": np.int32, "f64": np.float64}


def _missing(value: object) -> bool:
    """An empty cell, as the API's serializer reads one."""
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, (list, tuple, set)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _text(value: object) -> str:
    return "" if _missing(value) else str(value)


def _codes(values: Iterable[object]) -> tuple[list[str], np.ndarray]:
    """Dictionary-encode a single-valued column, with "" at index 0.

    Index 0 is the missing value: nothing selectable maps to it, so a row
    without a province is simply never matched by a province filter, exactly
    as `isin` behaves on the server.
    """
    normalized = [_text(value).strip() for value in values]
    present = sorted({value for value in normalized if value})
    lookup = {value: position + 1 for position, value in enumerate(present)}
    codes = np.fromiter(
        (lookup.get(value, 0) for value in normalized), dtype=np.uint32, count=len(normalized)
    )
    return ["", *present], codes


def _place_codes(values: Iterable[object]) -> tuple[list[str], np.ndarray]:
    """Places, with "?" at index 0 for a row whose findspot the corpus lacks.

    `aggregate_places` normalizes an empty place to "?" before grouping, so
    the map's own naming is the one stored here. The filter options exclude
    "?", which is why "?" stays unselectable.
    """
    normalized = [_text(value).strip() or "?" for value in values]
    present = sorted({value for value in normalized if value != "?"})
    lookup = {value: position + 1 for position, value in enumerate(present)}
    codes = np.fromiter(
        (lookup.get(value, 0) for value in normalized), dtype=np.uint32, count=len(normalized)
    )
    return ["?", *present], codes


def _coord_codes(
    latitudes: pd.Series, longitudes: pd.Series
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Unique 5-decimal coordinate pairs, as the map groups them.

    Rounding before de-duplicating is what `aggregate_places` does, so float
    noise in the source data cannot split one place across two circles.
    """
    pairs = [
        (round(float(lat), 5), round(float(lon), 5))
        for lat, lon in zip(latitudes, longitudes, strict=True)
    ]
    ordered = sorted(set(pairs))
    lookup = {pair: position for position, pair in enumerate(ordered)}
    codes = np.fromiter((lookup[pair] for pair in pairs), dtype=np.uint32, count=len(pairs))
    return (
        codes,
        np.array([pair[0] for pair in ordered], dtype=np.float64),
        np.array([pair[1] for pair in ordered], dtype=np.float64),
    )


def _fits_u16(count: int, what: str) -> None:
    if count > np.iinfo(np.uint16).max:
        raise ValueError(f"the container cannot index {count} {what}")


def _csr(values: Iterable[object]) -> tuple[list[str], np.ndarray, np.ndarray]:
    """Token sets (languages, categories) as one flat list plus row offsets."""
    token_sets = [sorted(_token_set(value)) for value in values]
    dictionary = sorted({token for tokens in token_sets for token in tokens})
    _fits_u16(len(dictionary), "distinct values")
    lookup = {token: position for position, token in enumerate(dictionary)}
    offsets = np.zeros(len(token_sets) + 1, dtype=np.uint32)
    flat: list[int] = []
    for position, tokens in enumerate(token_sets):
        flat.extend(lookup[token] for token in tokens)
        offsets[position + 1] = len(flat)
    return dictionary, offsets, np.array(flat, dtype=np.uint16)


def _years(column: pd.Series) -> np.ndarray:
    years = np.full(len(column), MISSING_YEAR, dtype=np.int32)
    for position, value in enumerate(column):
        if not _missing(value):
            years[position] = int(value)
    return years


def _record_parts(record_ids: pd.Series, edcs_ids: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """EDCS-00000001-0 as its number and its inscription index within the monument."""
    numbers = np.zeros(len(record_ids), dtype=np.uint32)
    indexes = np.zeros(len(record_ids), dtype=np.uint16)
    for position, (record_id, edcs_id) in enumerate(
        zip(record_ids, edcs_ids, strict=True)
    ):
        if _missing(record_id):
            continue
        match = RECORD_ID.match(str(record_id))
        if match is None:
            raise ValueError(f"record_id is not EDCS-########-#: {record_id!r}")
        number, index = match.groups()
        numbers[position] = int(number)
        if int(index) > np.iinfo(np.uint16).max:
            raise ValueError(f"inscription index out of range: {record_id!r}")
        indexes[position] = int(index)
        if str(edcs_id) != f"EDCS-{number}":
            raise ValueError(f"edcs_id and record_id disagree: {edcs_id!r} {record_id!r}")
    return numbers, indexes


def _text_blob(texts: Iterable[object]) -> tuple[bytes, np.ndarray]:
    """One mode's texts, concatenated, with where each row starts and ends.

    Missing text becomes zero bytes, which the browser reads back as null, as
    the API serializes it.
    """
    values = list(texts)
    chunks: list[bytes] = []
    offsets = np.zeros(len(values) + 1, dtype=np.uint32)
    total = 0
    for position, value in enumerate(values):
        data = _text(value).encode("utf-8")
        chunks.append(data)
        total += len(data)
        offsets[position + 1] = total
    return b"".join(chunks), offsets


def build_container(
    frame: pd.DataFrame, text_offsets: dict[str, np.ndarray], fingerprint: str
) -> bytes:
    """The typed-array container the browser queries.

    Layout: a u32 header length, that much JSON (padded so the payload starts
    on an 8-byte boundary), then the arrays. Each array is padding-aligned and
    its byte offset is in the header, so the reader never guesses a layout.
    """
    arrays: dict[str, tuple[str, np.ndarray]] = {}
    header: dict[str, object] = {}

    place_dict, place = _place_codes(frame["place"])
    province_dict, province = _codes(frame["province"])
    material_dict, material = _codes(frame["material_en"])
    # The results table prints the corpus's own string ("Greek, Latin"), while
    # the filter matches the tokens it holds: both are stored.
    language_text_dict, language_text = _codes(frame["language"])
    _fits_u16(len(province_dict), "provinces")
    _fits_u16(len(material_dict), "materials")
    language_dict, language_offsets, language_ids = _csr(frame["language"])
    category_dict, category_offsets, category_ids = _csr(frame["category_en"])
    coord, coord_latitude, coord_longitude = _coord_codes(frame["latitude"], frame["longitude"])
    edcs, index = _record_parts(frame["record_id"], frame["edcs_id"])

    rows = len(frame)
    arrays["place"] = ("u32", place)
    arrays["coord"] = ("u32", coord)
    arrays["province"] = ("u16", province.astype(np.uint16))
    arrays["material"] = ("u16", material.astype(np.uint16))
    arrays["edcs"] = ("u32", edcs)
    arrays["index"] = ("u16", index)
    arrays["not_before"] = ("i32", _years(frame["not_before"]))
    arrays["not_after"] = ("i32", _years(frame["not_after"]))
    _fits_u16(len(language_text_dict), "languages")
    arrays["language_offsets"] = ("u32", language_offsets)
    arrays["language_ids"] = ("u16", language_ids)
    arrays["language_text"] = ("u16", language_text.astype(np.uint16))
    arrays["category_offsets"] = ("u32", category_offsets)
    arrays["category_ids"] = ("u16", category_ids)
    arrays["coord_latitude"] = ("f64", coord_latitude)
    arrays["coord_longitude"] = ("f64", coord_longitude)
    for mode, offsets in text_offsets.items():
        arrays[f"text_{mode}"] = ("u32", offsets)

    header["dicts"] = {
        "place": place_dict,
        "province": province_dict,
        "material": material_dict,
        "language": language_dict,
        "language_text": language_text_dict,
        "category": category_dict,
    }
    header["coord_count"] = int(len(coord_latitude))

    payload = bytearray()
    table: dict[str, dict[str, object]] = {}
    for name, (dtype, array) in arrays.items():
        payload += b"\0" * (-len(payload) % 8)
        table[name] = {"dtype": dtype, "offset": len(payload), "length": int(array.size)}
        payload += np.ascontiguousarray(array).tobytes()

    header = {
        "format": FORMAT,
        "fingerprint": fingerprint,
        "rows": int(rows),
        "modes": list(TEXT_COLUMNS),
        "arrays": table,
        **header,
    }
    raw = json.dumps(header, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    # JSON allows trailing whitespace, so the payload can start aligned.
    raw += b" " * (-(4 + len(raw)) % 8)
    return struct.pack("<I", len(raw)) + raw + bytes(payload)


def read_container(data: bytes) -> dict:
    """Decode a container. The browser does this too; here it proves the output."""
    length = struct.unpack_from("<I", data, 0)[0]
    header = json.loads(data[4 : 4 + length].decode("utf-8"))
    payload = memoryview(data)[4 + length :]
    arrays = {
        name: np.frombuffer(payload, dtype=NP_DTYPES[spec["dtype"]], count=spec["length"],
                            offset=spec["offset"])
        for name, spec in header["arrays"].items()
    }
    if header["format"] != FORMAT:
        raise ValueError(f"unknown container format: {header['format']!r}")
    return {"header": header, "arrays": arrays}


def _write_gz(path: Path, data: bytes) -> int:
    # mtime=0: the same corpus writes the same bytes, so a rebuild of an
    # unchanged corpus does not churn the deployment.
    with gzip.GzipFile(path, "wb", mtime=0) as handle:
        handle.write(data)
    return path.stat().st_size


def _write_json(path: Path, body: object) -> int:
    path.write_text(
        json.dumps(body, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    return path.stat().st_size


def write_static_site(
    catalog: Catalog, out_dir: Path, log: Callable[[str], None] = print
) -> dict:
    """Write every artifact, then the manifest that names them."""
    out_dir.mkdir(parents=True, exist_ok=True)
    log("Reading the cleaned corpus…")
    frame = catalog.inscriptions()
    fingerprint = catalog.fingerprint()
    log(f"  {len(frame):,} rows in the search frame (fingerprint {fingerprint})")

    sizes: dict[str, int] = {}
    files: dict[str, object] = {"text": {}, "map": {}}

    log("Writing the search-mode texts…")
    text_offsets: dict[str, np.ndarray] = {}
    text_bytes: dict[str, int] = {}
    for mode, column in TEXT_COLUMNS.items():
        blob, offsets = _text_blob(list(frame[column]))
        text_offsets[mode] = offsets
        text_bytes[mode] = len(blob)
        name = f"text-{mode}.bin.gz"
        sizes[name] = _write_gz(out_dir / name, blob)
        files["text"][mode] = name  # type: ignore[index]
        log(f"  {name}: {len(blob) / 1e6:.1f} MB of text, {sizes[name] / 1e6:.1f} MB on disk")

    log("Writing the query container…")
    corpus = "corpus.bin.gz"
    sizes[corpus] = _write_gz(out_dir / corpus, build_container(frame, text_offsets, fingerprint))
    files["corpus"] = corpus
    log(f"  {corpus}: {sizes[corpus] / 1e6:.1f} MB on disk")

    log("Writing the filter options and the known places…")
    values = catalog.filter_options()
    forms = catalog.short_forms()
    facets = "facets.json"
    sizes[facets] = _write_json(
        out_dir / facets,
        {
            "values": {FACET_NAMES[column]: options for column, options in values.items()},
            "short_forms": {FACET_NAMES[column]: mapping for column, mapping in forms.items()},
        },
    )
    files["facets"] = facets

    known = catalog.known_places()
    known_name = "places-known.geojson"
    sizes[known_name] = _write_json(
        out_dir / known_name,
        place_collection(
            known,
            False,
            {"places": len(known), "inscriptions": int(known["count"].sum())},
        ),
    )
    files["known_places"] = known_name

    log("Writing the map layers…")
    layers = {
        "provinces": catalog.provinces_geojson(),
        "province-labels": catalog.province_labels(),
        "roads": catalog.roads_geojson(),
        "cities": [
            {"latitude": latitude, "longitude": longitude, "label": label}
            for latitude, longitude, label in catalog.cities_points()
        ],
    }
    for layer, body in layers.items():
        name = f"map-{layer}.json"
        sizes[name] = _write_json(out_dir / name, body)
        files["map"][layer] = name  # type: ignore[index]

    # The API-reference icon in the top bar: the spec the app was built against.
    spec = "openapi.yaml"
    (out_dir / spec).write_bytes(API_SPEC.read_bytes())
    sizes[spec] = (out_dir / spec).stat().st_size
    files["openapi"] = spec

    manifest = {
        "format": FORMAT,
        "fingerprint": fingerprint,
        "rows": int(len(frame)),
        "modes": list(TEXT_COLUMNS),
        "text_bytes": text_bytes,
        "files": files,
    }
    # Last: a directory without a manifest is an unfinished build, and the
    # frontend says so instead of fetching half a snapshot.
    _write_json(out_dir / "manifest.json", manifest)
    log(f"\n{len(frame):,} rows, {sum(sizes.values()) / 1e6:.1f} MB of artifacts in {out_dir}")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m epigcorpus.export.static_site",
        description="Write the corpus as static artifacts for a serverless deployment.",
    )
    parser.add_argument(
        "--out", type=Path, required=True, help="Directory to write the artifacts into."
    )
    args = parser.parse_args(argv)
    catalog = Catalog(corpus_files, map_layers)
    write_static_site(catalog, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

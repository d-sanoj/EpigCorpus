"""The implementation against its contract, backend/src/epigcorpus/api/openapi.yaml.

Every response checked here is validated against the schema the contract
declares for that path, method, status and media type.
"""

from __future__ import annotations

import gzip
import io
import json
import time

import pandas as pd
import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from epigcorpus.services import search
from epigcorpus.services.catalog import selections_from

RAW = "Raw inscriptions"
NO_FILTERS = selections_from({})


# ─── contract helpers ─────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def validate(spec):
    registry = Registry().with_resource("urn:spec", Resource(spec, specification=DRAFT202012))

    def operation(path: str, method: str) -> dict:
        return spec["paths"][path][method]

    def check(response, path: str, method: str = "get"):
        op = operation(path, method)
        status = str(response.status_code)
        where = f"{method.upper()} {path}"
        assert status in op["responses"], f"{where}: {status} is not in the contract"
        declared = op["responses"][status]
        if "$ref" in declared:
            declared = _deref(spec, declared["$ref"])
        media_type = response.headers.get("content-type", "").split(";")[0]
        content = declared.get("content")
        if not content:
            return
        assert media_type in content, f"{path}: {media_type} not declared ({list(content)})"
        schema = content[media_type].get("schema")
        is_json = media_type == "application/json" or media_type.endswith("+json")
        if schema is None or not is_json:
            return
        schema = json.loads(json.dumps(schema).replace('"#/components', '"urn:spec#/components'))
        errors = list(Draft202012Validator(schema, registry=registry).iter_errors(response.json()))
        assert not errors, f"{path}: {errors[0].message} at {list(errors[0].absolute_path)}"

    return check


def _deref(spec, ref: str) -> dict:
    node = spec
    for part in ref.lstrip("#/").split("/"):
        node = node[part]
    return node


# ─── the contract and the app describe the same operations ───────────────────


def test_every_contract_operation_is_routed_and_nothing_else(client, spec):
    contract = {
        (method.upper(), path)
        for path, item in spec["paths"].items()
        for method in item
    }
    # FastAPI's own view of what is routed (its generated schema is not served;
    # openapi.yaml is the contract).
    generated = client.app.openapi()["paths"]
    routed = {(method.upper(), path) for path, item in generated.items() for method in item}
    assert routed == contract


def test_the_contract_is_served(client):
    response = client.get("/api/v1/openapi.yaml")
    assert response.status_code == 200
    assert response.text.startswith("openapi: 3.1.0")


def test_health(client, validate):
    response = client.get("/api/v1/health")
    validate(response, "/api/v1/health")
    assert response.json()["status"] == "ok"


# ─── inscriptions ─────────────────────────────────────────────────────────────


def test_list_matches_the_search_service(client, catalog, validate):
    response = client.get("/api/v1/inscriptions", params={"limit": 500})
    validate(response, "/api/v1/inscriptions")
    body = response.json()
    expected = catalog.rows("", RAW, NO_FILTERS)
    assert body["page"]["total"] == len(expected) > 0
    assert [row["record_id"] for row in body["data"]] == expected["record_id"].tolist()


def test_search_matches_the_service_exactly(client, catalog, validate):
    term = "vixit"
    frame = catalog.inscriptions()
    expected = search.apply_filters(frame, "inscription_text_interpretive", term, {})
    response = client.get(
        "/api/v1/inscriptions/search", params={"q": term, "search_in": "interpretive", "limit": 500}
    )
    validate(response, "/api/v1/inscriptions/search")
    got = [row["record_id"] for row in response.json()["data"]]
    assert got == expected["record_id"].tolist()
    assert got, "the fixture corpus should contain 'vixit'"


def test_filters_use_the_documented_semantics(client, catalog):
    """material exact; language on token overlap; filters AND, values OR."""
    frame = catalog.inscriptions()
    material = frame["material_en"].dropna().iloc[0]
    response = client.get(
        "/api/v1/inscriptions", params={"material": material, "language": "Latin"}
    )
    rows = response.json()["data"]
    assert rows
    assert all(row["material_en"] == material for row in rows)
    assert all("Latin" in (row["language"] or "").split(", ") for row in rows)
    expected = search.apply_filters(
        frame, "inscription_text", "", {"material_en": [material], "language": ["Latin"]}
    )
    assert response.json()["page"]["total"] == len(expected)


def test_pages_join_up_and_carry_a_link_header(client):
    first = client.get("/api/v1/inscriptions", params={"limit": 7})
    body = first.json()
    assert body["page"]["next_cursor"]
    assert 'rel="next"' in first.headers["link"]
    seen = [row["record_id"] for row in body["data"]]
    cursor = body["page"]["next_cursor"]
    while cursor:
        page = client.get("/api/v1/inscriptions", params={"limit": 7, "cursor": cursor}).json()
        seen += [row["record_id"] for row in page["data"]]
        cursor = page["page"]["next_cursor"]
    whole = client.get("/api/v1/inscriptions", params={"limit": 500}).json()
    assert seen == [row["record_id"] for row in whole["data"]]


def test_a_cursor_from_another_query_is_refused(client, validate):
    cursor = client.get("/api/v1/inscriptions", params={"limit": 2}).json()["page"]["next_cursor"]
    response = client.get(
        "/api/v1/inscriptions", params={"limit": 2, "cursor": cursor, "language": "Latin"}
    )
    validate(response, "/api/v1/inscriptions")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"].endswith("/cursor-mismatch")


def test_get_one_and_its_variants(client, catalog, validate):
    record_id = catalog.inscriptions()["record_id"].iloc[0]
    response = client.get(f"/api/v1/inscriptions/{record_id}")
    validate(response, "/api/v1/inscriptions/{record_id}")
    record = response.json()
    assert record["record_id"] == record_id
    assert record["retrieved_at"].endswith("Z")

    variants = client.get(f"/api/v1/inscriptions/{record_id}/variants")
    validate(variants, "/api/v1/inscriptions/{record_id}/variants")
    assert variants.json()["raw"] == record["inscription_text"]
    assert variants.json()["interpretive"] == record["inscription_text_interpretive"]

    one = client.get(f"/api/v1/inscriptions/{record_id}/variants/conservative")
    validate(one, "/api/v1/inscriptions/{record_id}/variants/{variant}")
    assert one.json()["text"] == record["inscription_text_conservative"]


def test_unknown_inscription_is_a_404_problem(client, validate):
    response = client.get("/api/v1/inscriptions/EDCS-99999999-0")
    validate(response, "/api/v1/inscriptions/{record_id}")
    assert response.status_code == 404
    assert response.json()["instance"] == "/api/v1/inscriptions/EDCS-99999999-0"


def test_bad_parameters_are_422_problems(client, validate):
    response = client.get(
        "/api/v1/inscriptions/search", params={"q": "x", "search_in": "diplomatic"}
    )
    validate(response, "/api/v1/inscriptions/search")
    assert response.status_code == 422
    assert response.json()["errors"][0]["name"] == "search_in"
    missing_q = client.get("/api/v1/inscriptions/search")
    assert missing_q.status_code == 422
    bad_variant = client.get("/api/v1/inscriptions/EDCS-00000001-0/variants/diplomatic")
    validate(bad_variant, "/api/v1/inscriptions/{record_id}/variants/{variant}")
    assert bad_variant.status_code == 422


# ─── places and the map ───────────────────────────────────────────────────────


def test_result_places_match_run_query(client, catalog, validate):
    response = client.get("/api/v1/places", params={"q": "vixit", "limit": 500})
    validate(response, "/api/v1/places")
    body = response.json()
    result = catalog.run_query("vixit", RAW, NO_FILTERS)
    assert body["summary"] == {
        "inscriptions": len(result.rows),
        "places": len(result.places),
        "title": 'Inscriptions matching "vixit"',
        "subtitle": "Searched in raw inscription text",
    }
    assert [p["count"] for p in body["data"]] == result.places["count"].tolist()
    assert all(len(p["examples"]) <= 8 for p in body["data"])


def test_known_places(client, catalog, validate):
    response = client.get("/api/v1/places/known", params={"limit": 500})
    validate(response, "/api/v1/places/known")
    known = catalog.known_places()
    assert response.json()["page"]["total"] == len(known)
    assert response.json()["summary"]["inscriptions"] == int(known["count"].sum())


def test_result_places_as_geojson(client, catalog, validate):
    response = client.get("/api/v1/places.geojson", params={"q": "vixit"})
    validate(response, "/api/v1/places.geojson")
    assert response.headers["content-type"].startswith("application/geo+json")
    body = response.json()
    paged = client.get("/api/v1/places", params={"q": "vixit", "limit": 500}).json()
    assert body["summary"] == paged["summary"]
    assert [
        {
            "place": f["properties"]["place"],
            "latitude": f["geometry"]["coordinates"][1],
            "longitude": f["geometry"]["coordinates"][0],
            "count": f["properties"]["count"],
            "examples": f["properties"]["examples"],
        }
        for f in body["features"]
    ] == paged["data"]


def test_known_places_as_geojson(client, validate):
    response = client.get("/api/v1/places/known.geojson")
    validate(response, "/api/v1/places/known.geojson")
    body = response.json()
    paged = client.get("/api/v1/places/known", params={"limit": 25000}).json()
    assert body["summary"] == paged["summary"]
    assert [
        {
            "place": f["properties"]["place"],
            "latitude": f["geometry"]["coordinates"][1],
            "longitude": f["geometry"]["coordinates"][0],
            "count": f["properties"]["count"],
        }
        for f in body["features"]
    ] == paged["data"]


@pytest.mark.parametrize("layer", ["land", "provinces", "province-labels", "roads", "cities"])
def test_map_layers_with_etags(client, validate, layer):
    path = f"/api/v1/map/layers/{layer}"
    response = client.get(path)
    validate(response, path)
    assert response.json()
    etag = response.headers["etag"]
    cached = client.get(path, headers={"If-None-Match": etag})
    assert cached.status_code == 304


def test_province_labels_name_every_province_in_the_frame(client, catalog):
    labels = client.get("/api/v1/map/layers/province-labels").json()
    frame = catalog.inscriptions()
    assert {label["name"] for label in labels} == set(frame["province"].dropna())
    counts = [label["inscriptions"] for label in labels]
    assert counts == sorted(counts, reverse=True)
    assert sum(counts) == len(frame.dropna(subset=["province"]))


# ─── exports ──────────────────────────────────────────────────────────────────


def test_corpus_jsonl_is_sent_gzipped_or_plain(client, corpus_dir, validate):
    stored = gzip.decompress((corpus_dir / "edcs_inscriptions.jsonl.gz").read_bytes())
    plain = client.get("/api/v1/exports/corpus.jsonl", headers={"Accept-Encoding": "identity"})
    validate(plain, "/api/v1/exports/corpus.jsonl")
    assert "content-encoding" not in plain.headers
    assert plain.content == stored
    assert 'filename="edcs_inscriptions.jsonl"' in plain.headers["content-disposition"]
    packed = client.get("/api/v1/exports/corpus.jsonl", headers={"Accept-Encoding": "gzip"})
    assert packed.headers["content-encoding"] == "gzip"
    assert packed.content == stored  # httpx decodes Content-Encoding


def test_a_missing_export_is_a_503_problem(client):
    response = client.get("/api/v1/exports/corpus.tsv")
    assert response.status_code == 503
    assert response.json()["type"].endswith("/export-unavailable")


def test_search_tsv_is_the_maps_download(client, catalog, validate):
    response = client.get("/api/v1/exports/search.tsv", params={"q": "vixit"})
    validate(response, "/api/v1/exports/search.tsv")
    rows = catalog.rows("vixit", RAW, NO_FILTERS)
    assert response.content == search.results_tsv(catalog.results_table(rows))
    assert 'filename="edcs_search_results.tsv"' in response.headers["content-disposition"]
    table = pd.read_csv(io.BytesIO(response.content), sep="\t")
    assert set(rows["edcs_id"]) == set(table["edcs_id"])


def test_search_tsv_needs_a_query(client, validate):
    response = client.get("/api/v1/exports/search.tsv")
    validate(response, "/api/v1/exports/search.tsv")
    assert response.status_code == 422


@pytest.mark.parametrize(
    "params,filename",
    [
        ({"q": "Vixit Annos"}, "edcs_map_vixit_annos.png"),
        ({"language": "Latin"}, "edcs_map_filtered.png"),
        ({}, "edcs_map_all_known_places.png"),
    ],
)
def test_map_png(client, catalog, validate, params, filename):
    response = client.get("/api/v1/exports/map.png", params=params)
    validate(response, "/api/v1/exports/map.png")
    assert response.content.startswith(b"\x89PNG")
    assert f'filename="{filename}"' in response.headers["content-disposition"]
    filters = {"language": [params["language"]]} if "language" in params else {}
    expected = catalog.export_png(params.get("q", ""), RAW, selections_from(filters))
    assert response.content == expected.content


# ─── lookups ──────────────────────────────────────────────────────────────────


def test_lookups_and_one_table(client, validate):
    whole = client.get("/api/v1/lookups")
    validate(whole, "/api/v1/lookups")
    assert set(whole.json()) >= {"materials", "categories", "provinces", "languages", "places"}
    table = client.get("/api/v1/lookups/provinces")
    validate(table, "/api/v1/lookups/{table}")
    assert table.json()["entries"] == whole.json()["provinces"]
    assert client.get("/api/v1/lookups/emperors").status_code == 422


def test_facets_are_the_filter_options(client, catalog, validate):
    response = client.get("/api/v1/facets")
    validate(response, "/api/v1/facets")
    values = response.json()["values"]
    options = catalog.filter_options()
    assert values["material"] == options["material_en"]
    assert values["language"] == options["language"]
    assert response.json()["short_forms"]["language"] == catalog.short_forms()["language"]


# ─── pipeline runs ────────────────────────────────────────────────────────────


def _wait_for(client, url, statuses):
    for _ in range(250):
        run = client.get(url).json()
        if run["status"] in statuses:
            return run
        time.sleep(0.02)
    raise AssertionError(f"run never reached {statuses}: {run}")


def _stages(run) -> dict:
    return {stage["name"]: (stage["status"], stage["result"]) for stage in run["stages"]}


def test_a_run_harvests_cleans_and_reloads(client, harvest, cleaner, catalogs, validate):
    harvest.release.set()
    before = catalogs.current
    started = client.post("/api/v1/pipeline-runs", json={"limit": 5, "workers": 2})
    validate(started, "/api/v1/pipeline-runs", "post")
    assert started.status_code == 202
    location = started.headers["location"]
    assert location == started.json()["links"]["self"]

    run = _wait_for(client, location, {"succeeded"})
    validate(client.get(location), "/api/v1/pipeline-runs/{run_id}")
    assert _stages(run) == {
        "harvest": ("succeeded", "complete"),
        "clean": ("succeeded", 42),
        "reload": ("succeeded", "fixture-snapshot"),
    }
    assert run["progress"]["cursor"] == 5
    assert run["links"]["cancel"] is None
    assert harvest.calls == [(5, 2, False, False)]
    assert cleaner.calls == 1
    assert catalogs.current is before  # the fixture's build hands back the same catalog

    listed = client.get("/api/v1/pipeline-runs")
    validate(listed, "/api/v1/pipeline-runs")
    assert [r["id"] for r in listed.json()["data"]] == [run["id"]]


def test_a_clean_only_run_skips_the_harvest(client, harvest, cleaner):
    started = client.post("/api/v1/pipeline-runs", json={"harvest": False}).json()
    assert [stage["name"] for stage in started["stages"]] == ["clean", "reload"]
    run = _wait_for(client, started["links"]["self"], {"succeeded"})
    assert _stages(run) == {"clean": ("succeeded", 42), "reload": ("succeeded", "fixture-snapshot")}
    assert harvest.calls == []


def test_a_harvest_only_run_does_not_clean(client, harvest, cleaner):
    harvest.release.set()
    started = client.post("/api/v1/pipeline-runs", json={"clean": False}).json()
    run = _wait_for(client, started["links"]["self"], {"succeeded"})
    assert _stages(run) == {"harvest": ("succeeded", "complete")}
    assert cleaner.calls == 0


def test_one_run_at_a_time_and_cancel_during_the_harvest(client, harvest, cleaner, validate):
    started = client.post("/api/v1/pipeline-runs").json()
    second = client.post("/api/v1/pipeline-runs", json={})
    validate(second, "/api/v1/pipeline-runs", "post")
    assert second.status_code == 409
    assert second.json()["running_run"] == started["links"]["self"]

    cancelling = client.post(f"/api/v1/pipeline-runs/{started['id']}/cancel")
    validate(cancelling, "/api/v1/pipeline-runs/{run_id}/cancel", "post")
    assert cancelling.status_code == 202
    harvest.release.set()
    run = _wait_for(client, started["links"]["self"], {"cancelled"})
    assert _stages(run) == {
        "harvest": ("cancelled", None),
        "clean": ("skipped", None),
        "reload": ("skipped", None),
    }
    assert cleaner.calls == 0

    again = client.post(f"/api/v1/pipeline-runs/{started['id']}/cancel")
    validate(again, "/api/v1/pipeline-runs/{run_id}/cancel", "post")
    assert again.status_code == 409


def test_a_cancel_during_cleaning_still_reloads(client, cleaner):
    cleaner.release.clear()
    started = client.post("/api/v1/pipeline-runs", json={"harvest": False}).json()
    assert cleaner.started.wait(5)
    client.post(f"/api/v1/pipeline-runs/{started['id']}/cancel")
    cleaner.release.set()
    run = _wait_for(client, started["links"]["self"], {"succeeded", "cancelled"})
    assert run["status"] == "succeeded"
    assert _stages(run)["reload"] == ("succeeded", "fixture-snapshot")


def test_a_failing_stage_fails_the_run(client, cleaner, validate):
    cleaner.error = OSError("disk full")
    started = client.post("/api/v1/pipeline-runs", json={"harvest": False}).json()
    run = _wait_for(client, started["links"]["self"], {"failed"})
    validate(client.get(started["links"]["self"]), "/api/v1/pipeline-runs/{run_id}")
    assert _stages(run) == {"clean": ("failed", None), "reload": ("skipped", None)}
    assert run["error"]["title"] == "The clean stage failed"
    assert run["error"]["detail"] == "OSError: disk full"


def test_run_request_is_validated(client, validate):
    response = client.post("/api/v1/pipeline-runs", json={"workers": 0, "colour": "red"})
    validate(response, "/api/v1/pipeline-runs", "post")
    assert response.status_code == 422
    neither = client.post("/api/v1/pipeline-runs", json={"harvest": False, "clean": False})
    validate(neither, "/api/v1/pipeline-runs", "post")
    assert neither.status_code == 422
    assert client.get("/api/v1/pipeline-runs/nope").status_code == 404


def test_a_reload_swaps_the_catalog_for_later_requests(reader):
    from epigcorpus.adapters import map_layers
    from epigcorpus.services.catalog import Catalog, CatalogSlot

    built = []

    def build():
        built.append(Catalog(reader, map_layers))
        return built[-1]

    slot = CatalogSlot(build)
    first = slot.current
    assert slot.reload() == "fixture-snapshot"
    assert slot.current is built[-1] is not first
    assert slot.current.is_loaded()

# ─── the web UI is served alongside ───────────────────────────────────────────


def test_the_ui_is_served_at_the_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "EpigCorpus" in response.text


def test_client_side_routes_fall_back_to_the_ui(client):
    response = client.get("/inscriptions/EDCS-00000001-0")
    assert response.status_code == 200
    assert "EpigCorpus" in response.text


def test_unknown_api_paths_stay_404_problems(client):
    response = client.get("/api/v1/nothing-here")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")

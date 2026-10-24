# Tests

Offline. Every fixture is a real payload captured from the live EDCS on
**2026-08-19** (release tag `20260807-142626`); no test makes a network request.

| Fixture | Source | Contents |
|---|---|---|
| `fixtures/monuments/*.json` | `/data/monument/{shard}/{id8}.json` | 50 real monument records, sampled with seed `20260819` |
| `fixtures/lookups.json` | `/data/indexes/lookups.json` | materials, provinces, categories |
| `fixtures/languages.json` | `/data/indexes/languages.json` | language codes |
| `fixtures/places_subset.json` | `/data/indexes/places.json` | the 38 places referenced by the monument fixtures, plus Londinium, Eboracum and Colonia Agrippinensis for the bounding-box regression test |
| `fixtures/searchable_sample.json` | `/data/indexes/searchable.json` | first 200 of 542,854 index rows |

The full index is 19 MB and is deliberately not committed;
`adapters/test_edcs_upstream_contract.py` documents its schema against the
200-row sample.

```bash
uv run pytest -q          # all tests
uv run ruff check .       # lint
```

## What these tests are for

`adapters/test_edcs_upstream_contract.py` (formerly `tests/test_api_contract.py`)
pins the **current** EDCS schema so that the next time
EDCS is rebuilt, we find out from a red test rather than from silent data loss.
That is the specific failure mode described in `docs/LATEPIG_BREAKAGE.md`:
both LatEpig and EpigCorpus were broken by interface changes they had no way to
detect.

`core/test_cleaner_baseline.py` characterises the cleaning pipeline as it behaves
today, so any change to what the corpus is shows up as a deliberate test edit.
The rules' known gaps (T05-T09) are recorded in `docs/AUDIT.md`, not as
expected-to-fail tests; a fix for one adds its test.

`test_layering.py` enforces the dependency rule between the layers of the
`epigcorpus` package (see `docs/adr/0001-layered-app-layout.md`): services
never import adapters, `core` and `models` import nothing above them, and
fastapi, starlette and pydantic appear only in `api`. It reads imports
with `ast`, so it needs nothing beyond pytest.

`api/test_contract.py` runs the HTTP API against `src/epigcorpus/api/openapi.yaml`. It
builds a real cleaned corpus offline from the monument fixtures (parse, clean,
write, read back), routes every operation the contract declares and nothing
else, validates every response against its declared schema, and checks the
behaviour that matters: search and places identical to the services, pages
that join up, problem+json errors, gzip negotiation, the TSV and PNG bytes,
and the scrape-job lifecycle against a fake harvest.

The name `test_edcs_upstream_contract.py` is deliberate: those tests are about
the EDCS API we consume, not the EpigCorpus HTTP API described in
`app/api/openapi.yaml`.

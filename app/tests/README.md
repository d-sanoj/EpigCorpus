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

The full index is 19 MB and is deliberately not committed; `tests/test_api_contract.py`
documents its schema against the 200-row sample.

```bash
uv run pytest -q          # all tests
uv run ruff check .       # lint
```

## What these tests are for

`test_api_contract.py` pins the **current** EDCS schema so that the next time
EDCS is rebuilt, we find out from a red test rather than from silent data loss.
That is the specific failure mode described in `docs/LATEPIG_BREAKAGE.md`:
both LatEpig and EpigCorpus were broken by interface changes they had no way to
detect.

`test_cleaner_baseline.py` characterises the cleaning pipeline **as it behaves
today, bugs included**. Several tests assert wrong output on purpose and are
marked `xfail` with the task ID that will fix them. They exist so that Phase 1
changes are visibly deliberate: when a fix lands, its `xfail` flips to a pass and
the assertion is rewritten to the correct value in the same commit.

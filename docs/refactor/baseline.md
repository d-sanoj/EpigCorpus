# Phase 2 — Baseline before the restructure

Recorded 2026-10-23 at commit `ba11fd2` (tree identical to `main` @ `bcdc14e`
plus `docs/refactor/`), Python 3.13.17, uv 0.11.32, ruff 0.16.9.

Every run that writes data ran in a throwaway `git worktree` in the session
scratchpad, never in this checkout. Running the pipeline rewrites the tracked
cleaned `.gz`, and gzip's header mtime changes its bytes (discovery §5.14).

## 1. Environment

`uv sync` succeeded through the proxy and installed the locked dependency set.
`uv.lock` is unchanged.

## 2. Lint

| Scope | Result |
|---|---|
| `ruff check .` | **532 findings**, all in `scripts/` (371 E701/E702, 42 E501, 26 I001, 23 E401, 14 F401, 14 E741, …) |
| `ruff check src main.py tests` | **All checks passed** |

## 3. Tests

`pytest -rA`: **69 passed, 16 xfailed** in 1.7 s. No failures, errors or
xpasses. The 85 test IDs are kept in the scratchpad for comparison after the
move.

## 4. CLI help

```text
$ python main.py --help
usage: main.py [-h] [--skip-scrape] [--skip-map]

EDCS Analytics: Scrape → Clean → Launch Interactive Map

options:
  -h, --help     show this help message and exit
  --skip-scrape  Skip scraping and only clean the latest data/*.jsonl file.
  --skip-map     Skip launching the Streamlit map after cleaning.
```

```text
$ python src/edcs_scraper.py --help
usage: edcs_scraper.py [-h] [--limit LIMIT] [--workers WORKERS] [--restart]
                       [--refresh-index]

Harvest EDCS inscriptions.

options:
  -h, --help         show this help message and exit
  --limit LIMIT      Only harvest the first N monuments (smoke test).
  --workers WORKERS  Concurrent requests (default 16).
  --restart          Discard existing output and checkpoint, then start over.
  --refresh-index    Re-fetch the corpus index instead of using the cache.
```

## 5. Full-corpus cleaning run

`python main.py --skip-scrape --skip-map` in the worktree: exit 0, 164 s.
It loaded `data/edcs_inscriptions.jsonl.gz` and reported Forged 6265,
Unreadable 13229, 588,509 rows.

| Output | sha256 |
|---|---|
| plain `edcs_inscriptions_cleaned.jsonl` (662,844,584 bytes) | `54b9ee378f6d20648ddafd8652d53a2301119f9044f0b474c2419f8506d79699` |
| decompressed new `edcs_inscriptions_cleaned.jsonl.gz` | `54b9ee37…79699` (same) |
| decompressed **committed** `data/edcs_inscriptions_cleaned.jsonl.gz` | `54b9ee37…79699` (same) |

The committed cleaned corpus is therefore exactly what the current code
produces from the committed raw corpus.

## 6. Scraper characterisation (fixture harvest)

The harness fakes `requests.Session.get` to serve the 50 committed monument
fixtures plus `lookups.json`, `languages.json` and `places_subset.json`. It
also serves a synthetic `sources.json` (there is no fixture for it), freezes
`time.time` and `time.gmtime`, and no-ops `time.sleep`. It runs the scraper's
real `main()` with `--restart --refresh-index --workers 4`. Two runs were
made. A repeat run gave identical bytes.

**Run 1:** index = 50 fixture ids + 2 unknown ids (404). This covers the
failure path, the checkpoint and the incomplete-harvest message.

| File | sha256 |
|---|---|
| `edcs_inscriptions.jsonl` (51 rows) | `72b2b28dfa85512151bf75d2b18c0b8ee9e6e635a768914601ebadec188d0a78` |
| `edcs_inscriptions.tsv` | `e0e2dafddad6f0295629f02ae6bf4416801ac8e330c40fd7a7a260860c93cba5` |
| `edcs_checkpoint.json` | `61289bf72d2cd9330fabacef94ba75f67d709c623c2466b2c8ddc40e8da991a1` |
| `edcs_failed_ids.json` | `6483a3b1b38a3b43ccf3030d84b18d57c62ab22d3205ddea29a975308b9f0501` |
| `edcs_lookup.json` | `6e0b059e341e934c126ee9f749418d7f7988d3b26eba6a26a614484e3d8e556f` |
| `edcs_index_cache.json` | `41b012429531e12ffa30245756113709d4d54ce2afd72fc0e0b55041be089aee` |

**Run 2:** index = the 50 fixture ids only. This covers compression, the
manifest and checkpoint removal.

| File | sha256 |
|---|---|
| `edcs_inscriptions.jsonl` | `72b2b28d…0a78` (same as run 1) |
| `edcs_inscriptions.tsv` | `e0e2dafd…cba5` (same as run 1) |
| `edcs_inscriptions.jsonl.gz` | `575f71cf639daefee91a88a737b48869e81d424e4e2ccf7608cf87cd6603872a` |
| `edcs_inscriptions.tsv.gz` | `3108db1818117bdb4ca0ee886c49644b34f53626cbf31d80bb0fa4265c62b1d1` |
| `edcs_harvest_manifest.json` | `6b861621511060e5d5db857bd59178885018bc8eee958169c86fc8290b293e18` |
| `edcs_failed_ids.json` | `846e3be26e1f8f06fb491810935577d3c28135d851ba9ea022d2631a2e9697d0` |
| `edcs_lookup.json` | `6e0b059e…6f` (same as run 1) |
| `edcs_index_cache.json` | `40574225958fc7e8d0bf2076899bc617bf3ce0bf60c7a1092278012dc9afc640` |
| checkpoint | removed, as expected |

Stdout of both runs is kept for line-by-line comparison. Only absolute
paths may differ.

## 7. Fixture cleaning run

`clean_data(verbose=True)` was run on the 51 harvested fixture rows plus the
first 2,000 rows of the committed raw corpus (`belege` renamed to `evidence`,
as `main.py` does). The result was written with the same `to_json` call as
`main.py`.

| Output | sha256 |
|---|---|
| cleaned JSONL (2,051 rows) | `81db7815d1dd99f702da976fbb75ad69ce9e9c5280c000965805b20f48e187e1` |
| captured verbose stdout | `31d3412f5be3249d101a1392360eed0efcabaab5288468fcb8bbd6843f498c5f` |

## 8. Streamlit app characterisation

This loads the app module from its file path and calls its functions, then
runs `streamlit.testing.v1.AppTest` on the file (190 s in total, most of it
loading 588k rows). Hashes are the first 16 hex characters of sha256.

| Probe | Result |
|---|---|
| `load_known_places()` | 21,999 places, `60a142e538a00938` |
| `filter_options()` | material 21, province 62, place 21,949, category 37, language 14; `e3df491b4329a0df` |
| `short_forms()` | `9529cc99cf3de9b8` |
| `run_query("viator", raw, no filters)` | 515 rows `7ac9df75d0cc57b9`, 257 places `41685c5b30c66801` |
| `run_query("viator", interpretive, Latin + Noricum/Dalmatia)` | 36 rows `a41903f78b5f6115`, 20 places `10f3ebad554eb6c8` |
| `run_query("", conservative, same filters)` | 14,712 rows `a2ac4ad9a6cbd720`, 1,312 places `93188d61772f1024` |
| `describe_query` for the three queries | `c79992cade0c4356`, `0cd9e7e21798c3b2`, `1595fe985eaf31b9` |
| `build_png_bytes`, viator query | 713,775 bytes, `b0760bdcfa95e2f0` |
| `build_png_bytes`, all known places | 1,148,839 bytes, `8bcc2dcc609d1011` |
| `build_map_html`, query / known | `ebd05e7845467111` / `d5ddfb05f49b3e90` |
| results TSV for viator | 927 rows, `85279c429e1ecd28` |
| AppTest first run | no exception. Widgets: selectbox `Search in:`; text input `Keyword`; multiselects `Language, Province, Place, Category, Material`; button `Search`; info "Enter a keyword, choose filters, or both, then press Search." |
| AppTest after typing `viator` + Search | no exception; summary "515 inscriptions match. Searched in raw inscription text."; 1 dataframe |

## 9. Git attributes and ignores

`git check-attr -a`: `data/edcs_inscriptions.jsonl` → `text: set`, `eol: lf`;
`data/x.gz` → `binary` (`diff`, `merge`, `text` unset); nested
`data/derived/v1/a.tsv` → no attributes (the `data/*.tsv` rule is not
recursive).

`git check-ignore`: `data/*.jsonl`, `data/*.tsv`,
`data/edcs_checkpoint.json`, `data/edcs_index_cache.json`,
`data/edcs_inscriptions_cleaned.csv` and `data/derived/**/*.tsv` each match
their rule.

## 10. Data checksums

`docs/refactor/checksums-before.txt`: sha256 of all **154** tracked files
under `data/`, `results/`, `reports/`, `img/` and `tests/fixtures/`.
`docs/refactor/path-map.tsv` maps each one to its new path.

# The EDCS API as of 2026-08-19

**Status: the API this project was built against no longer exists.**

All observations below were made on 2026-08-19 (UTC) against
`https://edcs.hist.uzh.ch`, serving release tag `20260807-142626`.
Probe scripts and captured payloads are described in §8.

---

## 1. Summary of the change

EpigCorpus was written against a DataTables-style JSON endpoint at
`https://edcs.hist.uzh.ch/api/query`, paginated with `start`/`length` and
reporting `recordsTotal`. That endpoint has been withdrawn.

The site released on 2026-08-07 is a **client-side application backed by static
JSON files**. The browser downloads a complete index of the corpus once, filters
it locally, and fetches individual monument records as static files. There is no
server-side pagination and no query endpoint of the old shape.

| | Old EDCS (EpigCorpus target) | Current EDCS (2026-08-07) |
|---|---|---|
| Corpus listing | `GET /api/query` paginated | `GET /data/indexes/searchable.json` (one file) |
| Pagination | `start` / `length`, `recordsTotal` | none — whole index in one response |
| Record detail | inline in the page payload | `GET /data/monument/{shard}/{id8}.json` |
| Fulltext search | `search[value]` server-side | `GET /api/search` (returns bare id array) |
| Code lookups | `lookup` key inside each response | `GET /data/indexes/lookups.json` |
| Places / coords | `obj.coord` inline | `GET /data/indexes/places.json`, joined by geo id |
| Citations | `obj.belege` inline | `GET /data/indexes/sources.json`, joined by source id |

## 2. Endpoint status observed

| URL | Status | Notes |
|---|---|---|
| `https://edcs.hist.uzh.ch/` | 200 | new SPA, `release-tag: 20260807-142626` |
| `https://edcs.hist.uzh.ch/api/query` | **403** | withdrawn; 403 regardless of headers |
| `https://edcs.hist.uzh.ch/en/search` | **404** | the `Referer` our scraper sends |
| `https://edcs.hist.uzh.ch/api/search?q=…` | 200 | new fulltext endpoint, returns `[id, …]` |
| `https://edcs.hist.uzh.ch/data/indexes/*.json` | 200 | static, gzip, no auth |
| `https://edcs.hist.uzh.ch/data/monument/{shard}/{id8}.json` | 200 | static, gzip, no auth |
| `http://db.edcs.eu/epigr/epi.php` | **404** | legacy host, 301s to `edcs.hist.uzh.ch`, then 404 |

The 403 on `/api/query` is **not** user-agent gating. Six header combinations were
tested — the project's exact spoofed Firefox headers, an honest identifying UA,
UA-only, no UA — and all six returned an identical 403 (322-byte Apache error
page). The path is simply no longer routed. `/api/query` appears nowhere in the
current frontend bundle `/assets/app.js`; only `/api/search` and `/api/comment` do.

This matters for **T26**: replacing the spoofed User-Agent costs us nothing,
because the spoofing is not what is buying access. It never was.

## 3. `/data/indexes/searchable.json` — the corpus index

One request. 19,302,697 bytes on the wire (gzip), retrieved in **1.2 s**.

```json
{ "v": 9, "k": ["id","g","m","ci","li","d","h"], "d": [[...], ...],
  "bc": [...], "bl": [...] }
```

- `v` — schema version (9 at time of writing)
- `k` — column names for each row in `d`
- `d` — **542,854 rows**, every row exactly 7 fields
- `bc` — category bitmask table (9,266 entries)
- `bl` — language bitmask table (32 entries)

Row columns:

| Key | Meaning | Notes |
|---|---|---|
| `id` | monument id, integer | range 1 … 85,701,225; 542,854 unique |
| `g` | geo id → `places.json` | `null` for 3,481 rows (0.6%) |
| `m` | material id → `lookups.json` `materials` | |
| `ci` | index into `bc` category bitmask table | |
| `li` | index into `bl` language bitmask table | |
| `d` | dating | `[]`, a bare int (`465`), or `[from, to]`; negative = BC |
| `h` | image count | integer |

`d` being polymorphic — empty list, scalar, or pair — is a parsing trap and is
directly relevant to **T35**.

## 4. `/data/monument/{shard}/{id8}.json` — record detail

`id8` is the monument id zero-padded to 8 digits; `shard` is its first 3
characters. Monument 1 → `/data/monument/000/00000001.json`.

```json
{ "v": 2,
  "d": { "g": 580553, "m": 8,
         "q": [[3966, "0", "00455"]],
         "p": [{"b": "Muchar_1_p_398.jpg", "u": null}],
         "i": [{"t": "?] C(aius) Trebonius IIvir et praef(ectus) i(ure) d(icundo) civitatis Agunti [?",
                "d": [null, null], "s": [5], "g": [4,20,26,28]}],
         "c": [] } }
```

| Key | Meaning | Old equivalent |
|---|---|---|
| `d.g` | geo id | `obj.ort` + `obj.provinz` + `obj.coord` |
| `d.m` | material id | `obj.material` |
| `d.q` | citations, `[source_id, volume, number]` | `obj.belege` |
| `d.p` | photos, `{b: basename, u: url}` | `obj.bilder` |
| `d.i` | inscriptions array | `obj.inschriften` |
| `d.i[].t` | inscription text | `inschriften[i][0]` |
| `d.i[].d` | `[not_before, not_after]` | `inschriften[i][1]` |
| `d.i[].s` | language ids | `inschriften[i][2]` |
| `d.i[].g` | category ids | `inschriften[i][3]` |
| `d.c` | comments | *not present in old API* |

The one-monument-to-many-inscriptions model that EpigCorpus built its
`record_id` scheme around **survives unchanged**. `d.i` is still an array. The
project's core data-model decision is still correct; only the transport and the
field names changed.

## 5. `/data/indexes/places.json` — geography

```json
{ "v": 4, "k": ["geo_id","ort","p","coord"], "d": [[110001,"?",0,null], …] }
```

23,707 places; 22,007 carry coordinates. `p` indexes `lookups.json` `provinces`.

**`coord` is `[latitude, longitude]`.** Verified against known points:
Roma `[41.89332, 12.48293]`, Köln `[50.6464, 7.17858]`, Maribor `[46.55563, 15.64477]`.
Observed ranges across all 22,007: field 0 spans 11.90 … 61.13, field 1 spans
−13.50 … 79.82 — consistent with latitude first and inconsistent with the reverse.

The old API's `obj.coord` was `[longitude, latitude]`, and
`edcs_scraper.parse_monument` hardcodes that order (`longitude = coord[0]`).
**Any port to the new API that keeps that line will transpose every coordinate
in the corpus.** This is exactly the silent failure **T34** anticipates.

## 6. `/data/indexes/lookups.json` — code tables

3,114 bytes. `{"v": 2, "d": {materials, provinces, categories}}`.

- `materials` — 22 entries, `[token, {de,en,es,fr,it}]`, index = id, slot 0 `null`
- `provinces` — 64 plain strings, index = id
- `categories` — 37 entries, `[bit, token, {de,en,es,fr,it}]`

Companion files: `languages.json` (14 entries), `sources.json` (4,007 citation
sources), `acknowledgements.json`, `i18n/{code}.json`, `citations/{id}.json`.

English translation still works — the `en` key is present throughout, so
`get_material_en` and `translate_categories` port over directly.

## 7. Rate limiting and politeness

No rate limiting was observed. 50 sequential monument fetches at 1 req/s
returned 200 with latency min/mean/max **0.14 / 0.17 / 0.66 s**. No
`Retry-After`, no `X-RateLimit-*` headers, no 429s. These are static files behind
Apache 2.4.68 (Debian), so throughput is bounded by the CDN/webserver, not a
quota.

The practical consequence for **T27**: a full harvest under the new architecture
is **1 index request + ~542,854 monument requests** if full detail is required —
versus 1,086 paginated requests under the old API. At 1 req/s that is ~6.3 days.
The index alone, which already carries id, place, material, category, language,
dating and image count, costs **one request**. Deciding how much per-monument
detail we actually need is now a real design question, not a formality; the
inscription *text* lives only in the per-monument files.

## 8. Reproducing these observations

Probes were run from the project venv with an identifying User-Agent.
Captured payloads used as test fixtures live in `tests/fixtures/` (50 monument
records; see `tests/README.md`). The index, places, sources and lookups files
were captured but are **not** committed — `searchable.json` alone is 19 MB and
`data/*.json` is gitignored.

```bash
# index (19 MB, ~1.2 s)
curl -s -H 'Accept-Encoding: gzip' \
  'https://edcs.hist.uzh.ch/data/indexes/searchable.json?v=20260807-142626' | gunzip | head -c 400

# one monument
curl -s 'https://edcs.hist.uzh.ch/data/monument/000/00000001.json'

# the withdrawn endpoint
curl -s -o /dev/null -w '%{http_code}\n' 'https://edcs.hist.uzh.ch/api/query'   # 403
```

## 9. Fields the old API exposed that the new one does not

- `recordsTotal` — no server-side count; the index length (542,854) replaces it
- `draw` / DataTables echo — gone
- the inline `lookup` dictionary — moved to a separate file, and *richer* (5 languages, was `en` only in our use)

## 10. Fields the new API exposes that the old one did not

- `d.c` — per-monument comments
- `bc` / `bl` — category and language bitmasks, enabling fast local filtering
- `sources.json` — full citation-source registry with human-readable descriptions
  and example formats, which would let **T36** normalise CIL-style references
  properly instead of truncating to `entry[:3]`
- five-language labels throughout

# EpigCorpus data-flow audit

Audit date: 2026-08-19. Covers `src/edcs_scraper.py`, `src/edcs_cleaner.py`,
`src/edcs_streamlit_map.py`, `main.py` at commit `df745d7`.

Purpose (T00): establish exactly where records are dropped, where they are
mutated, and which counts the user is shown — before anything is changed.

Every behaviour marked **[reproduced]** was executed against the live code.

---

## 1. Pipeline overview

```
EDCS /api/query  ──▶ edcs_scraper.py ──▶ data/edcs_inscriptions.jsonl
   (403 as of                              data/edcs_inscriptions.tsv
    2026-08-07)                            data/edcs_lookup.json
                                                   │
                              main.py: read newest data/*.jsonl
                                       rename belege → evidence
                                                   │
                          edcs_cleaner.clean_data ─▶ +4 columns
                                                   │
                              data/edcs_inscriptions_cleaned.jsonl
                                                   │
                        edcs_streamlit_map.py: load, filter, search, map
```

**The pipeline does not currently run end to end.** The scraper 403s against the
current EDCS (see `docs/LATEPIG_BREAKAGE.md` §5). Everything below describes the
code's behaviour on data harvested before 2026-08-07.

---

## 2. Where records disappear

Ordered by severity. "Silent" means no count, log, or warning reaches the user.

### D1 — Bounding box drops 9.3% of the corpus, silently
`edcs_streamlit_map.py:214-219`, `ROMAN_BOUNDS = (25, 50, -10, 45)` at line 48.

Measured against the live EDCS place index (23,707 places, 542,854 index
entries) **[reproduced]**:

| | records | share |
|---|---:|---:|
| index entries | 542,854 | 100% |
| no geo id | 3,481 | 0.6% |
| geo id but no coordinates | 14,200 | 2.6% |
| inside `ROMAN_BOUNDS` | 474,899 | 87.5% |
| **outside `ROMAN_BOUNDS` — deleted** | **50,274** | **9.3%** |

Provinces lost, in full or near-full:

| lost | province |
|---:|---|
| 19,247 | Britannia |
| 11,855 | Germania inferior |
| 9,874 | Germania superior |
| 8,041 | Belgica |
| 687 | Barbaricum |
| 479 | Belgica / Germania inferior |
| 74 | Aegyptus |

Worst-hit sites: Mainz/Mogontiacum 5,668 · Bavay/Bagacum 5,170 ·
Köln 2,615 · London/Londinium 2,472 · Vechten/Fectio 2,151 ·
York/Eboracum 1,036 · Vindolanda 1,199.

Vindolanda and Mogontiacum are not marginal to Roman epigraphy. A referee who
searches for a Britannic term and gets zero results will not conclude the corpus
is bounded — they will conclude the corpus is wrong. (T10)

### D2 — Non-geolocated records are dropped before search, not just before mapping
`edcs_streamlit_map.py:212`, `dropna(subset=["latitude","longitude"])`.

`load_all_inscriptions()` is the **only** source for `filter_inscriptions()`.
So D1 and D2 together remove ~12.5% of records from *search* as well as from the
map. An inscription from York is not merely unplotted — it cannot be found.

The full dataset *is* loaded separately by `load_full_cleaned_data()`
(line 224), but only to decorate rows already matched in the reduced set
(line 636). It never widens the match set. (T11)

### D3 — Failed pages are skipped, and every subsequent resume is misaligned
`edcs_scraper.py:419-423`.

On `json.JSONDecodeError` the scraper prints a line and does `start += page_size`,
discarding up to 500 monuments. The loss compounds: `count_local_records()`
(line 330) counts *unique `edcs_id`s actually written*, while resume computes
`start = local_count` (line 578). After any skipped page, `local_count` is
smaller than the true API offset, so the next run restarts at the wrong offset
and re-walks or misses a whole region of the corpus. Nothing records that this
happened. (T19)

### D4 — `except requests.exceptions.RequestException` retries forever
`edcs_scraper.py:415-418`. Sleeps 15 s and `continue`s with no attempt counter
and no backoff. A persistent 403 — i.e. the current state of the world — is
retried indefinitely. Only the connect-phase probe (line 508) exits. (T19, T27)

### D5 — Monuments with no inscriptions become one empty row
`edcs_scraper.py:304-324`. Deliberate and defensible, but the resulting row has
`inscription_text = ""`, which downstream becomes `is_unreadable = True`
(cleaner line 152) and then `pd.NA` (line 192). These rows are indistinguishable
in the output from monuments whose text was lost. No flag marks the difference.

### D6 — Non-list inscriptions are skipped without a count
`edcs_scraper.py:261-262`, `if not isinstance(insc, list): continue`. Silent.

### D7 — Rows are dropped to nine columns for the map
`edcs_streamlit_map.py:31-45, 204`. `REQUIRED_COLS` excludes `is_forged`,
`is_unreadable`, `category`, `category_en`, `belege`/`evidence`, `image_urls`,
`material`, `inscription_index`. Memory-motivated and reasonable, but it means
the map cannot filter on forgery or readability even once those flags work.

### D8 — Empty-string→NA replacement runs across every column
`edcs_cleaner.py:192`, `cleaned.replace(r"^\s*$", pd.NA, regex=True)`.
Applied indiscriminately, including to numeric and boolean columns. (T37)

---

## 3. Where records are mutated

### M1 — `main.py:111` renames `belege` → `evidence`
This is the single most confusing line in the pipeline. It silently repurposes
the bibliographic-citation column under the name the cleaner uses for its
forgery test. See F1.

### M2 — The 13-step cleaning pipeline
`edcs_cleaner.py`, applied twice per row (conservative + interpretive). Every
step rewrites `inscription_text`. Verified misbehaviours **[all reproduced]**:

| Step | Input | Output | Issue |
|---|---|---|---|
| 10 | `atque` | `at que` | non-enclitic `-que` split (T05) |
| 10 | `neque` | `ne que` | " |
| 10 | `usque` | `us que` | " |
| 10 | `quinque` | `quin que` | " |
| 10 | `cuiusque` | `cuius que` | " |
| 10 | `denique` | `deni que` | " |
| 10 | `itaque` | `ita que` | " (not in the brief) |
| 10 | `quisque` | `quis que` | " (not in the brief) |
| 10 | `utque` | `ut que` | " (not in the brief) |
| 10 | `populusque` | `populus que` | **correct** — a real enclitic |
| 11 | `IIvir` | `II vir` | intended |
| 11 | `sevir` | `sevir` | **not** split — inconsistent (T06) |
| 11 | `duovir` | `duovir` | **not** split (T06) |
| 6/7 | `L(ucius) Aemilius +++ vixit an[nos] ++` | `L Aemilius +++ vixit an ++` | `+` survives as a token (T07) |
| 8 | `vixit annos XXV … anno 2024` | `vixit annos XXV et menses III anno` | digits stripped, Roman numerals kept (T08) |
| 4/9 | `((sestertium)) HS ((mille))` | cons `HS` / interp `sestertium HS mille` | **correct** — the brief's example works |
| 4/9 | `(A(uli) f(ilius))` | cons `f` | **nested** parens mis-parsed (T09) |
| 4/9 | `[[Domitiani]] erasa` | cons `erasa` / interp `Domitiani erasa` | erasure markup lost silently (T09) |

Step 10's stoplist must not be a blanket exclusion — `populusque` shows the rule
is right for genuine enclitics. Three forms beyond the brief's list are also
affected (`itaque`, `quisque`, `utque`), so the stoplist needs a lexicon, as T05
specifies, not an ad-hoc list.

T09 also needs a sharper example than the brief gives. `((sestertium))` parses
**correctly** — `\([^)]*\)` consumes `(sestertium)` and leaves a bare pair of
outer parens that step 9 removes. The rule breaks on genuine *nesting*:
`(A(uli) f(ilius))` is split into `(A(uli)` + `(ilius)` because the regex stops
at the first `)`, leaving a stray `f` that conservative cleaning should have
dropped entirely.

Note the laundering mechanism precisely: `step4_*` leaves unmatched delimiter
residue, and `step9_unclosed_brackets` then deletes every remaining bracket
character. The output is clean-looking text with no marker that anything was
mis-parsed. This is why T09 requires a warnings log, not just a better regex.

### M3 — Coordinate order is assumed, never checked
`edcs_scraper.py:237-238`: `longitude = coord[0]`, `latitude = coord[1]`.

Correct for the old API. **The current EDCS `places.json` returns
`[latitude, longitude]`** — verified against Roma `[41.89332, 12.48293]` and 22,007
places whose field-0 range (11.90…61.13) is only consistent with latitude first.
Porting the parser without fixing this transposes the corpus; the bounds filter
at D1 then deletes almost all of it, silently. (T34)

---

## 4. Counts reported to the user

Every user-visible number in the pipeline, and whether it can be trusted.

| # | Location | Reported | Trustworthy? |
|---|---|---|---|
| C1 | `edcs_scraper.py:517` | `Total in EDCS: {recordsTotal}` | endpoint is 403 — unobtainable |
| C2 | `edcs_scraper.py:468-473` | live offset / monuments / rows / % / ETA | offset drifts after any D3 skip |
| C3 | `edcs_scraper.py:487-489` | monuments saved, rows, last EDCS ID | counts writes only; excludes D3/D6 losses |
| C4 | `edcs_scraper.py:569-571` | local monuments vs EDCS total | equality assumed; D3 breaks it |
| C5 | `edcs_cleaner.py:158` | `Forged: {n}` | reported as always 0; logic is sound — see F1 |
| C6 | `edcs_cleaner.py:158` | `Unreadable: {n}` | correct, but conflates D5 rows |
| C7 | `edcs_cleaner.py:169-179` | empty-text summary before/after NA | correct |
| C8 | `edcs_cleaner.py:181-190`, `195-204` | missing-value and null reports | correct |
| C9 | `edcs_streamlit_map.py:599` | **`Matches for "{term}": {n}`** | **over the D1+D2 subset only** |
| C10 | `edcs_streamlit_map.py:411` | **PNG `Results: {n}`** | same defect, and it is exported |

C9 and C10 are the serious ones. They are the numbers a reader takes from a
figure, and both are computed on a corpus that has already lost ~12.5% of its
records with no indication on the figure that any filtering occurred. C10 is
worse than C9 because the PNG leaves the application and gets cited. (T11)

### F1 — `is_forged`: not dead code, but untested code on an accidental data path

**The brief's diagnosis is wrong, and the correction makes T04 much easier.**

The brief states `evidence` is never emitted, so the flag can never fire.
In fact `main.py:111` renames `belege` → `evidence`, so `clean_data` *does*
receive the column — holding bibliographic citations as Python lists.
`edcs_cleaner.py:153` stringifies them and tests for `*`.

And `*` is exactly how EDCS marks *falsae*. Verified against
`/data/indexes/citations/875.json` (CIL) on 2026-08-19: **4,719 CIL citation
numbers carry the marker**, formatted as a **prefix** — `*00226`, `*00641,5`,
`*01088,226` — not a suffix. Confirmed independently by `sources.json`, which
cites Gregori–Papini on "CIL VI 990* and CIL VI 991*", the standard convention
for forged inscriptions.

So the logic is sound and the marker is real **[reproduced: `is_forged`
correctly returns `[True, False]` for `["CIL 06 *00226"], ["CIL 06 00002"]`]**.
Why then does every run print `Forged: 0`?

Three candidate causes, in order of likelihood, none yet distinguishable because
no corpus is committed:

1. The old `/api/query` `obj.belege` may not have carried the `*` at all — the
   new API exposes it in the citation *index*, and whether the per-monument `q`
   field preserves it is unverified (none of the 50 fixtures carry one, which is
   equally consistent with forgeries simply being rare).
2. `parse_belege` filters with `if p` and truncates to `entry[:3]` (line 216),
   which could drop a marker held in a later element. (T36)
3. Genuine absence in whatever subset was harvested.

There is also a second, independent route to a permanent `False`: the
`if "evidence" not in cleaned.columns` guard at line 147 means running the
cleaner on scraper output *directly*, without `main.py`'s rename, silently
creates an empty column. Both paths need closing.

**Revised T04:** this is no longer "find the field or delete the flag". The
field exists and the test works. The work is to (a) confirm whether `q` preserves
the marker per-monument, (b) emit an explicit `is_forged` input from the scraper
instead of depending on a rename that reads like a mistake, and (c) test it.
The **[DECISION]** the brief anticipated — what to do if the API no longer
exposes forgery — does not arise. It does.

---

## 5. Other findings for later phases

- **Runtime dependency on Lat-Epig.** `edcs_streamlit_map.py:18` pulls shapefiles
  and the Hanson 2016 cities CSV from `raw.githubusercontent.com/mqAncientHistory/Lat-Epig/main`
  at runtime. We depend on the repository we argue is unmaintained. (T30)
- **`@st.cache_data` on `build_png_bytes`** (line 329) hashes a full DataFrame per
  call, with `ttl=3600` silently expiring. (T39)
- **`parse_belege` truncates to `entry[:3]`** (line 216), discarding any further
  elements. The new `sources.json` would support proper normalisation. (T36)
- **Citation metadata contradicts itself**: README says `(n.d.)`, `PNG_CITATION`
  (line 25) says `(2026) Version 1.0`. (T21)
- **Stale naming**: `pyproject.toml` `name = "edcs-analytics"`, scraper docstring
  header `EDCS-Analytics/`, `main.py:1` "EDCS Analytics". (T32)
- **`requires-python = ">=3.13"`** blocks installation where geopandas wheels lag. (T33)
- **Spoofed identity**: Firefox UA, forged `Referer` to `/en/search` (now 404),
  `X-Requested-With` (`edcs_scraper.py:65-71`). Confirmed to buy no access
  whatsoever — six header variants all return the same 403. (T26)
- **No provenance fields** on any record: no `retrieved_at`, `source_url`, or
  version. Nothing in an existing corpus file identifies which EDCS it came
  from — which, given EDCS has now been rebuilt twice, means existing harvests
  cannot be dated from their contents. (T17)

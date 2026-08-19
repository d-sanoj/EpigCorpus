# LatEpig 2.0 against the current EDCS — reproduction and evidence

**Verdict: LatEpig is broken against the current EDCS. So is EpigCorpus.**

Read §4 before using this document to frame the paper. The claim "LatEpig no
longer harvests EDCS" is confirmed, but it is no longer a claim that
distinguishes us, because the EDCS release of 2026-08-07 broke our harvester too.

---

## 1. What was tested

| | |
|---|---|
| Repository | `mqAncientHistory/Lat-Epig` |
| Commit | `91559166a041529554cc6d2c2785dd5e8d64ab32` |
| Commit date | 2024-06-18 11:18:36 +1000 ("updating citing doi") |
| Cited as | Ballsun-Stanton, Heřmánková & Laurence 2024 |
| Run at | 2026-08-19 16:10:57 UTC (traceback), 16:11:10 UTC (detail probe) |
| Python | 3.12, clean venv |
| Deps | `mechanicalsoup`, `beautifulsoup4`, `lxml`, `clint`, `yaspin` |

The repository has had no commits since June 2024.

## 2. Reproduction

```bash
git clone https://github.com/mqAncientHistory/Lat-Epig.git
cd Lat-Epig
python -m venv .venv && .venv/bin/pip install mechanicalsoup beautifulsoup4 lxml clint yaspin
```

```python
import sys, argparse
sys.path.insert(0, "src")
from lat_epig.parse import scrape

args = argparse.Namespace(
    EDCS='76700107', publication=None, province=None, place=None, operator='and',
    term2=None, dating_from=None, dating_to=None, inscription_genus=None,
    and_not_inscription_genus=None, to_file=None, from_file=None,
    debug=True, term1='%')
scrape(args, prevent_write=True, show_inscription_transform=True)
```

The search arguments are LatEpig's own — taken verbatim from its committed test
suite, `src/lat_epig/test_inscriptions.py:9`. This is a query the project itself
asserts should work.

## 3. Observed failure

```
Traceback (most recent call last):
  File "run_latepig.py", line 12, in <module>
    out = scrape(args, prevent_write=True, show_inscription_transform=True)
  File ".../yaspin/core.py", line 226, in inner
    return fn(*args, **kwargs)
  File ".../Lat-Epig/src/lat_epig/parse.py", line 412, in scrape
    br.select_form('[name="epi"]')
  File ".../mechanicalsoup/stateful_browser.py", line 241, in select_form
    raise LinkNotFoundError()
mechanicalsoup.utils.LinkNotFoundError
```

`parse.py:410` opens the legacy search form; `parse.py:412` selects it. The
open succeeds at the HTTP level but lands on an error page, so no form exists to
select. Instrumenting the same call:

```
requested : http://db.edcs.eu/epigr/epi.php?s_sprache=en
redirects : [(301, 'https://edcs.hist.uzh.ch/de/epigr/epi.php?s_sprache=en')]
final url : https://edcs.hist.uzh.ch/de/epigr/epi.php?s_sprache=en
status    : 404
ctype     : text/html; charset=iso-8859-1
length    : 319
title     : 404 Not Found
forms found: []
```

## 4. The API contract change that causes it

LatEpig drives the **legacy HTML form interface** at
`db.edcs.eu/epigr/epi.php`, using `mechanicalsoup` to fill `<form name="epi">`
and submit it (`parse.py:401-441`). That interface is gone. `db.edcs.eu` now
301s to `edcs.hist.uzh.ch`, and every legacy path under it returns 404.

The breakage is not subtle or intermittent: there is no form on the page,
because there is no page. The failure mode is also unhelpful — `mechanicalsoup`
does not raise on a 404, so the error surfaces as `LinkNotFoundError` from the
form selector rather than as an HTTP error, which is why it reads as a parsing
bug rather than a dead endpoint.

## 5. The finding that changes the paper's framing

**EpigCorpus's harvester is equally broken, as of the same EDCS release.**

Running our own scraper, unmodified, on 2026-08-19:

```
$ uv run python src/edcs_scraper.py
[+] Connecting to EDCS API...
[!] Page size 500 failed: 403 Client Error: Forbidden for url: https://edcs.hist.uzh.ch/api/query?...
[!] Page size 100 failed: 403 Client Error: Forbidden for url: https://edcs.hist.uzh.ch/api/query?...
[!] Could not connect to EDCS API. Check your internet connection.
```

`/api/query` — the endpoint the whole project is built on, and the endpoint whose
discovery is the paper's stated contribution — returns 403. It is absent from the
current frontend bundle. `/en/search`, the `Referer` we send, returns 404.

Note also that the scraper's diagnostic is wrong: it reports a connection
problem. There is nothing wrong with the connection. This is the "fail loudly"
problem from ground rule 3 appearing in the one place where a silent
misdiagnosis costs most.

Both tools broke for the same underlying reason, roughly two years apart:
each was written against whatever interface EDCS happened to expose, and EDCS
rebuilt. LatEpig targeted the PHP form interface and was broken by the move to
the DataTables API. We targeted the DataTables API and were broken by the move
to static files.

### Timeline

| Date | Event |
|---|---|
| 2024-06-18 | Last LatEpig commit |
| (before 2026-08) | EDCS retires `db.edcs.eu` PHP forms → **LatEpig breaks** |
| — | EpigCorpus written against `/api/query` |
| 2026-08-07 | EDCS release `20260807-142626` retires `/api/query` → **EpigCorpus breaks** |
| 2026-08-19 | Both failures reproduced here |

## 6. What survives, and why this is recoverable

The new architecture is *more* harvestable, not less — see `docs/EDCS_API.md`.
The entire corpus index, 542,854 entries, is one static 19 MB file served in
1.2 seconds, with no pagination, no checkpointing and no rate limiting. Full
inscription text still requires per-monument fetches.

Critically, **the data model EpigCorpus chose is still right**. Monuments still
carry an array of inscriptions (`d.i`), so the one-row-per-inscription decision
and the `EDCS-xxxxxxxx-n` record_id scheme port over directly. What must be
rewritten is the transport layer and the field names, not the model.

Two adaptations are non-negotiable on porting:

1. **`coord` order flipped.** The old API gave `[longitude, latitude]`; the new
   `places.json` gives `[latitude, longitude]`. `parse_monument` hardcodes the
   old order. Porting without fixing this transposes the entire corpus. (T34)
2. **Coordinates now require a join.** They live in `places.json` keyed by geo
   id, not inline on the record.

## 7. Honest framings available to the paper

The "we work where LatEpig doesn't" framing is no longer available on today's
EDCS. Three that are:

1. **The fragility itself is the finding.** Two independent harvesters, built by
   different teams against the same database, both dead within two years, each
   killed by an interface change. That is a reproducibility argument about
   epigraphic infrastructure, and it is stronger and more honest than a tool
   comparison. It also motivates the provenance work in Phase 3 rather than
   treating it as hygiene.
2. **Be the first harvester for the 2026 EDCS.** Nothing published targets the
   static-file architecture. Getting there first, with the API documented (we
   have that already) and a snapshot deposited, is a real data-paper
   contribution — and the new architecture makes a *complete* corpus snapshot
   genuinely cheap, which the old one did not.
3. **Quantify what the old snapshots now miss.** T24's SDAM comparison still
   works and gets more interesting: SDAM's deposits describe an EDCS that has
   since been rebuilt twice.

**This needs your decision before Phase 1.** Phases 1–7 assume a working
harvester against `/api/query`; that assumption is void, and the port changes
what several tasks mean (T17–T20 in particular).

# Phase 0 — Reproduce before changing

**Purpose.** Establish that the extraction base is fixed and re-derivable
before any correction, split, or model is built on it. Nothing here is a
finding about Latin; it is a finding about whether the foundation moves.

**Run date.** 2026-08-20 (UTC). Commit `537c159`, working tree clean for all
tracked files. Environment recorded in `reports/env/phase0_environment.md`.

---

## 0.1 Result

**CONFIRMED. 1,424,314 pairs from 588,509 records.** The brief's stop
condition is not triggered.

| quantity | required by brief | measured this session | method |
| --- | --- | --- | --- |
| source records | 588,509 | **588,509** | independent `json.loads` pass, not the probe |
| extracted pairs | 1,424,314 | **1,424,314** | independent `csv.reader` parse of the TSV |

Both figures were re-derived by a verification script that never reads
`reports/abbrev_probe.md`, so neither number is quoted from the artifact it
is meant to validate.

## 0.2 The input is the committed input

| artifact | sha256 |
| --- | --- |
| `data/edcs_inscriptions.jsonl` (working, gitignored) | `9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317` |
| `data/edcs_inscriptions.jsonl.gz` (committed, decompressed) | `9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317` |

Identical. A fresh clone reconstructs the exact bytes consumed here; no
reviewer depends on an uncommitted working file. Matches D-0001.

## 0.3 Record-level accounting — nothing was silently dropped

| check | value |
| --- | --- |
| lines in raw file | 588,509 |
| lines that parsed as JSON | 588,509 |
| lines that failed to parse | **0** |
| distinct `record_id` | 588,509 |
| distinct EDCS base id (segment suffix stripped) | 542,854 |
| records missing any of the five required fields | **0** |

No record is lost to a parse failure or a missing field, so the pair count
cannot be inflated or deflated by silent skipping.

## 0.4 TSV integrity — the line count and the record count cannot diverge

| check | value |
| --- | --- |
| data rows by `csv.reader` | 1,424,314 |
| raw lines by `awk` (minus header) | 1,424,314 |
| rows with the wrong column count | **0** |
| rows containing an embedded CR or LF | **0** |
| distinct `inscription_id` appearing in the TSV | 337,744 |
| TSV ids absent from the raw corpus | **0** |

## 0.5 Determinism

Re-ran the extraction **unmodified** four times. Every output byte-identical
to the prior session's frozen copies in `reports/baseline_v0/` and
`data/derived/abbrev_pairs_v0_prior_session.tsv`.

| run condition | `abbrev_pairs.tsv` sha (first 16) | `abbrev_probe.md` sha (first 16) | pairs |
| --- | --- | --- | --- |
| default | `fabb6e0e5b3a7cf4` | `7799f9b8bbd9b7b0` | 1,424,314 |
| `PYTHONHASHSEED=1` | `fabb6e0e5b3a7cf4` | `7799f9b8bbd9b7b0` | 1,424,314 |
| `PYTHONHASHSEED=12345` | `fabb6e0e5b3a7cf4` | `7799f9b8bbd9b7b0` | 1,424,314 |
| `PYTHONHASHSEED=random`, different cwd | `fabb6e0e5b3a7cf4` | — | — |

Set-iteration order does not leak into any output. Wall time: **14.7 s**
single-threaded for the full 566 MB corpus.

---

## 0.6 Self-adversarial pass (R7)

**Most likely reviewer attack:** *"Re-running the same script on the same
machine demonstrates determinism, not correctness. If the extractor has a
bug, you have reproduced the bug."*

This is correct and the byte-identity check above does not answer it. So a
**second extractor was written independently** — from the EDCS convention
rather than from `scripts/abbrev_probe.py` — and its output diffed against
the primary at the level of the (record_id, abbrev, expansion) multiset.

| | pairs |
| --- | --- |
| primary probe | 1,424,314 |
| independent second implementation | 1,471,175 |
| **agreement (multiset intersection)** | **1,424,238 — 99.995% of the primary** |
| only in the second implementation | 46,937 |
| only in the primary probe | 76 |

**Outcome: the attack fails, and it fails in the direction that favours the
primary extractor.** Both disagreement sets trace to defects in the *second*
implementation, not the first:

- **The 76 pairs the primary found and the second missed** were each
  mechanically classified. 74 are cases where the crude second version
  retained trailing punctuation or a combining diacritic that the primary
  normalises away (`q̅(uaestori),` → the primary yields `q̅ → q̅uaestori`; the
  second yields the same pair with a comma glued on). The remaining 2, both
  in `EDCS-00000567-0`, are because the second version does not treat the
  interpunct `·` as a token boundary and emits the fused `Mac·I·h →
  Macedonicae·I·hastatus` where the primary correctly emits `Mac →
  Macedonicae` and `h → hastatus`. Zero disagreements are unexplained.

- **The 46,937 extra pairs the second implementation produced** are
  bracket-contaminated fragments its cruder masking let through — `[3 qui
  frumen]t(o)` yielding the meaningless `t → to`, `[vi]x(it)` yielding `x →
  xit`. The primary correctly excludes these. This is independent evidence,
  arrived at accidentally, that the `inside_bracket_markup` exclusion
  audited in Phase 1 is suppressing real label noise and is not merely
  discarding data.

**What this does and does not establish.** It establishes that the primary
extractor contains no coding defect large enough for a separately-written
implementation to detect. It does **not** establish that the shared reading
of the EDCS convention is right — a misunderstanding present in both
implementations would be invisible to this check. That question is Phase 1's,
and is exactly why the exclusion audit is scoped as the highest-value phase.

---

## 0.7 Observations carried forward (not Phase 0 findings)

- **42,538 records contain `(` but contribute no pair.** 380,282 records
  contain an opening parenthesis; only 337,744 appear in the TSV. The
  difference is the whole exclusion surface Phase 1 must account for. This
  is a measured count, not yet an explanation.
- **U+0305 (combining overline) occurs in the corpus.** Seen in
  `EDCS-00000939-0`, `q̅(uaestori)`. *Impression, 1 example examined* — it is
  not counted here and must not be quoted as a rate. Phase 2 measures it.

## 0.8 Reproduce this phase

```bash
.venv/bin/python scripts/abbrev_probe.py
shasum -a 256 data/derived/abbrev_pairs.tsv reports/abbrev_probe.md
```

Expected: `fabb6e0e…` and `7799f9b8…`.

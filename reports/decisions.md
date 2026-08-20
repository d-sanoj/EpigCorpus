# Provenance log

One entry per decision. Every later step cites these IDs. Each entry records
what was decided, the evidence, alternatives rejected, and what would
overturn it.

Status vocabulary: **SETTLED** (evidence in hand) · **ASSUMPTION** (stated,
falsifiable, not yet tested) · **UNRESOLVED** (known gap, not imputed).

---

## D-0001 — The input corpus is fixed and identified by hash

**Decided.** All work in this session runs against
`data/edcs_inscriptions.jsonl`, 588,509 records,
sha256 `9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317`.

**Evidence.** Computed this session:

```bash
wc -l data/edcs_inscriptions.jsonl        # 588509
shasum -a 256 data/edcs_inscriptions.jsonl
gunzip -c data/edcs_inscriptions.jsonl.gz | shasum -a 256
```

The working `.jsonl` (gitignored) and the committed `data/edcs_inscriptions.jsonl.gz`
hash **identically**. A fresh clone therefore reconstructs the exact bytes this
session consumed; no reviewer is dependent on the uncommitted working file.

**Alternatives rejected.** Using `data/edcs_inscriptions_cleaned.jsonl` as the
base. Rejected: the cleaner (`src/edcs_cleaner.py`) applies its own rewrite
rules, so probing it would measure the cleaner and EDCS together, and the
brief forbids modifying `src/`. The raw harvest is the only base with a
documented upstream.

**Overturned by.** A re-harvest from EDCS (the corpus is live upstream and
grows). Any change to the hash invalidates every number in this session.

**Status.** SETTLED.

---

## D-0002 — Step 0 reproduction: the extraction is bit-reproducible, not merely count-stable

**Decided.** The base of 1,424,314 pairs is confirmed and the pipeline is
treated as a fixed, non-moving foundation for Steps 1-6.

**Evidence.** `scripts/abbrev_probe.py` was re-run **unmodified**. Both output
artifacts are byte-identical to the prior session's:

| artifact | sha256 (prior session) | sha256 (this session) | result |
| --- | --- | --- | --- |
| `data/derived/abbrev_pairs.tsv` | `fabb6e0e5b3a7cf4…` | `fabb6e0e5b3a7cf4…` | identical |
| `reports/abbrev_probe.md` | `7799f9b8bbd9b7b0…` | `7799f9b8bbd9b7b0…` | identical |

Pair count re-derived from the TSV **independently of the report** — by `awk`
line count and by a `csv.reader` parse — both give **1,424,314**, matching the
report's headline. The TSV has 0 rows with a wrong column count and 0 rows with
embedded newlines, so the naive line count and the parsed record count cannot
diverge.

The brief's stop condition ("if the count differs, STOP") is not triggered.

**Alternatives rejected.** Accepting the prior report's stated 1,424,314 at
face value. Rejected under standing rule 1: the figure is now traceable to a
computation in this session.

**Overturned by.** A change to `data/edcs_inscriptions.jsonl` (see D-0001) or
any edit to `scripts/abbrev_probe.py`.

**Status.** SETTLED.

---

## D-0003 — Reproduction is not hash-seed dependent

**Decided.** The "deterministic, seeded" constraint is satisfied by the
extraction as written; no seed pinning is required for Step 0.

**Evidence.** A byte-identical re-run on the same machine proves the script is
a pure function of its input, but does **not** by itself prove determinism —
Python's `PYTHONHASHSEED` randomises `set` iteration order per process, and the
probe iterates sets when building report tables. Re-ran under
`PYTHONHASHSEED=1` and `PYTHONHASHSEED=12345`:

| seed | `abbrev_probe.md` sha | `abbrev_pairs.tsv` sha | pairs |
| --- | --- | --- | --- |
| (unset) | `7799f9b8bbd9b7b0` | `fabb6e0e5b3a7cf4` | 1,424,314 |
| 1 | `7799f9b8bbd9b7b0` | `fabb6e0e5b3a7cf4` | 1,424,314 |
| 12345 | `7799f9b8bbd9b7b0` | `fabb6e0e5b3a7cf4` | 1,424,314 |

Unchanged. Every set is sorted before rendering, or is only counted.

**Alternatives rejected.** Asserting determinism from a single re-run.
Rejected: that is the exact check a reviewer would call circular.

**Overturned by.** Introducing an unsorted set/dict traversal into report
rendering in later steps. Steps 3-4 must re-run this seed check before
publishing new headline numbers.

**Status.** SETTLED.

---

## D-0004 — 588,509 records are NOT 588,509 independent inscriptions

**Decided.** The corpus unit is a *text segment*, not a monument. Record counts
must not be described as inscription counts without qualification, and any
future train/test split must group by EDCS base id, not by `record_id`.

**Evidence.** Computed this session. `record_id` has the form
`EDCS-<8 digits>-<segment>`:

| quantity | value |
| --- | --- |
| records | 588,509 |
| distinct `record_id` | 588,509 (no duplicate keys) |
| distinct EDCS base id (segment suffix stripped) | **542,854** |
| records sharing a base id with another record | 45,655 |
| multi-segment groups | 30,503 |

Segment-suffix distribution: `-0` 542,854 · `-1` 30,503 · `-2` 7,351 ·
`-3` 2,977 · `-4` 1,297 · `-5` 757 · `-6` 513 · `-7` 336 · (tail continues).

Members of a multi-segment group are related faces or panels of one monument,
not unrelated texts: **100.0% of the 30,503 groups have all members in the same
province**, and 299 groups (1.0%) have members with byte-identical
`inscription_text`.

**Why this is a label-noise and leakage finding, not bookkeeping.** In
`EDCS-00000245`, segments `-0` and `-2` carry the *same* imperial titulature
expanded differently by the editor:

```
-0: ... divi Traiani / Parthici nepos divi Nervae pronepos T(itus) ...
-2: ... divi Traiani / Parthic(i) nep(os) divi Nervae pronep(os) T(itus) ...
```

The stone-vs-expansion boundary here is an editorial choice, not a property of
the monument. This is first-hand evidence for the datasheet claim (Step 5) that
a model may learn EDCS conventions rather than Roman practice.

**Alternatives rejected.** (a) Deduplicating to one segment per base id —
rejected under standing rule 4, nothing is deleted; the grouping is recorded so
a consumer can choose. (b) Treating the prior reports' "588,509 inscriptions"
phrasing as harmless — rejected: it understates near-duplicate content.

**Overturned by.** Evidence that the EDCS segment suffix means something other
than a segment of one monument record. This reading is inferred from the data's
own structure, **not** from EDCS documentation — see the caveat below.

**Status.** SETTLED as a measurement (the counts and the province/text
agreement are computed). The *interpretation* of the suffix as "faces of one
monument" is ASSUMPTION — falsified if EDCS documents the suffix differently.
NEEDS CITATION: EDCS's own definition of the record-id segment suffix.

---

## D-0005 — Embedded CR/LF in `inscription_text` is handled, not lost

**Decided.** No action required; recorded so a reviewer does not re-flag it.

**Evidence.** 50 of 588,509 records (0.01%) contain a CR and 92 (0.02%) contain
an LF inside `inscription_text` (e.g. `EDCS-00000619-0`). `abbrev_probe.py:325`
normalises context whitespace with `re.sub(r"\s+", " ", s).strip()` before
writing, which is why a `csv.reader` parse of the TSV finds **0** rows with
embedded newlines. Tokenisation splits on whitespace, so a CR/LF acts as a
token boundary exactly as a space does. No pair is dropped and no row is
corrupted.

**Alternatives rejected.** Pre-normalising the raw file. Rejected: forbidden by
the brief and unnecessary.

**Status.** SETTLED.

---

## D-0006 — Phase 0 re-verified in the current session; D-0001..D-0003 stand

**Decided.** The 1,424,314-pair base is re-confirmed by computation performed
in *this* session, not inherited from the prior session's log. Phases 1-12
may build on it.

**Evidence.** Commit `537c159`, tracked tree clean, Python 3.13.13, pandas
3.0.1, numpy 2.4.3 (full package set in `reports/env/phase0_environment.md`).

`scripts/abbrev_probe.py` re-run **unmodified**; `data/derived/abbrev_pairs.tsv`
and `reports/abbrev_probe.md` are byte-identical to the frozen prior copies
(`fabb6e0e…`, `7799f9b8…`). Counts were then re-derived *independently of the
report* by `scripts/phase0_verify.py`: 588,509 records by a fresh JSON pass
(0 parse failures, 0 missing required fields) and 1,424,314 pairs by
`csv.reader` (0 malformed rows, 0 embedded newlines, 0 TSV ids absent from
the raw corpus). Working `.jsonl` and committed `.jsonl.gz` hash identically.

Determinism re-tested this session under `PYTHONHASHSEED` 1, 12345 and
`random`, and from a different working directory: all outputs unchanged.

**Alternatives rejected.** Accepting D-0002 as already settled. Rejected
under R1 — a decision log entry written by an earlier session is a claim, not
a computation, and the standing rule requires the number to be traceable to
work done here. The re-run cost 15 s; the alternative cost credibility.

**Overturned by.** Any change to `data/edcs_inscriptions.jsonl` (D-0001) or
to `scripts/abbrev_probe.py`.

**Status.** SETTLED.

---

## D-0007 — Extractor correctness is corroborated by a second implementation, not by re-running the first

**Decided.** The primary extractor is treated as free of *coding* defects at
the 10⁻⁴ level. Its *interpretive* correctness remains open and is delegated
to Phase 1.

**Evidence.** `scripts/phase0_second_implementation.py` is a separately
written extractor built from the EDCS convention rather than from
`abbrev_probe.py`. Multiset comparison on (record_id, abbrev, expansion):

| | pairs |
| --- | --- |
| primary | 1,424,314 |
| second implementation | 1,471,175 |
| agreement | 1,424,238 (99.995% of primary) |
| only in second | 46,937 |
| only in primary | 76 |

All 76 primary-only pairs were mechanically classified by
`scripts/phase0_explain_disagreements.py`: 74 are trailing-punctuation or
combining-diacritic noise the second version fails to normalise, 2 are the
second version failing to treat the interpunct `·` as a token boundary
(`EDCS-00000567-0`). **Zero unexplained.** The 46,937 second-only pairs are
bracket-contaminated fragments (`[3 qui frumen]t(o)` → `t → to`) that the
primary correctly rejects.

**Why this matters beyond Phase 0.** The 46,937 figure is incidental
independent support for the `inside_bracket_markup` exclusion being a genuine
noise filter rather than arbitrary data loss — but it is *not* a measurement
of that exclusion and must not be quoted as one. Phase 1 measures it properly.

**Alternatives rejected.** Treating byte-identical re-runs as sufficient.
Rejected: reproducing a bug reproduces the bug. This is the single most
likely reviewer attack on Phase 0 and needed an answer that does not reuse
the code under test.

**Limitation, stated plainly.** A misreading of the EDCS convention shared by
both implementations is invisible to this check. Nothing in Phase 0 rules
that out.

**Overturned by.** A third implementation, or a Latinist review of the
convention, disagreeing materially. [VERIFY — LATINIST]

**Status.** SETTLED as to coding correctness. Interpretive correctness
UNRESOLVED, deferred to Phase 1.

---

## D-0008 — Two observations logged but deliberately not quantified in Phase 0

**Decided.** Recorded so they are not lost, and explicitly barred from any
table of computed figures until their own phase measures them.

**Evidence and status.**

1. **42,538 records carry `(` yet yield no pair.** 380,282 raw records
   contain an opening parenthesis; 337,744 distinct ids appear in the pair
   TSV. Both counts are computed (`scripts/phase0_verify.py`). The *reason*
   for the gap is not. This is the total exclusion surface Phase 1 must
   reconcile against its per-category counts — if the categories do not sum
   to something consistent with this, a category is missing.
   **Status:** SETTLED as a count, UNRESOLVED as an explanation.

2. **U+0305 combining overline is present in the corpus.** Observed in
   `EDCS-00000939-0` (`q̅(uaestori)`). **Impression, 1 example examined.** No
   rate, share, or prevalence claim is made or permitted from this. It
   raises the prior that Phase 2's vinculum scan will not be a pure negative
   result — which is precisely why Phase 2 must run before any numeral rule
   is written, rather than being skipped as an expected null.
   **Status:** UNRESOLVED, owned by Phase 2.

**Alternatives rejected.** Folding either into the Phase 0 result tables.
Rejected under R6 — one is an unexplained count and the other is an
eyeballed sighting; neither is a finding yet.

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

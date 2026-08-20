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

---

## D-0009 — Every published exclusion count is a first-match count; two categories are materially undercounted

**Decided.** `reports/exclusion_audit.md`'s ten category totals are retained
as *first-match* counts and relabelled as such. Two are corrected:
`editorial_marker_paren` 58,720 → **88,339 true membership**, `greek_script`
12,987 → **15,800 (+21.7%)**.

**Evidence.** `abbrev_probe.extract_pairs` applies ten tests in fixed order
and stops at the first failure, so a count is "caught first", not
"described by". `scripts/phase1_supplement.py` evaluates every test against
every token independently of order. The chain was re-implemented three times
across the supplement scripts and returned 1,424,314 kept / 342,714 dropped
each time, so the mirror is faithful.

The other eight ratios (up to 7069×) are **mechanical, not findings**: a
token overlapping a bracket span usually contains a literal `[`, which makes
it non-alphabetic and markup-carrying by construction; and EDCS writes
lacunae as `[3]`, so Arabic digits are almost all bracketed (1,130 of 1,150).
Greekness and empty-parenthesis-ness are the only two properties that hold
independently of bracketing, which is why only those two are real.

**Alternatives rejected.** Reporting the raw membership counts as
corrections. Rejected: it would claim 190,822 concealed symbol
abbreviations, which is false and would not survive one reviewer question.

**Overturned by.** Reordering the filter chain in `abbrev_probe.py`, which
would change every first-match count. This is a reason to freeze the
extractor.

**Status.** SETTLED.

---

## D-0010 — The undercount does NOT enlarge the abstention class; the prior recommendation stands

**Decided.** `editorial_marker_paren → RECOVER AS SEPARATE CLASS` stands. The
abstention pool is **42,805 → at most 45,288 (+5.8%)**, not +50%.

**Evidence.** `scripts/phase1_supplement2.py` sub-classifies the 29,606
hidden empty-paren tokens by the prior audit's own rule: 24,623 are `x(?)`
uncertain-reading marks, 2,432 standalone `(?)`, 44 `(!)`, 24 bare — and only
**2,483** are the "editor could not resolve it" abstention sub-class. Those
2,483 sit inside editorial restorations and carry the section-D circularity
risk, so the defensible increment is smaller still.

**Why this is logged as its own decision.** D-0009 found a real 50%
undercount, and the natural inference — that the headline recommendation
built on it grows by 50% — is wrong. Logging only D-0009 would leave that
inference open for a later phase to make.

**Alternatives rejected.** Quoting 88,339 as the abstention-class supply.
Rejected: it is the category total, not the sub-class, and the two differ by
a factor of two.

**Status.** SETTLED. I expected this check to overturn the prior audit and it
confirmed it.

---

## D-0011 — Circularity is written as a continuous restored-letter fraction, not a boolean

**Decided.** Phase 3's `circularity_risk` column carries the share of the
abbreviation's letters supplied by the editor: 0.0 = every letter carved,
1.0 = wholly editorial. Rows at 1.0 are barred from every test set (4e).

**Evidence.** Over 251,283 `inside_bracket_markup` tokens carrying ≥1
abbreviation letter: **63.25% (158,945) are fully editorial; only 1.35%
(3,401) are fully attested;** 35.39% partial.

Cross-validated against the prior audit by a different method: its sub-classes
"whole thing is editorial reconstruction" (149,582) + "abbreviation restored,
expansion outside the bracket" (9,359) + "abbreviation restored, expansion
straddles the bracket" (4) = **158,945**, exactly the count at fraction 1.0.

**Consequence.** The 253,256 "recoverable in principle" shrinks to **3,401**
tokens clean under the strictest reading. The 64,991 partly-restored middle
remains a Latinist's call. **[VERIFY — LATINIST]**

**Alternatives rejected.** A boolean flag. Rejected: it forces the 64,991
partial cases into one bucket or the other, which is the exact judgement the
prior audit correctly refused to automate.

**Status.** SETTLED as a measure. The threshold for test-set exclusion above
1.0 is UNRESOLVED and belongs to Phase 4.

---

## D-0012 — Bias verdicts require a bootstrap null; six of the prior audit's readings are withdrawn

**Decided.** TVD is reported against a bootstrapped null at the same n, never
against an absolute threshold. The prior audit's flat rule ("above about 0.15
is materially different") is withdrawn and its verdicts for six categories
must not be quoted.

**Evidence.** 200 draws from the kept province distribution at each category's
n, seed 20260820 (`scripts/phase1_supplement2.py`). TVD is upward-biased by
sampling noise, severely at small n — the null p95 is 0.012 at n=253,256 but
0.586 at n=19.

Withdrawn: `contains_numeral` 0.490 was "a different population", null p95
0.586 — **0.8×, indistinguishable from noise**. `nested_parens` at n=1 —
no claim possible. `non_alphabetic_expansion` 1.4×, `unbalanced_parens` 1.6×,
`token_carries_markup` 2.1× — weak to modest, not "materially different".
Confirmed and strengthened: `greek_script` **22.6×**, the most skewed
exclusion in the dataset; `editorial_marker_paren` 16.4×;
`inside_bracket_markup` 12.0×; `non_alphabetic_abbrev` 9.5×.

**No recommendation changes** — each small category was recommended on other
grounds.

**Also.** The prior summary's `bias risk if kept out` column conflates *is
the excluded set different* with *does excluding it bias the Latin task*.
For `greek_script` the answers are "emphatically yes" and "arguably no". The
paper must separate them.

**Alternatives rejected.** A chi-square or G-test. Rejected: at n=253,256
every difference is significant, so a p-value would answer a question nobody
asked. The null-TVD ratio reports effect size against noise, which is the
question.

**Status.** SETTLED.

---

## D-0013 — The `|` inventory is a separate resource but is NOT a symbol-to-word mapping

**Decided.** The 376 distinct `|(...)` forms (16,194 occurrences) are
released as a separate class. The brief's framing of it as "a symbol-to-word
mapping" is rejected on evidence.

**Evidence.** Full enumeration in `data/derived/phase1_supplement.json`. One
ASCII pipe stands for at least eight unrelated glyphs — centurial sign
(4,944), reversed C = *mulieris*/*Gaiae* (4,077), monetary/denarius (3,135),
milliary (1,429), fractions and weights (922), *obitus* theta nigrum (702),
Greek numerals and measures (591), Christian signs (5), plus 291 occurrences
across 64 forms left unclassified. Expanding `|` is therefore the same
context-disambiguation problem as expanding `V`, on a symbol vocabulary.

Three structural properties, each measured:
- **Plurality by repetition** — `||(mulierum)` 28, `||(centuriones)` 8,
  `||(librae)` 11, `|||(mulierum)`, `||||(milia)`. The same geminatio
  principle as `DD(ominis)`; Phase 3a must treat them as one phenomenon.
- **Inflectional ambiguity inside the symbol set** — ten inflections of
  *centurio* behind one glyph.
- **Its own abstention class** — `|()` / `||()`, 98 occurrences.

**Label noise, measured.** 56 forms have count ≤2 and Levenshtein distance 1
from a form with count ≥20 (`|(mulierus)`, `|(cenurionis)`, `|(mlliaria)`,
`|(denarrii)`). Ancient orthographic variation and editor keying errors
cannot be separated mechanically — **[VERIFY — LATINIST]** — but the count
establishes that editor-side noise exists in the gold labels at a measurable
rate, in the most formulaic corner of the corpus. It is a lower bound.

**Alternatives rejected.** Publishing a `|` → word lookup. Rejected: it would
be wrong for every form outside the majority family and would silently encode
a false claim about the database.

**Status.** SETTLED as an enumeration. The family grouping is a stated
rule-based pass and is **[VERIFY — LATINIST]**.

---

## D-0014 — 2,230 rasura tokens are misfiled and belong with `token_carries_markup`

**Decided.** The `⟦ ⟧` tokens inside the prior audit's "other non-letter
character" bucket (2,572) are re-assigned: **2,230 erasure, 304 `« »`, 46
other, 25 Greek, 4 combining diacritic.**

**Evidence.** `scripts/phase1_supplement.py`. `⟦Fl(avio) Constanti⟧no` is an
abbreviation that *was* carved and was later chiselled out — *damnatio
memoriae*, recorded by EDCS. That is the epigraphic opposite of an editorial
restoration: the letters are attested, not supplied, so `circularity_risk`
for these is 0.0, not 1.0. Recommendation for `token_carries_markup` becomes
RECOVER, 339 → **2,569**.

**Overturned by.** Evidence that EDCS uses `⟦ ⟧` for something other than
rasura. Inferred from the data's structure and from the Leiden conventions,
**not** from EDCS documentation. **[NEEDS CITATION]** — EDCS's own statement
of its bracket conventions.

**Status.** SETTLED as a count. The rasura reading is ASSUMPTION.

---

## D-0015 — The pooled exclusion reshapes the corpus along the candidate finding's own two axes

**Decided.** Flagged as the most serious open threat in the project.
Phase 3f must re-derive the province and century signal on the
**pre-exclusion** population as well as the kept set and report both.

**Evidence.** `scripts/phase1_supplement3.py`, all ten filters pooled.
Province TVD **0.131** against null p95 0.012 (**10.9×**); century TVD
**0.097** against 0.006 (**16×**); dropped texts mean 504 chars against 280
kept. The exclusion is reshaping the dataset, not filtering it.

The skew runs along exactly the axes the candidate finding is claimed on:

| | lift (drop share ÷ kept share) |
| --- | --- |
| Numidia — where *V = vixit* is claimed | **0.53** (least-excluded large province) |
| Asia / Achaia / Thracia / Cappadocia — Greek East | 2.4–3.9 |
| 1AD — where *C = Caius* is claimed | **0.71** |
| 5AD — where *C = clarissimo* is claimed | **1.51** |

A 2.1× differential across precisely the span over which `C` is said to shift,
and the province carrying the *vixit* reading survives filtering at roughly
twice the corpus rate.

**What this does and does not show.** It does **not** show the candidate
finding is an artifact. It shows the finding is currently measured on a
corpus whose relevant strata were unevenly thinned by a filter designed
without this hypothesis in view, and that a reviewer can say so in one
sentence. If the signal exists only in the kept set, it belongs to the
filter, not to Rome.

**Alternatives rejected.** Deferring the observation to Phase 6. Rejected:
Phase 4 freezes the splits, and a split built on a skewed base cannot be
un-skewed afterwards.

**Status.** UNRESOLVED. Owned by Phase 3f. The most important open question
in the project.

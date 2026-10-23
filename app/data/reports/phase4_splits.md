# Phase 4 — Dataset splits, seeded, documented, frozen

**Output.** `data/derived/v1/splits/` — 12 TSVs plus `split_manifest.json`
and `split_stats.json`. Built by `scripts/phase4_build_splits.py`, seed
`20260820`, from the frozen v1 table (sha256 `ba98ccac…`).

**Task population.** 1,421,743 of the 1,424,314 v1 rows. The 2,571
`NUMERAL_ELLIPSIS` rows stay in the release and are flagged out of the task
(D-0021) — nothing is deleted.

**Determinism.** Split assignment is `blake2b(SEED:base_id)` mapped to
[0,1). Not Python's `hash()`, which is salted per process; not a shuffle over
a list, which depends on the order rows happen to be read. Any split file
regenerates alone.

---

## 4a. Primary split — 80/10/10, grouped by monument

| split | rows | share | monuments |
| --- | --- | --- | --- |
| primary_train | 1,140,108 | 80.19% | 256,051 |
| primary_val | 139,591 | 9.82% | 31,664 |
| primary_test | 139,580 | 9.82% | 31,799 |

**The grouping unit is the EDCS base id, not `record_id`.** D-0004 established
that 588,509 records are only 542,854 monuments: the `-0`/`-1`/`-2` suffix
marks faces or panels of one stone, 100% of multi-segment groups share a
province, and 299 groups carry byte-identical text. Splitting on `record_id`
would put two faces of one monument on opposite sides of the line.

### Leak verification

| check | result |
| --- | --- |
| groups (monuments) in the primary split | 319,514 |
| **groups straddling train/val/test** | **0** |
| groups straddling the held-out province split | **0** |

**But grouping by monument does not stop all leakage, and this is the finding
of Phase 4.**

| | |
| --- | --- |
| test rows whose **exact** (abbrev, expansion, left context, right context) also occurs in train | **18,381 — 13.17% of the test split** |
| distinct context types shared | 10,534 |

Top offenders: `Leg → Legio` 715, `f → fecit` 618, `Of → Officina` 565,
`D → Dis` 451, `L → Luci` 448, `C → Cai` 447.

These are **different monuments carrying byte-identical text**. The corpus is
formulaic and the context window is 40 characters, so `Leg(io)` inside the
same surrounding string recurs across the empire. No grouping rule based on
identity can catch it, because the stones genuinely are different stones.

Whether that is leakage or the domain is a real question: an epigrapher
reading `D(is) M(anibus)` for the thousandth time is *also* doing lookup. So
the rows are **neither silently left in nor silently removed**. The primary
test split keeps them, and `test_no_context_duplicate` (121,199 rows) drops
them. **Phase 6 must report both**; the gap between them is the share of
apparent performance that is memorised string matching.

### Are the primary splits comparable? Yes — measured at the right unit

| statistic | observed | null p95 | ratio |
| --- | --- | --- | --- |
| province TVD, test vs train, **by pairs** | 0.0262 | 0.0103 | 2.6× |
| province TVD, test vs train, **by monuments** | **0.0126** | **0.0163** | **0.8×** |

The pair-level null is too generous and the apparent 2.6× is an artifact of
block structure: every pair on one stone shares that stone's province, so the
effective sample size is 31,799 monuments, not 139,580 pairs. **At the unit
the split is actually drawn on, the test split is indistinguishable from a
random draw of the training distribution.** This is the same correction
applied to the Phase 3f nulls (D-0025).

Century profiles confirm it directly:

| split | 1BC | 1AD | 2AD | 3AD | 4AD | 5AD |
| --- | --- | --- | --- | --- | --- | --- |
| primary_train | 5.1% | 26.9% | 36.7% | 19.1% | 8.2% | 1.7% |
| primary_val | 5.0% | 27.0% | 36.4% | 20.1% | 7.7% | 1.7% |
| primary_test | 4.4% | 26.8% | 37.4% | 18.9% | 8.0% | 1.8% |

---

## 4b. Held-out province split

**Chosen on stated criteria, before any split was drawn.** The requirement is
one province per *genre regime* — as separated by the `V` reading in Phase 3f
— across three widely separated geographic zones, at comparable size, each
with a **same-regime sister province left in training** so the test measures
transfer rather than absence.

| province | rows | share of task | monuments | genre regime (V reading) | zone | sister left in train |
| --- | --- | --- | --- | --- | --- | --- |
| **Britannia** | 26,606 | 1.87% | — | military — *victrix* 36%, *valeria* 30% | NW frontier | Germania sup./inf. |
| **Mauretania Caesariensis** | 24,997 | 1.76% | — | funerary — *vixit* 81% | North Africa | Numidia, Africa proc. |
| **Pannonia inferior** | 20,094 | 1.41% | — | votive — *votum* 79% | Danube | Pannonia sup., Dacia |

| split | rows | monuments |
| --- | --- | --- |
| heldout_province_train | 1,349,016 | 301,337 |
| heldout_province_test | 71,697 | 18,303 |

**5.04% of the task withheld.** Numidia was rejected despite being the
cleanest funerary case: at 7.72% it would have removed a tenth of training
and the dominant funerary province at once. Mauretania Caesariensis carries
the same regime at a fifth of the cost.

### This split confounds province with century, and Phase 6 must condition on it

| | 1BC | 1AD | 2AD | **3AD** | 4AD | 5AD |
| --- | --- | --- | --- | --- | --- | --- |
| primary_test | 4.4% | 26.8% | 37.4% | 18.9% | 8.0% | 1.8% |
| **heldout_province_test** | **0.0%** | **5.7%** | 34.1% | **47.1%** | 7.9% | 3.6% |

Province TVD 0.9494 is by construction — the provinces are disjoint. **Century
TVD 0.3022 is not**, and it is a genuine confound: the three withheld
provinces are frontier territory, Romanised late, and their material peaks in
the 3rd century where the corpus peaks in the 2nd. 1AD is 26.8% of the primary
test and **5.7%** of the held-out test.

A model that fails on the held-out provinces may be failing on the era, not
the geography. **Phase 6 must report held-out province results stratified by
century**, not pooled. Reported here rather than discovered there.

The confound is partly irreducible: genre regime and century are correlated in
the real world, because the frontier provinces where votive and military
formulae dominate were incorporated later than Italy. No choice of three
provinces removes it entirely.

---

## 4c. Lexical-only test set — and why it does not discriminate on its own

The prior figure was to be re-derived, not assumed. D-0024 did so: lexical
ambiguity is **34.5%** of all ambiguous keys but **54.5%** of keys seen ≥20
times, which is the population a test set can score. The `LEX_MIN_N = 20`
threshold follows that.

**Key statistics are computed on `primary_train` only.** Deriving "lexically
ambiguous" or "rare" from the whole corpus would let the definition of the
test set peek at the test set.

| set | rows | share of primary_test | ambiguity rate |
| --- | --- | --- | --- |
| `test_lexical_only` | **122,928** | **88.1%** | 0.989 |
| `test_lexical_hard` | **69,150** | 49.5% | 0.996 |

**`test_lexical_only` is 88% of the test split, so it is not a difficulty
filter.** That is not a defect in the construction — it is a fact about the
corpus. The head keys (`m`, `d`, `c`, `l`, `f`, `p`, `s`, `v`, `a`) are all
lexically ambiguous and they carry most of the volume, so lexical ambiguity is
the majority case **by pairs as well as by keys**. It is reported because the
number is informative, not because it is a useful test set.

`test_lexical_hard` is the discriminating set: a lexically ambiguous key **and**
a gold label that the most-frequent-expansion baseline gets wrong. 69,150 rows,
49.5% of test, and its century profile stays close to the primary test
(1AD 28.7%, 2AD 34.1%) so it is not silently an era subset.

---

## 4d. Rare-form test set — N = 10, justified

`N` is stated as **fewer than 10 occurrences of the key in `primary_train`**.

Measured band structure of the test split, by the training frequency of each
row's key:

| training frequency of the key | test rows | share |
| --- | --- | --- |
| **0 — never seen in training** | 2,352 | 1.7% |
| **1–9** | 3,959 | 2.8% |
| 10–99 | 8,139 | 5.8% |
| 100–999 | 14,126 | 10.1% |
| ≥1000 | 111,004 | 79.5% |

**Why 10.** Below 10 observations a most-frequent-expansion lookup has no
usable distribution to estimate — it is fitting a multinomial from single
digits. 10 also sits below the ≥20 threshold `abbrev_probe` and D-0024 use for
ambiguity tables, so the rare set and the lexical set do not overlap by
construction, and above 1, so the set is not purely hapax. The full band table
is published so a reader who prefers a different N can cut it themselves.

| set | rows | keys | ambiguity rate |
| --- | --- | --- | --- |
| `test_rare_form` (train freq < 10) | 6,311 | 5,165 | 0.103 |
| `test_unseen_form` (train freq = 0) | 2,352 | 2,139 | 0.043 |

**Rare forms are mostly unambiguous** — 0.103 against 0.918 on the full test
split — so this set measures *coverage*, not disambiguation. They also skew
late: 4AD is 13.3% of the rare set against 8.0% of the primary test, and 6AD
3.9% against 1.5%. Rare abbreviations concentrate in late and unusual texts.
That must be stated alongside any rare-band result.

---

## 4e. Rows barred from every test set

The brief names `circularity_risk`. **It bars zero rows, and the zero is
reported rather than passed over**: D-0027 established that every kept pair
sits wholly outside `[ ] < > { }` by construction of the extractor, so no
abbreviation letter in v1 is editor-supplied. The rule would bar 63.25% of the
bracket-excluded population (D-0011) if Phase 1's recoverable rows were ever
merged in, and the check stays in the code for that reason.

Three further bars are **my decision, stated so they are reversible**. A test
set containing `ix → ixit` measures the extractor, not the model.

| bar | rows in task | rationale |
| --- | --- | --- |
| `circularity_risk > 0` | **0** | the brief's rule; D-0027 |
| `linebreak_fragment` | 9,492 | D-0026 — `v/` + `ix(it)` is not an abbreviation |
| `unresolved_correction` | 3,350 | D-0020 — no reading was asserted, so none can be scored |
| `miskeyed_date` | 29 | D-0023 |
| **total distinct rows** | **12,817** | |

These rows are **kept in training** and removed only from val/test:
**2,464** barred out of primary val/test, **1,030** out of the held-out test.
Both barred sets are written to disk (`barred_from_test_*.tsv`) so the
decision can be inspected and undone.

---

## 4f. Per-split statistics and comparability

| split | rows | monuments | keys | types | ambiguity | dated | prov TVD | (null) | cent TVD | (null) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| primary_train | 1,140,108 | 256,051 | 32,138 | 62,219 | 0.951 | 36.0% | — | — | — | — |
| primary_val | 139,591 | 31,664 | 7,964 | 14,843 | 0.911 | 36.4% | 0.0234 | 0.0120 | 0.0123 | 0.0059 |
| primary_test | 139,580 | 31,799 | 7,810 | 14,530 | 0.918 | 36.1% | 0.0262 | 0.0120 | 0.0122 | 0.0061 |
| heldout_province_train | 1,349,016 | 301,337 | 35,520 | 68,792 | 0.952 | 36.1% | 0.0507 | 0.0120 | 0.0151 | 0.0059 |
| **heldout_province_test** | 71,697 | 18,303 | 4,289 | 7,657 | 0.908 | 35.2% | **0.9494** | 0.0120 | **0.3022** | 0.0093 |
| test_lexical_only | 122,928 | 29,824 | 865 | 6,106 | 0.989 | 35.7% | 0.0339 | 0.0120 | 0.0140 | 0.0068 |
| test_lexical_hard | 69,150 | 22,938 | 752 | 5,331 | 0.996 | 38.6% | 0.0709 | 0.0120 | 0.0376 | 0.0086 |
| test_rare_form | 6,311 | 4,397 | 5,165 | 5,439 | 0.103 | 34.9% | 0.1065 | 0.0365 | 0.1325 | 0.0315 |
| test_unseen_form | 2,352 | 1,797 | 2,139 | 2,156 | 0.043 | 35.9% | 0.1464 | 0.0583 | 0.1219 | 0.0473 |
| test_no_context_duplicate | 121,199 | 25,139 | 7,235 | 13,186 | 0.916 | 38.6% | 0.0633 | 0.0120 | 0.0152 | 0.0064 |

**Comparable:** `primary_val` and `primary_test` (see the monument-level check
in 4a — ratio 0.8×, indistinguishable from a random draw).

**Deliberately not comparable, and this is the point of them:**
`heldout_province_test` (disjoint provinces by construction, plus the century
confound in 4b), `test_rare_form` and `test_unseen_form` (selected on
frequency, and late-skewed).

**One caveat on the ambiguity column.** It is measured *within* each split, so
a small split shows a lower rate simply because fewer of a key's expansions
appear in it — this is why test reads 0.918 against train's 0.951 rather than
because the test rows are easier. Phase 6 must compute ambiguity against the
**training** expansion sets, not within-split.

**Dating coverage is 35–39% on every split**, consistent with the ~35% figure
the limitations section must carry.

---

## The split's difficulty, measured in advance

**Most-frequent-expansion lookup, trained on `primary_train`, scored on
`primary_test`: 45.84% (63,977 / 139,580).**

This is a property of the split, not a result: it is published here so the
splits can be judged before any model is built, and it is what defines
`test_lexical_hard`. **Phase 5 must recompute it as M1 under three seeds**, and
that recomputation — not this number — is what belongs in the paper (R2).

Two further split properties worth having in hand:

- **96.6%** of test rows have their (key, expansion) type present in training.
  That is not leakage, it is the task: expanding abbreviations means mapping to
  a known vocabulary. The 3.4% that is not is where `test_unseen_form` lives.
- **54.2%** of test rows have a gold label the majority baseline gets wrong,
  matching the 54.5% lexical share from D-0024 closely enough to suggest the
  two are measuring the same underlying structure.

---

## Self-adversarial pass (R7)

**Attack 1: *"Grouping by inscription id doesn't stop leakage — formulaic
text repeats across stones."*** **Run, and it landed.** 13.17% of test rows
are byte-identical to a training row, context included. Grouping cannot fix it
because the monuments genuinely differ. Answered with a published
de-duplicated variant and a requirement that Phase 6 report both.

**Attack 2: *"Your splits aren't distributionally comparable — province TVD is
2.6× the null."*** **Run, and it failed.** The pair-level null is wrong for a
grouped split. At the monument level, TVD 0.0126 against null p95 0.0163 —
**0.8×**, indistinguishable from a random draw.

**Attack 3: *"Your held-out province test measures era, not geography."***
**Run, and it landed.** Century TVD 0.3022; 1AD is 26.8% of the primary test
and 5.7% of the held-out test. Reported with the numbers, and Phase 6 is
required to stratify. The confound is partly irreducible — frontier provinces
were Romanised late.

**Attack 4: *"Your lexical test set is just the test set."*** **Run, and it
landed.** 88.1% of the test split. Kept, because the number is a fact about
the corpus, but `test_lexical_hard` (49.5%) is supplied as the discriminating
set.

**What remains unresolved.**
- Whether the 18,381 exact-context duplicates are leakage or the domain.
  A judgement about epigraphic practice, not a computation. **[VERIFY —
  LATINIST]**
- The inflectional/lexical rule is still a prefix proxy (D-0024).
  **[VERIFY — LATINIST]**
- Whether three held-out provinces are enough to separate geography from era.
  Phase 6's stratified results will show it; if they cannot, the held-out
  result must be reported as inconclusive rather than as a generalisation gap.

---

## FROZEN

The files in `data/derived/v1/splits/` are frozen as of this report. **The
test sets are not to be inspected again until Phase 6.** Every number above is
a property of the splits themselves or was computed from `primary_train`; the
one figure derived from test rows — the 45.84% baseline — is published now
precisely so that it cannot be tuned against later.

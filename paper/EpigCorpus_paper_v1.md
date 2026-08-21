# EpigCorpus: mining editorial parentheses for Latin abbreviation expansion, and what EDCS's transcription conventions do to anything built from them

**[VERIFY — AUTHORS, AFFILIATIONS]**

---

## Abstract

The Epigraphik-Datenbank Clauss-Slaby (EDCS) marks the editorial expansion of
ancient abbreviations in round parentheses: the stone reads `D M`, the edition
prints `D(is) M(anibus)`. We treat those parentheses as free ground-truth
labels and extract **1,424,314** (abbreviation, expansion) pairs from
**588,509** records — to our knowledge the first dataset for Latin
epigraphic abbreviation expansion, a task the epigraphic NLP literature has not
addressed while converging on the restoration of lost text.

The larger contribution is an audit. We show that EDCS's plain-text
transcription **does not preserve the numeral vinculum** — a full census of
39,470,885 characters finds U+0305 twice and U+0304 not at all — and that it
substitutes a supplied word, `HS X(milia)`, in 65.4% of cases preceded by
the sestertius sign. We show that a single ASCII `|` stands for at least eight
unrelated epigraphic signs across **376 distinct forms**. We show
that the plural-marking geminatio doubles an abbreviation's **final** letter,
not its first, and that the natural leading-letter rule undercounts it by 41%.
We show that the extraction filter **reshapes** the corpus rather than
filtering it, at 11.1× and 16.3× a bootstrap null for
province and century.

We then use the dataset to test whether abbreviation meaning tracks province
and century. It does, strongly, in the data. But the effect is **memorisation,
not knowledge**: province conditioning is worth **+0.0833** to a lookup
model on provinces seen in training and **exactly +0.0000** on provinces
withheld, and it is worth nothing at all — -0.0070 and -0.0070 — to a
linear re-ranker and a fine-tuned Latin transformer that can read the
surrounding text. We report this as a negative result and reframe the work as a
resource paper.

---

## 1 Introduction

Roman inscriptions abbreviate heavily and inconsistently. A funerary formula
may compress *Dis Manibus* to `D M`, and a single letter `V` may stand for
*vixit*, *votum*, *vivus*, *vir* or *Victrix* depending on where and when the
stone was carved. Expanding those abbreviations is a prerequisite for reading
the corpus at scale, and until now it has had neither a dataset nor a
benchmark.

It does, however, have an unusual source of supervision. Epigraphic editions
record expansions in round parentheses, and EDCS applies the convention across
its whole corpus. That yields labels at a scale no annotation project could
fund. It also yields labels that are **editorial interpretations rather than
attested text**, which is the central tension of this work and the subject of
Section 5.

**Contributions.**

1. A dataset of 1,424,314 (abbreviation, expansion) pairs over 588,509
   records, with frozen, monument-grouped splits and four difficulty-targeted
   test sets (Section 4).
2. Six previously undocumented properties of EDCS's transcription conventions,
   each measured, that distort any dataset derived this way (Section 3, 5).
3. A benchmark with three baselines under three context conditions, and the
   finding that conditioning on province is memorisation rather than
   generalisation (Sections 6, 7).
4. An explicit account of what is not resolved, including the items that
   require a Latinist and the permission that blocks release (Section 8).

**How this differs from LatEpig.** LatEpig is a retrieval tool: it reproducibly
executes an EDCS search and exports the matching records. EpigCorpus is a
derived labelled dataset and benchmark: it mines EDCS's editorial parentheses
as ground truth and measures the conventions that distort any such derivation.

---

## 2 Related work

**Restoration, not expansion.** Ithaca [Assael et al. 2022] restores and
attributes ancient Greek; Aeneas [Assael et al. 2025] contextualises Greek and
Latin, restoring arbitrary-length lost text; Locaputo et al. (2023) fill
lacunae in Latin inscriptions. All three restore characters **lost from the
stone**, where the label is a lacuna. We expand abbreviations the stone
**deliberately carries**, where the label is an editorial parenthesis. The
circularity profiles are opposite: a restoration can in principle be checked
against a rediscovered fragment, whereas no independent record exists of what a
carver meant by `V`.

**EDCS as an ML source.** Kaše et al. (2021) reconcile the incompatible
inscription-type taxonomies of EDCS and EDH with a learned classifier, at the
level of whole inscriptions. Heřmánková et al. (2021) argue for treating
epigraphic editions as data and release LIRE, an EDH/EDCS aggregate restricted
to dated, geolocated records. Cui and Ströbel (2026) annotate 1,000 EDCS
inscriptions for named entities and do not address abbreviation expansion. Ours
is token-level and derived rather than hand-annotated: 1,424,314 labels against
their 1,000 inscriptions.

**Abbreviation indices.** Elliott (1998) compiled a standing index from
*L'Année Épigraphique* 1888–1993. An index gives the expansions an abbreviation
*can* take; we give the distribution over expansions it *does* take, with
ambiguity measured — of which **54.5%** among evaluable keys is lexical
rather than merely inflectional.

---

## 3 The EDCS corpus and its conventions

588,509 records, 39,470,885 characters. The records are **not** 588,509
inscriptions: the `-N` suffix marks segments of one monument, and
542,854 distinct monuments underlie them, with 45,655 records
sharing a base id. Any split must group by monument.

### 3.1 The vinculum is not preserved

Roman numerals were multiplied by a thousand with an overline. We ran a full
character census — all 39,470,885 characters, all 414 distinct code
points, no sampling — and found:

| code point | occurrences |
| --- | --- |
| U+0305 combining overline | 2 |
| U+0304 combining macron | 0 |
| Number Forms block (incl. U+2183 Ↄ) | 0 |
| Ancient Symbols block (Roman denarius, sextans, uncia) | 0 |

Unicode provides dedicated code points for the reversed C and the Roman
monetary signs. EDCS uses none of them. **The multiplicative vinculum has zero
attestations.** All three surviving combining marks are reproduced in the
appendix; none is a vinculum.

This is not an encoding limitation: 208 Leiden underdots (U+0323)
pass through the same pipeline.

**What replaced it.** EDCS renders the overline as a supplied word in
parentheses. `N(milia)` occurs 1,875 times, **65.4% immediately
preceded by `HS`**, with small multiplier numerals. `HS X(milia)` is ten
thousand sesterces. This matters for dataset construction: **2,338** such
pairs sit in the extracted set as strings like `X → Xmilia`, which is not a
Latin word and was never on a stone.

### 3.2 One `|` for at least eight signs

EDCS renders non-typeable signs as ASCII `|`. We enumerate **376
distinct `|(...)` forms, 16,194 occurrences**, spanning the centurial
sign, the reversed C for *mulieris*, monetary and milliary signs, the fractional
weight system, the *obitus* theta nigrum, Greek measures, and Christian signs.

Three structural properties follow. Plurality is marked by **repetition** —
`||(mulierum)`, `||||(milia)` — the same principle as letter geminatio. The
inflectional ambiguity of the Latin recurs inside the symbol set. And the
inventory carries **its own unresolvable class**, `|()`, where the sign is on
the stone and the editor withheld the word.

### 3.3 Geminatio doubles the final letter

The plural is marked by doubling — `Aug → Augg(ustorum)`,
`Imp → Impp(eratoribus)`, `Cos → Coss(ulibus)`. For a one-letter abbreviation
the doubled letter is both first and last (`D → DD(ominis)`), which makes a
leading-letter rule look correct on the headline examples while missing the
AUGG/IMPP/CONSS/CAESS/NOBB family entirely. We made this error: our first
implementation found 5,293 cases; the trailing-run rule finds **8,986**, a
41% undercount.

---

## 4 Dataset construction

### 4.1 Extraction and verification

Extraction is per whitespace token, so `co(n)s(ul)` yields one pair
(`cos → consul`). The accounting closes exactly: **1,767,028**
paren-bearing tokens = **1,424,314** kept + **342,714** dropped, residual
zero.

Because reproducing a bug reproduces the bug, we verified against a **second,
independently written extractor**: 1,424,238 pairs agree (99.995% of the
primary), and all 76, and zero unexplained, disagreements are accounted for.

### 4.2 Corrections, all reversible

The original columns are carried through untouched; every correction is an
added column and every exclusion is a flag. Numeral cases are decided by three
**explicit printed word lists** rather than a heuristic:

| class | pairs | treatment |
| --- | --- | --- |
| Type 1, numeral stands for the word | 338 | expansion = bracket content |
| Type 2, numeral + supplied unit | 2,571 | flagged out of the task |
| Type 3, numeral as word-prefix | 508 | gold = EDCS surface form |

Type 3 gold labels are the **surface form** (`VIvir`), never the Latin reading
(*sevir*), which is confined to a normalisation column: a gold label must not
depend on a contested scholarly reading.

### 4.3 Splits, frozen

80/10/10 grouped by monument (1,140,108 / 139,591 / 139,580), verified:
**zero of 319,514 monument groups straddle**. Three provinces —
Britannia, Mauretania Caesariensis, Pannonia inferior — are withheld entirely, chosen one per genre regime with a
same-regime sister left in training.

**Monument grouping does not stop all leakage.** 18,381 test rows
(13.17%) are byte-identical to a training row, context included, because
different stones carry identical formulae. We publish both the full test split
and a de-duplicated variant; the gap is the memorisation premium.

---

## 5 Artifacts and bias

### 5.1 The filter reshapes the corpus

Pooled across all ten exclusion reasons, the dropped set differs from the kept
set by **province TVD 0.131** against a bootstrap null p95 of
0.012 (11.1×) and **century TVD 0.097** against
0.006 (16.3×). Dropped texts average 504 characters
against 280 kept. The filter preferentially removes long, damaged,
Greek-East and fragmentary material.

We also note a methodological trap we fell into: total variation distance is
biased upward at small n, and an absolute threshold declares small categories
skewed when sampling noise alone produces the observed value. Bias must be
reported against a null computed at each category's own n.

### 5.2 Circularity, measured

For every token excluded as bracket markup we compute the share of the
**abbreviation's own letters** supplied by the editor. **63.25%** are
wholly editorial and only **1.35%** fully attested.

### 5.3 Editorial label noise

Holding 80 characters of context byte-identical, **13,706 rows
(0.96%)** carry an expansion that differs from another row with
identical evidence. This is a floor, not the rate.

Separately, **66.4% of expansion forms also occur as uncontracted
plain text** — *vixit* appears 31,388 times as an expansion and 25,244 times
carved in full. This is variation between stones, not editorial inconsistency,
but it fixes the task's real definition: *given that an editor judged this
abbreviated, what did they expand it to*.

---

## 6 Experimental setup

Three baselines under three context conditions, as candidate ranking: for each
key the candidates are the expansions seen for it in training, so all models
choose from the same set and the context delta is measured identically.

- **M1** most-frequent-expansion lookup (57,054 parameters)
- **M2** logistic-regression candidate re-ranker over character n-grams and
  context words (1,048,580 parameters)
- **M3** fine-tuned `bowphs/LaBerta` (137,180,135 parameters)

Conditions: **C1** local text only, **C2** + province, **C3** + province and
century. The C1→C3 delta is the experiment.

**Ceilings.** Candidate ranking cannot propose an expansion never seen for a
key. `test_unseen_form` therefore has a ceiling of **0.0000** and
`test_rare_form` of **0.4812**. Accuracy on these sets is
uninterpretable without them.

---

## 7 Results

| model | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1 lookup | 0.4582 | 0.5247 | 0.5414 | **+0.0833** |
| M2 linear | 0.7376 | 0.7347 | 0.7305 | **-0.0070** |
| M3 LaBerta | 0.7575 | 0.7513 | 0.7505 | **-0.0070** |

**The candidate finding fails.** Province and century are worth
**+0.0833** to a model that cannot read text and nothing to either model
that can. M2 and M3 — a linear re-ranker and a 137M-parameter Latin transformer
— agree to four decimal places.

### 7.1 The signal is memorised, not learned

Retraining without the three withheld provinces and testing on exactly those
provinces:

| | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1, provinces withheld | 0.3665 | 0.3665 | 0.3665 | **+0.0000** |

**Exactly zero, to four decimal places.** M1's province mechanism is a lookup
keyed on province; for an unseen province the table is empty and the model
backs off. The +8.3 points is memorisation of local distributions.

We stress that the association itself is real: normalised mutual information
between province and expansion for `V` is 0.3709, over 12.2× an
inscription-level block-permutation null, and it survives restoring the
excluded strata. Province genuinely predicts meaning. It simply does not
transfer, and it adds nothing a model cannot already read off the text.

### 7.2 Cost

| model | parameters | training time | accuracy (C1) |
| --- | --- | --- | --- |
| M1 | 57,054 | 0s | 0.4582 |
| M2 | 1,048,580 | 72s | 0.7376 |
| M3 | 137,180,135 | 1078s | 0.7575 |

M2 recovers **97.4%** of M3's accuracy for a small fraction of the
cost. M3 was trained on 100,000 rows against M2's 1,140,108, so this
comparison favours the cheap model and should be read with that caveat.

---

## 8 Limitations

1. **Expansions are editorial interpretations, not attested text.** A model
   trained here learns EDCS conventions; it cannot be shown to have learned
   Roman practice.
2. **The task is conditioned on the editor judging an abbreviation present**
   (66.4% of expansion forms also occur in full).
3. **Editorial noise ≥0.96%**, a floor.
4. **The exclusion filter is bias-inducing** (11.1× and
   16.3× null).
5. **EDCS dates only 36.0% of records**; every century-conditioned
   result rests on that third.
6. **Survival and excavation bias** are inherited and uncorrected.
7. **13.17% of the test split is byte-identical to training rows.**
8. **The held-out province split confounds province with century** — the
   withheld provinces are late-Romanised frontier territory.
9. **Several judgements need a Latinist** and have not had one: the numeral
   word lists, the `|` sign families, which near-duplicate forms are keying
   errors, and whether `X(milia)` renders a vinculum. **[VERIFY — LATINIST]**
10. **M3 used 100,000 training rows at one seed** for compute reasons, so
    its result is a lower bound and its seed variance is unmeasured.
11. **Redistribution permission from EDCS is unresolved and blocks release.**
    **[ACTION — PERMISSION]**

---

## 9 Conclusion

We set out to build a benchmark and test whether abbreviation meaning tracks
province and era. The benchmark exists — 1,424,314 labelled pairs with frozen
splits and difficulty-targeted test sets. The hypothesis failed: province
predicts meaning in the data but transfers not at all, and adds nothing to a
model that reads Latin.

The more durable contribution is the audit. EDCS does not preserve the
vinculum, collapses at least eight signs onto one character, marks plurality on
an abbreviation's final letter, fragments words across lines, and is filtered by
conventions that reshape the corpus along the very axes one might wish to study.
Every one of these is invisible to a consumer who mines the parentheses and
starts training. Documenting them, with counts, is the service this paper
offers.

---

## References

### 1. Assael et al. 2025 — *Aeneas* — **primary contrast**


*Full citations with DOIs in `reports/related_work.md`.*

---

## Appendix A — the three surviving combining marks

| record | mark | context |
| --- | --- | --- |
| `EDCS-00000939-0` | U+0305 on `q` | `Augustal(i) Cumis, q̅(uaestori)` |
| `EDCS-05802229-0` | U+0305 on `I` | `Messal(l)ae II[I̅viro(?)` |
| `EDCS-25500308-0` | U+0332 on `τ` | `Διονύϲιοϲ / οπτο τ̲` |

None is a multiplicative vinculum: the first is the abbreviation overline on a letter, the second a numeral-prefix compound, the third Greek.

## Appendix B — reproduction

Every number in this paper is injected from `results/all_results.json` by
`scripts/build_paper.py`; none is typed. The pipeline is reproduced by
`./reproduce.sh`, which verifies the corpus sha256
(`9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317`) before running and refuses a different snapshot.
Decisions are logged with evidence in `reports/decisions.md` (41
entries).

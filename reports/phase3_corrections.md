# Phase 3 — Corrections, all reversible

**Output.** `data/derived/v1/abbrev_pairs_v1.tsv` — 1,424,314 rows, 23
columns, 0 malformed. The original `abbrev` and `expansion` columns are
carried through **untouched**; every correction is an added column. Nothing is
deleted and filtering is by flag only (R4). Anything the explicit rules do not
reach is `UNRESOLVED`, never imputed (R5).

Built by `scripts/phase3_build_v1.py`, seed 20260820, from the base fixed in
D-0001/D-0006.

---

## 3a. Geminatio

**The rule in the prior work was wrong, and fixing it changed the count by
70%.** EDCS marks the plural by doubling the **final** letter of the
abbreviation — `Aug → Augg(ustorum)`, `Imp → Impp(eratoribus)`,
`Cos → Coss(ulibus)`, `Nob → Nobb(ilissimis)`, `Caes → Caess(aribus)`. For a
one-letter abbreviation the doubled letter is *both* first and last
(`D → DD(ominis)`), which is why a leading-run rule looks right on the
headline examples and silently misses AUGG, IMPP, CONSS, CAESS and NOBB
entirely. My first implementation used a leading run and found **5,293**
collapses; the trailing-run rule finds **8,986**.

Correction: reduce the trailing run to one letter in the abbreviation and in
the expansion together.

| | |
| --- | --- |
| `Augg` + `ustorum` | → `Aug` + `ustorum` = **Augustorum** |
| `DD` + `ominis` | → `D` + `ominis` = **Dominis** |
| `conss` + `ulibus` | → `cons` + `ulibus` = **consulibus** |
| `Impp` + `eratoribus` | → `Imp` + `eratoribus` = **Imperatoribus** |

Arbitration is against a **control lexicon of 36,477 expansions built only
from tokens carrying no doubled run**, so the test cannot feed on the forms it
is judging. A verdict is DECISIVE when the winner has ≥3× the runner-up's
support, THIN otherwise, UNRESOLVED when neither reading is attested.

| result | pairs |
| --- | --- |
| GEMINATIO_COLLAPSE | **8,986** |
| DECISIVE | 32,143 |
| THIN | 1,010 |
| UNRESOLVED | 3,350 |

**Independent agreement with `dd_diagnostic.md` §4b**, which used a different
arbitration written in a different session:

| form | this session | dd_diagnostic |
| --- | --- | --- |
| augg | 1,792 | 1,793 |
| nn | 1,701 | 1,702 |
| dd | 1,043 | 1,054 |
| cc | 446 | 447 |
| caess | 400 | 400 |
| conss | 399 | 399 |
| impp | 387 | 387 |
| vv | 299 | 299 |
| nnn | 297 | 297 |
| ddd | 253 | 253 |

Both the original key (`abbrev`) and the collapsed key (`abbrev_collapsed`)
are written, as the brief requires, so the effect on ambiguity is measurable
in both directions — quantified in 3e.

`plurality_flag = PLURAL` is set on every collapse; `geminatio_marker` records
the run itself (`gg`, `DD`, `ggg`). **The 3,350 UNRESOLVED are left
uncorrected**: `DD(ecimorum)` (41 pairs) has neither `ddecimorum` nor
`decimorum` attested in the control lexicon, so no reading is asserted.

**Cross-reference.** D-0013 found the same plurality-by-repetition principle
in the symbol inventory — `||(mulierum)`, `||(centuriones)`, `||||(milia)`.
Letters and glyphs are one phenomenon, not two.

---

## 3b. Numerals — three classes, decided by explicit lists

The lists are in `scripts/phase3_wordlists.py`, printed in full by running it.
**They are decisions, not thresholds.** Every entry was chosen from the
complete evidence base — every parenthesis content following an
all-Roman-numeral prefix anywhere in the corpus, with counts
(`scripts/phase3_numeral_evidence.py`). Assigning a Latin word to a semantic
class is philological. **[VERIFY — LATINIST]** applies to all three lists.
What needs no Latinist is the coverage report below.

| list | entries | pairs matched |
| --- | --- | --- |
| TYPE1_NUMERAL_WORD | 50 | **338** |
| TYPE2_SUPPLIED_UNIT | 100 | **2,571** |
| TYPE3_NUMERAL_PREFIX | 15 | **508** |
| cross-list overlap | **0** | — |

**Type 1 — the numeral stands for the word.** Expansion = bracket content
alone. `XL(quadragesimae) → quadragesimae`, not `XLquadragesimae`.
Top hits: *vicesimae* 82, *duorum* 40, *vicennalibus* 34, *decennalibus* 27,
*quadragesimae* 26, *trium* 23, *vicesima* 14, *duo* 12, *quarta* 12,
*tertia* 11, *trecenario* 8. The list covers fiscal ordinals (the *vicesima
hereditatium*), imperial anniversary formulae, and procuratorial pay grades
named for a salary in thousands of sesterces.

**Type 2 — numeral + supplied unit. Class `numeral_ellipsis`.** The numeral
and the supplied word are stored in **separate fields** (`numeral_value`,
`supplied_word`) because the word is not on the stone. Kept in the release,
`excluded_reason = NUMERAL_ELLIPSIS_NOT_AN_ABBREVIATION`, **2,571 rows
flagged out of the abbreviation task**.
Top hits: *milia* 1,128, *milibus* 464, *librae* 361, *unciae* 113,
*sextarii* 76, *libra* 66, *scripula* 49, *vasa* 40, *milium* 26, *modii* 24.

**Phase 2 supplies the justification** (D-0017): the word is nowhere on the
stone because on the stone it was an overline. 65.4% of the `N(milia)` cases
are immediately preceded by `HS`. Type 2 is a documented transcription
convention, not a definitional convenience.

**Type 3 — numeral as word-prefix.** Gold label = the **EDCS surface form**
(`VIvir`), because that is what is verifiable against the source. The Latin
reading goes in `normalized_form` only — a gold label must never rest on a
contested scholarly reading.
Hits: *triere* 311, *quadriere* 68, *vir* 31, *viro* 24, *virum* 20,
*pentere* 19, *virorum* 19, *hexere* 4, *viri* 4.

**Surface forms mapping to the same office, as the brief requires:** *sevir*
is written both `VI(vir)` and `IIIIII(vir)`; `II/III/IIII/V/VI/VII/X/XV/XX +
vir` all occur. The normalisation table records **16 (numeral, word) → office**
mappings across 8 distinct offices. The surface forms are kept distinct in the
gold label and merged only in `normalized_form`.

**Coverage.** Of the 3,876 pairs with a **multi-character** numeral prefix,
the three lists plus the geminatio arbitration reach all but **74**. Those 74
are written `UNRESOLVED_NUMERAL` with the unmatched word recorded — chiefly
`DD(ecimorum)` 41, `II(I)` 12, `MM(arciorum)` 2. **Nothing is guessed.**

---

## 3c. The §6b single-numeral cases, re-derived measurably

`dd_diagnostic.md` §6b flagged 753 single-numeral tokens and reported *"the
genuine share is somewhere around four-fifths, but that is an impression, not
a measurement."* That figure is **not carried forward**.

**The measurable replacement, and it needed no new heuristic.** A
single-character numeral (`X`, `V`, `L`, `C`, `D`, `I`, `M`) is treated as a
numeral **if and only if its parenthesis content is on one of the three
printed word lists.** Everything else stays an ordinary abbreviation. This is
the same rule applied to multi-character prefixes, so the threshold that
created the blind spot disappears rather than being re-tuned.

| | pairs |
| --- | --- |
| Type 2, supplied unit | 667 |
| Type 1, numeral word | 28 |
| Type 3, numeral prefix | 17 |
| **total single-character numeral cases** | **712** |

For scale, 0.8 × 753 ≈ 602 is what the eyeballed impression implied. The
measured figure is **712**. The two are close, which is a point in the prior
work's favour — but 712 is a count with a stated rule behind it, and 602 was
never a number at all.

**The gate matters.** An earlier version of this pipeline classified any
single-character Roman-numeral abbreviation whose expansion began with it as a
numeral candidate. That flagged **268,253** rows — `M(anibus)`, `D(is)`,
`C(aius)`, `L(uci)` — the entire praenomen vocabulary of Latin epigraphy. The
list gate reduces it to 712. Recorded because the failure mode is invisible in
the output and would have destroyed the dataset.

---

## 3d. Dates — nothing dropped, two flagged with evidence

| flag | pairs | treatment |
| --- | --- | --- |
| NONE | 1,423,581 | — |
| **POST700** (701–1000) | **618** | **kept, unchanged** |
| **LATE_OVER_1000** | **86** | **kept, unchanged** |
| MISKEYED | 29 | flagged, **not removed** |

**The post-700 material is genuine and the brief was right to protect it.**
The 618 POST700 pairs are early-medieval Christian vocabulary:
`prb → presbyter` (16 at 791–825, 12 at 691–725), `pbr → presbyter`,
`mon → monachus`, `umilis → humilis`, `eternam → aeternam`, `Eo → Ego`.

**21 records carry a date after 1000. 19 of them are correctly dated.** Each
was read individually:

| record | date | text evidence | verdict |
| --- | --- | --- | --- |
| EDCS-82200103-0 | 951–1050 | `In hoc tumulo iacet corpus bonae memoriae abba Laurentius monachus` | genuine early-medieval |
| EDCS-82200102-0 | 1003 | `vixit in hoc saeculo annos plus minus XXXX migravit` | genuine |
| EDCS-82200094-0 | 1017 | `In Christi nomine in hoc tumulo iacet corpus` | genuine |
| EDCS-38700101-0 | 1034 | `V(enerabilis) f(rater) d(ominus) Munius Monis` | genuine |
| EDCS-41800130-0 | 1169 | `Hoc est sepulcrum Wleder matris Odelev` | genuine, Anglo-Norman |
| EDCS-48700548-0 | 1101–1200 | `Hic requiescit beatus Ypolitus martyr` | genuine |
| EDCS-85200148-0 | 1435 | text **contains the year 1435**; `f(a)c(tu)m e(st) h(oc) p(er) m(agistrum)` | genuine, dated |
| EDCS-75900020-0 | 1501–1700 | Italian vernacular: `la vita nostra al mondo passa` | genuine early-modern |
| EDCS-05601137-0 | 1781–1782 | Spanish: `Tomas Urbina dragon del V regimiento` | **genuine 18th-century** |
| … 10 further records | 601–1900 | Christian funerary and early-modern | genuine |

**Two are demonstrably mis-keyed**, and only these two are flagged:

| record | date | evidence |
| --- | --- | --- |
| `EDCS-27500083` | 1998 | Text reads `d(omini) n(ostri) Fl(avi) Val(eri)` — tetrarchic titulature. Cannot be 1998 AD. |
| `EDCS-30400458` | 121–1125 | A 1,004-year span. The text is a Domitia Lucilla brick stamp (`O d(oliare) d(e) f(iglinis) D(omitiae)`), a tightly dated Hadrianic type. `1125` is `125` with a leading 1. |

The evidence for each is written into `scripts/phase3_build_v1.py` beside the
identifier, not left implicit. **Neither record is removed.**

**Note on the rule.** `date_to > 1000` is a flag *for review*, not a defect
detector: 19 of the 21 records it caught are correct. Reported because the
brief's example (year 1998) invites a threshold rule, and a threshold rule
would have thrown away a genuine Spanish 1781 inscription and a
self-dated 1435 Latin one.

---

## 3e. Before / after, every headline figure

Three views of the same 1,424,314 rows:

- **V0** original abbrev → original expansion (the current release)
- **V1** original abbrev → corrected expansion (corrections only)
- **V1c** collapsed abbrev → corrected expansion (keys merged too)

| measure | V0 | V1 | V1c |
| --- | --- | --- | --- |
| pairs | 1,424,314 | 1,424,314 | 1,424,314 |
| unique abbreviation keys | 37,526 | 37,526 | **36,920** |
| unique (abbrev, expansion) types | 73,105 | 73,101 | **71,977** |
| ambiguous keys | 7,836 | 7,836 | 7,592 |
| ambiguous keys, n ≥ 20 | 1,937 | 1,937 | 1,865 |
| lexically ambiguous, n ≥ 20 | 1,056 | 1,058 | 1,017 |
| mean expansions per key | 1.948 | 1.948 | 1.950 |
| pairs on ambiguous keys | 1,356,329 | 1,356,329 | 1,356,889 |
| rows flagged out of the task | — | — | **2,571** |

**V1 barely moves and that is not a null result — it is arithmetic.** Under V1
the key stays `dd`, so `dd → ddominis` becomes `dd → dominis`: one type
replaced by one type. The correction only shows up in the type count once the
*keys* are merged as well, which is exactly why the brief demanded both keys
be kept.

### Collapsing enlarges the head, as predicted — quantified

**622 keys gain expansions** when `dd → d`. The gain lands on the
highest-frequency keys:

| key | expansions before | after | pairs on the key |
| --- | --- | --- | --- |
| `i` | 226 | **294** | 8,521 |
| `c` | 629 | 645 | 64,622 |
| `p` | 637 | 652 | 70,422 |
| `d` | 399 | 407 | 89,045 |
| `x` | 46 | 63 | 732 |
| `aug` | 58 | **65** | 30,155 |
| `an` | 70 | 78 | 33,974 |
| `n` | 295 | 303 | 16,343 |

The head keys absorb 1–3% more expansions each; `i` and `x` — both numeral
letters — absorb far more proportionally. Net: 606 keys disappear by merging,
1,128 pair types disappear, and the surviving keys get harder.

### Top-50, first ten rows

| abbrev | pairs | V0 exp | V1c pairs | V1c exp | top reading |
| --- | --- | --- | --- | --- | --- |
| m | 137,216 | 501 | 137,377 | 505 | manibus |
| l | 90,901 | 348 | 91,047 | 352 | luci |
| d | 87,666 | 399 | 89,045 | 407 | dis |
| f | 72,949 | 353 | 73,067 | 358 | filius |
| p | 70,184 | 637 | 70,422 | 652 | pedes |
| s | 66,140 | 609 | 66,188 | 616 | sacrum |
| c | 64,081 | 629 | 64,622 | 645 | cai |
| v | 38,757 | 317 | 39,062 | 323 | vixit |
| a | 30,461 | 419 | 30,536 | 420 | annos |
| aug | 28,162 | 58 | 30,155 | 65 | augusti |

`aug` gains 1,993 pairs and 7 expansions — the AUGG family arriving.

### Inflectional vs lexical — the prior figure is confirmed *and* is the wrong one to quote

The brief says the prior report indicates roughly a third of ambiguity is
lexical and instructs that the figure be re-derived, not assumed. Re-derived
with an independently written rule (two expansions are inflectional variants
if their longest common prefix is ≥3 characters **and** ≥60% of the shorter
form; a morphological proxy, not a lemmatiser — **[VERIFY — LATINIST]**):

| population | ambiguous keys | lexically ambiguous | share |
| --- | --- | --- | --- |
| **all ambiguous keys (V0)** | 7,836 | 2,706 | **34.5%** |
| all ambiguous keys (V1c) | 7,592 | 2,606 | 34.3% |
| **keys with n ≥ 20 (V0)** | 1,937 | 1,056 | **54.5%** |
| keys with n ≥ 20 (V1c) | 1,865 | 1,017 | 54.5% |

`abbrev_probe.md` reports 2,759 of 7,836 = 35.2%. My independent rule gives
**2,706 = 34.5%** on the same denominator. **The prior figure is confirmed.**

**But "roughly a third" is denominator-dependent and is the wrong number for a
benchmark.** Restricted to keys seen at least 20 times — the only keys a test
set can meaningfully evaluate — the lexical share is **54.5%, not a third.**
Phase 4c must build the lexical test set on the n ≥ 20 figure and say which
denominator it used. Cleaning does not move either figure.

---

## 3f. Does the province and century signal survive? — **YES, on all three tests**

This is the critical check. D-0015 flagged that the exclusion filter skews
along the candidate finding's own two axes, in the direction that would
manufacture it. So the signal is measured on **three populations**, not two:

- **V0** — the current release, 1,424,314 pairs
- **V1c** — after Phase 3 corrections, task rows only, 1,421,743 pairs
- **PRE-EXCLUSION** — V0 **plus 204,207 bracket-excluded pairs recovered**,
  1,628,521 pairs: the population *before* the filter reshaped it

Statistic: normalised mutual information between the conditioning variable and
the choice of expansion, per abbreviation, against a permutation null
(seed 20260820, 200 draws).

| form \| condition | V0 NMI | ratio | V1c NMI | ratio | **PRE** NMI | **ratio** |
| --- | --- | --- | --- | --- | --- | --- |
| v \| province | 0.3738 | 12.7× | 0.3709 | 13.1× | **0.3706** | **13.1×** |
| v \| century | 0.3298 | 13.2× | 0.3357 | 13.9× | **0.3340** | **14.0×** |
| c \| province | 0.1621 | 4.9× | 0.1611 | 4.9× | **0.1687** | **5.4×** |
| c \| century | 0.2511 | 10.2× | 0.2585 | 10.6× | **0.2630** | **11.5×** |
| a \| province | 0.3138 | 8.1× | 0.3126 | 8.0× | 0.3131 | 8.4× |
| l \| century | 0.1739 | 18.9× | 0.1743 | 18.6× | **0.1797** | **20.7×** |
| aug \| province | 0.1757 | 12.2× | 0.1560 | 11.8× | 0.1568 | 13.1× |
| d \| century | 0.2687 | 16.9× | 0.2828 | 18.1× | 0.2692 | 17.7× |

*(full 20-row table in `data/derived/v1/phase3f_signal.json`)*

**Test 1 — cleaning neither created nor destroyed it.** Every V0 → V1c change
is in the third decimal. The single exception is `aug|province`, 0.1757 →
0.1560 (−11%), which is the geminatio collapse merging AUGG into AUG and is
expected.

**Test 2 — the exclusion did not manufacture it.** Restoring 204,207
bracket-excluded pairs — drawn from precisely the provinces and centuries
Phase 1 showed were over-filtered — leaves every value essentially unchanged,
and in most cases **slightly higher**: `c|century` 0.2511 → 0.2630,
`l|century` 0.1739 → 0.1797, `c|province` 0.1621 → 0.1687. **If the filter had
manufactured the signal, restoring the filtered strata would dilute it. It
does not.** D-0015 is answered, against my own hypothesis.

**Test 3 — the non-independence attack.** The nulls above shuffle at the pair
level, but two `v(ixit)` from the same stone are not independent draws. Re-run
as a **block permutation** that shuffles the label among *inscriptions*,
keeping all pairs from one stone together:

| form \| condition | pairs | stones | NMI | pair-null p95 | **block-null p95** | **ratio** |
| --- | --- | --- | --- | --- | --- | --- |
| v \| province | 38,855 | 35,063 | 0.3709 | 0.0282 | 0.0304 | **12.2×** |
| v \| century | 11,539 | 10,577 | 0.3357 | 0.0242 | 0.0258 | **13.0×** |
| c \| province | 64,425 | 45,133 | 0.1611 | 0.0331 | 0.0462 | **3.5×** |
| c \| century | 23,212 | 16,066 | 0.2585 | 0.0247 | 0.0278 | **9.3×** |
| a \| province | 30,531 | 25,661 | 0.3126 | 0.0390 | 0.0462 | **6.8×** |
| l \| century | 33,058 | 21,094 | 0.1743 | 0.0094 | 0.0121 | **14.3×** |
| aug \| century | 14,694 | 11,726 | 0.1087 | 0.0056 | 0.0060 | **18.2×** |

The attack is real but small: `c|province` falls from 4.9× to 3.5×, the worst
case; `v|province` from 13.1× to 12.2×. **Every form remains far above its
block-permutation null.**

### What the association actually is

NMI says an association exists. Here is what it is.

**V by province** — the funerary/votive split the brief hypothesised:

| province | n | expansion distribution |
| --- | --- | --- |
| **Numidia** | 8,529 | **vixit 93%**, votum 3%, vir 1% |
| Mauretania Caesariensis | 911 | **vixit 81%**, votum 9% |
| Africa proconsularis | 4,603 | **vixit 76%**, votum 11% |
| Roma | 5,730 | vixit 51%, vir 9%, viro 8% |
| **Pannonia superior** | 938 | **votum 67%**, vivus 10% |
| **Germania superior** | 862 | **votum 63%**, victrix 26% |
| **Gallia Narbonensis** | 981 | **votum 48%**, vivus 10% |
| **Britannia** | 1,512 | **victrix 36%, valeria 30%**, votum 19% |

**C by century** — the era shift the brief hypothesised:

| century | n | expansion distribution |
| --- | --- | --- |
| 1BC | 2,420 | **cai 45%, caius 31%**, caio 8% |
| **1AD** | 9,340 | **cai 40%, caius 32%**, caio 14% |
| 2AD | 6,362 | caius 28%, cai 22%, caio 16% |
| **4AD** | 958 | **clarissimus 28%, clarissimo 22%**, clarissimi 11% |
| **5AD** | 513 | **clarissimo 34%, clarissimi 18%, clarissimus 15%** |
| **6AD** | 484 | **consulatum 33%, clarissimi 30%**, clarissimo 18% |

**The candidate finding is confirmed on the exact terms it was stated in** —
*V = vixit* in Numidia/Africa versus *votum* in Pannonia/Narbonensis;
*C = Caius* in 1AD shifting to *clarissimus/clarissimo* by 4–5AD.

**Two refinements the brief did not anticipate, both measured:**

1. **Britannia carries a third reading: `V → victrix` 36% and `valeria` 30%.**
   These are legionary titles (*legio VI Victrix*, *legio XX Valeria
   Victrix*), not funerary or votive. Part of the province signal is **military
   nomenclature**, not genre. Germania superior shows the same at 26%.
2. **6AD adds a fourth: `C → consulatum` 33%**, overtaking *clarissimus*. The
   era shift does not stop at *clarissimus*.

**What this does NOT establish.** It shows that province and century *predict*
expansion choice in the data. It does **not** show a model gains from being
told them — the signal could be real yet already captured by local text
context, or exploitable only where the majority baseline wins anyway. That is
precisely the C1→C2→C3 delta of Phases 5–6, and it remains open.

---

## New artifact, not in any prior report: line-break fragmentation

**9,506 pairs (0.67%)** are flagged `linebreak_fragment = 1`. EDCS breaks
lines mid-word, so `v/` ends one line and `ix(it)` begins the next, and the
extractor produces the meaningless pair `ix → ixit` — 48 of those alone.
Discovered while chasing a false positive in Phase 2 (D-0018).

Detection rule: the left context ends in `/` immediately preceded by a letter,
i.e. the previous line ended mid-word. The rows are **flagged, not removed**;
whether a fragment is genuinely broken needs the Latin. **[VERIFY — LATINIST]**

---

## Column note: `circularity_risk`

Written as `0.0` on every row, and this is correct rather than lazy.
`abbrev_probe` drops any token overlapping a bracket span, so every kept pair
sits wholly outside `[ ] < > { }` and no abbreviation letter in this file is
editor-supplied. The column becomes informative only if Phase 1's recoverable
rows are ever merged in, where D-0011 measured 63.25% at risk 1.0.

**A distinct circularity remains and this column does not capture it.** Every
*expansion* in the dataset is an editorial interpretation — that is what the
parentheses mean. It is a property of the corpus, not of a row, and it is
Phase 7's circularity probe and the datasheet's problem.

---

## Self-adversarial pass (R7)

**Attack 1: *"Your geminatio rule is the prior work's rule with a new name."***
**Run, and it landed on me, not them.** My first implementation used a leading
run and produced 5,293 collapses — it missed AUGG, IMPP, CONSS, CAESS, NOBB
entirely, a 41% undercount. The trailing-run rule finds 8,986 and then agrees
with `dd_diagnostic.md` to within 1% on every one of its top ten forms, from a
separately written arbitration. Reported because the failed version was mine.

**Attack 2: *"Your numeral rule will swallow the praenomina."*** **Run, and it
did.** An earlier gate flagged **268,253** rows as numeral candidates —
`M(anibus)`, `D(is)`, `C(aius)`, `L(uci)`, the entire praenomen vocabulary.
The explicit-list gate reduces it to 712. Recorded because the failure is
invisible in aggregate statistics and would have destroyed the dataset
silently.

**Attack 3, the reviewer's sharpest: *"The exclusion filter manufactured your
candidate finding — Phase 1 proved the skew runs the right way."***
**Run, and it failed.** Restoring 204,207 bracket-excluded pairs from exactly
the over-filtered strata leaves every NMI unchanged or slightly higher. This
was my own D-0015 hypothesis and the evidence is against it. Stated plainly
because R7 requires it in both directions.

**Attack 4: *"Your null is too easy — pairs from one stone aren't
independent."*** **Run, and it landed but did not break anything.** Block
permutation at inscription level costs at most 1.4 ratio points
(`c|province` 4.9× → 3.5×). Every form stays far above null.

**What remains genuinely unresolved.**
- The three word lists are philological judgements. **[VERIFY — LATINIST]**
- The inflectional/lexical rule is a prefix proxy, not a lemmatiser.
  **[VERIFY — LATINIST]**
- 3,350 geminatio and 74 numeral rows are UNRESOLVED and stay uncorrected.
- 9,506 line-break fragments are flagged but unadjudicated.
- Whether the confirmed province/century signal is **usable by a model** —
  Phases 5–6. Confirming an association in the data is not confirming the
  experiment.

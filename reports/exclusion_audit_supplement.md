# Exclusion audit — supplement

**Relationship to `reports/exclusion_audit.md`.** That report was re-run
unmodified this session and is **byte-identical** to its prior-session copy;
all ten of its category counts and its 1,424,314 kept-pair total reproduce
exactly. Its sub-classification, gain estimates and per-category tables
stand and are not repeated here.

This supplement answers five questions it left open. Three of them change
numbers in it; one confirms it against my own expectation that it was wrong;
one supplies a deliverable the brief required and it did not contain.

Everything below is computed by `scripts/phase1_supplement*.py`, seed
`20260820`, over the corpus fixed in D-0001.

---

## A. The accounting now closes

Before anything else, the exclusion surface is fully reconciled. D-0008
recorded 42,538 records that carry `(` but yield no pair, without an
explanation. Every paren-bearing token is now accounted for exactly once:

| quantity | tokens |
| --- | --- |
| whitespace tokens containing `(` | **1,767,028** |
| kept as pairs | 1,424,314 |
| dropped, ten reasons | 342,714 |
| **residual** | **0** |

| quantity | records |
| --- | --- |
| records containing `(` | **380,282** |
| records contributing ≥1 pair | 337,744 |
| records contributing none | **42,538** |

Both record figures reproduce D-0008 exactly, from a separately written
traversal. **No category is missing and nothing is unheld.** The filter
chain was independently re-implemented three times in this supplement and
returned 1,424,314 / 342,714 each time.

---

## B. Every reported count is a *first-match* count — two categories are
   materially undercounted, the rest is a mechanical artifact

`abbrev_probe.extract_pairs` applies its ten tests in a fixed order and
stops at the first failure. Every published count is therefore "tokens this
reason caught **first**", not "tokens this reason describes". Measuring
order-independent membership:

| reason | first-match (published) | true membership | hidden | ratio |
| --- | --- | --- | --- | --- |
| inside_bracket_markup | 253,256 | 253,256 | 0 | 1.00 |
| non_alphabetic_expansion | 27 | 190,877 | 190,850 | 7069× |
| non_alphabetic_abbrev | 16,335 | 190,822 | 174,487 | 11.7× |
| token_carries_markup | 339 | 186,946 | 186,607 | 551× |
| editorial_marker_paren | 58,720 | **88,339** | 29,619 | 1.50× |
| greek_script | 12,987 | **15,800** | 2,813 | 1.22× |
| contains_numeral | 19 | 1,169 | 1,150 | 61.5× |
| unbalanced_parens | 561 | 637 | 76 | 1.14× |
| no_letters_outside_parens | 469 | 496 | 27 | 1.06× |
| nested_parens | 1 | 1 | 0 | 1.00 |

**Most of this table is not a finding.** `inside_bracket_markup` is tested
first, and a token overlapping a bracket span very often *contains* a
literal `[` or `]`. That makes it non-alphabetic and markup-carrying **by
construction**. The 7069×, 551× and 11.7× ratios are that mechanical
coupling, not 190,000 concealed symbol abbreviations. The same applies to
`contains_numeral`: EDCS writes lacunae as `[3]` ("three letters lost"), so
Arabic digits live almost entirely inside brackets — 1,130 of the 1,150
hidden numeral tokens are bracketed.

**Two entries are genuine undercounts,** because Greekness and
empty-parenthesis-ness are properties of a token that hold whether or not it
is bracketed:

- **`greek_script`: 12,987 published, 15,800 actual (+21.7%).** The brief
  asks whether this category is bilingual material worth separating. The
  pool is a fifth larger than the published figure implies.
- **`editorial_marker_paren`: 58,720 published, 88,339 actual (+50.4%).**

### B.1 The +50% does *not* grow the abstention class — checked, and it fails

The prior audit's most consequential recommendation is that the 42,805
"editor could not resolve it" tokens (`PR()`, `M()`) be recovered as an
explicit **abstention class**. A 50% undercount looked like it should
enlarge that pool by half. Sub-classifying the 29,606 hidden tokens by the
prior audit's own rule:

| sub-class of the hidden empty-paren tokens | count |
| --- | --- |
| abbreviation present, reading marked uncertain (`x(?)`) | 24,623 |
| standalone `(?)` uncertainty mark | 2,432 |
| **abbreviation present, editor could not resolve it** | **2,483** |
| standalone `(!)` sic mark | 44 |
| bare empty parentheses | 24 |

**The abstention pool grows from 42,805 to at most 45,288 — +5.8%, not
+50%.** The hidden mass is `(?)` uncertainty marking, which the prior audit
already and correctly ruled out of the abstention class. Those 2,483 sit
inside editorial restorations and carry the circularity risk quantified in
section D, so the defensible increment is smaller still.

**Stated plainly: I expected this check to overturn the prior audit's
headline recommendation and it did not.** The recommendation
`editorial_marker_paren → RECOVER AS SEPARATE CLASS, ~42,805 pairs` survives
intact. Only the category total attached to it was wrong.

---

## C. The `|(...)` inventory — 376 distinct forms, and it is not a mapping

The brief requires every distinct `|(...)` form enumerated with counts, on
the hypothesis that this is a symbol-to-word mapping and may be a separate
resource. The prior audit gave a sub-class total and 40 mixed examples. The
full enumeration is `data/derived/phase1_supplement.json → pipe_forms`:
**376 distinct forms, 16,194 occurrences.**

**The hypothesis is half right, and the half that is wrong matters.** It is
a separate resource. It is **not** a mapping. `|` is a single ASCII stand-in
onto which EDCS collapses at least eight unrelated epigraphic glyphs:

| sign family | distinct forms | occurrences |
| --- | --- | --- |
| centurial sign = *centuria* / *centurio* | 45 | 4,944 |
| reversed C (Ɔ) = *mulieris* / *Gaiae* | 13 | 4,077 |
| monetary sign = *denarius* and kin | 36 | 3,135 |
| milliary sign = *miliaria* / *milia* | 22 | 1,429 |
| fraction / weight / measure signs | 123 | 922 |
| *obitus* sign (theta nigrum) = deceased | 27 | 702 |
| Greek numeral and measure signs | 39 | 591 |
| other / unclassified | 64 | 291 |
| unresolved `\|()` — sign present, expansion withheld | 2 | 98 |
| Christian / religious signs | 5 | 5 |
| **total** | **376** | **16,194** |

*The grouping into families is a rule-based pass over the surface strings —
the rule is in `scripts/phase1_supplement*.py` and is reproducible — but
assigning a Latin word to an epigraphic glyph is a philological judgement.*
**[VERIFY — LATINIST].** The 376-form enumeration with counts is the primary
artifact and does not depend on the grouping. The 64 unclassified forms are
left unclassified rather than forced.

Four consequences, each measured:

1. **`|` is many-to-many.** A model given `|` and asked for a word must
   disambiguate *centuria* from *mulieris* from *denarius* from *uncia*
   using context alone. This is not a lookup table; it is the same
   context-disambiguation task as `V`, on a symbol vocabulary.
2. **Repeated bars encode plurality** — `||(mulierum)` 28, `||(centuriones)`
   8, `||(librae)` 11, `||(centurionibus)` 7, `|||(mulierum)` 1,
   `||||(milia)` 1. This is the **same geminatio principle as `DD(ominis)`**,
   applied to glyphs instead of letters. Phase 3a should treat them as one
   phenomenon, not two.
3. **The inflectional/lexical ambiguity structure repeats inside the
   symbol set.** *centuria / centurio / centurioni / centurionis /
   centurione / centuriae / centuriarum / centuriones / centurionibus /
   centurionum* are ten inflections of one lexeme behind one glyph.
4. **There is an abstention class here too** — `|()` and `||()`, 98
   occurrences: the sign is on the stone and the editor withheld the word.

### C.1 Editor-side label noise is directly visible here

**56 forms are single- or double-occurrence strings at edit distance 1 from
a high-frequency form in the same inventory** (rule: count ≤ 2, Levenshtein
1 from a form with count ≥ 20). Examples: `|(mulierus)`, `|(mulierisi)`,
`|(umulieris)`, `|(muleris)` against `|(mulieris)` (4,019); `|(centuroni)`,
`|(cenurionis)`, `|(centuronis)`, `|(centura)` against the centurial forms;
`|(mlliaria)`, `|(miiaria)`, `|(milariae)` against `|(miliaria)`;
`|(denarrii)`, `|(sertertius)`.

Some of these are genuine ancient orthographic variation and some are
keying errors by the editor; the two cannot be separated mechanically.
**[VERIFY — LATINIST].** What the count establishes without a Latinist is
that **the gold labels contain editor-side noise at a measurable rate**, in
the cleanest, most formulaic corner of the corpus. That is evidence for the
datasheet and for Phase 7's circularity probe, and it is a lower bound —
the rule cannot catch a typo more than one edit away or one that collides
with a real word.

### C.2 A sub-class the prior audit lumped

Its `non_alphabetic_abbrev → "other non-letter character"` bucket (2,572) is
not one thing:

| | tokens |
| --- | --- |
| erasure `⟦ ⟧` (rasura — text carved, then chiselled out) | 2,230 |
| quotation `« »` | 304 |
| other | 46 |
| Greek letter present | 25 |
| combining diacritic (overline etc.) | 4 |

`⟦Fl(avio) Constanti⟧no` is an abbreviation that **was** on the stone and was
later erased — *damnatio memoriae*, recorded by EDCS. Epigraphically that is
the opposite of an editorial restoration: the letters are attested, not
supplied. These 2,230 belong with `token_carries_markup` (RECOVER), not with
symbol abbreviations. Also note the **4 combining-diacritic tokens** — this
is the U+0305 sighting from D-0008, now with a count attached in this
category. Phase 2 measures it corpus-wide.

---

## D. Circularity, as a measured quantity

The prior audit argues circularity in prose. It is measurable. For every
`inside_bracket_markup` token, compute the share of the **abbreviation's**
letters (outside the parentheses) that sit inside a bracket span — 0.0 =
every letter carved, 1.0 = the abbreviation is wholly the editor's supplement.

| restored fraction | tokens | share | cumulative |
| --- | --- | --- | --- |
| 0.0 (fully attested) | 3,401 | 1.35% | 1.35% |
| 0.1 – 0.4 | 47,926 | 19.07% | 20.43% |
| 0.5 – 0.9 | 41,011 | 16.32% | 36.75% |
| **1.0 (fully editorial)** | **158,945** | **63.25%** | 100.00% |

n = 251,283 tokens carrying ≥1 abbreviation letter.

**This measure independently reproduces the prior audit's sub-classes.** Its
"whole thing is editorial reconstruction" (149,582) plus "abbreviation
restored, expansion outside the bracket" (9,359) plus "abbreviation
restored, expansion straddles the bracket" (4) = **158,945** — exactly the
count at restored fraction 1.0, derived here by a different method. Two
independent routes to the same number.

**Recommended for Phase 3.** `circularity_risk` should be written as this
continuous fraction, not a boolean. Rows at 1.0 must never enter a test set
(R4/4e): the editor inferred the letters *and* the expansion of the letters
they inferred, so a model scored on them is scored on the editor's habits.
**Only 1.35% of this category is fully attested** — the recoverable-in-
principle 253,256 shrinks to 3,401 tokens that are clean under the strictest
reading.

---

## E. Bias: the pooled exclusion set, with a null baseline

### E.1 The prior audit's TVD threshold is not sound at small n

It applies one rule at every sample size: *"anything above about 0.15 is a
materially different population."* Total variation distance is biased upward
by sampling noise, severely so at small n. Bootstrapping the null — draw n
provinces from the kept distribution, 200 draws, seed 20260820:

| category | n | observed TVD | null median | null p95 | observed / p95 |
| --- | --- | --- | --- | --- | --- |
| greek_script | 12,987 | 0.583 | 0.021 | 0.026 | **22.6×** |
| editorial_marker_paren | 58,720 | 0.197 | 0.010 | 0.012 | **16.4×** |
| inside_bracket_markup | 253,256 | 0.141 | 0.010 | 0.012 | **12.0×** |
| non_alphabetic_abbrev | 16,335 | 0.214 | 0.019 | 0.023 | **9.5×** |
| no_letters_outside_parens | 469 | 0.497 | 0.110 | 0.133 | 3.7× |
| token_carries_markup | 339 | 0.322 | 0.125 | 0.150 | 2.1× |
| unbalanced_parens | 561 | 0.197 | 0.101 | 0.126 | 1.6× |
| non_alphabetic_expansion | 27 | 0.691 | 0.422 | 0.500 | 1.4× |
| contains_numeral | 19 | 0.490 | 0.494 | 0.586 | **0.8×** |
| nested_parens | 1 | 0.983 | 0.968 | 0.995 | 1.0× |

**Corrections to the prior audit's readings:**

- `contains_numeral` (0.490, "a different population") and `nested_parens`
  (0.983) are **indistinguishable from sampling noise**. At n = 19 and
  n = 1 no distributional claim is possible in either direction.
- `unbalanced_parens` (0.197, "materially different") and
  `non_alphabetic_expansion` (0.691, "a different population") sit at
  1.6× and 1.4× the null p95 — **weak, not material**.
- `token_carries_markup` (0.322, "a different population") is 2.1× —
  **modest**.
- The four large categories are all real and the prior audit understated
  them by using an absolute threshold rather than a relative one.
  `greek_script` at 22.6× is the most sharply skewed exclusion in the
  dataset, not `inside_bracket_markup`.

**None of these corrections changes a recommendation.** Each of the small
categories was recommended on grounds other than its bias reading. What
changes is that six bias readings in the prior audit are not supportable as
written and must not be quoted in the paper.

**A second point on the summary table.** Its `bias risk if kept out` column
conflates two different questions. For `greek_script`: *is the excluded set
distributionally different?* — emphatically yes, 22.6×. *Does excluding it
bias the Latin-expansion task?* — arguably no, since Greek-script tokens are
not Latin expansion pairs. Both readings are defensible; the column reports
one and the TVD reports the other. The paper must separate them.

### E.2 Pooled across all ten filters — the union is what a user receives

| measure | kept | dropped | null p95 | verdict |
| --- | --- | --- | --- | --- |
| province TVD | — | **0.131** | 0.012 | 10.9× — material |
| century TVD | — | **0.097** | 0.006 | 16× — material |
| median inscription length | 119 | 140 | — | — |
| mean inscription length | 280 | **504** | — | 1.8× longer |

Province, pooled (categories ≥ 4,000 tokens):

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Asia | 6,967 | 2.03% | 0.52% | **3.88** |
| Sicilia | 3,669 | 1.07% | 0.31% | 3.51 |
| Provincia incerta | 18,301 | 5.34% | 1.85% | 2.89 |
| Achaia | 2,218 | 0.65% | 0.22% | 2.88 |
| Raetia | 4,136 | 1.21% | 0.44% | 2.76 |
| Thracia | 1,762 | 0.51% | 0.21% | 2.43 |
| Arabia | 2,413 | 0.70% | 0.33% | 2.15 |
| Moesia inferior | 6,318 | 1.84% | 1.03% | 1.80 |
| … | | | | |
| Apulia et Calabria / Regio II | 3,906 | 1.14% | 1.66% | 0.69 |
| Mauretania Caesariensis | 4,036 | 1.18% | 1.79% | 0.66 |
| Lusitania | 4,893 | 1.43% | 2.18% | 0.66 |
| Aemilia / Regio VIII | 2,805 | 0.82% | 1.34% | 0.61 |
| **Numidia** | 14,026 | 4.09% | 7.72% | **0.53** |

Century, pooled (n = 512,860 kept / 135,243 dropped resolving to one century):

| century | dropped | kept | share dropped | share kept | lift |
| --- | --- | --- | --- | --- | --- |
| 1BC | 4,371 | 25,944 | 3.23% | 5.06% | 0.64 |
| **1AD** | 25,773 | 137,851 | 19.06% | 26.88% | **0.71** |
| 2AD | 51,249 | 188,209 | 37.89% | 36.70% | 1.03 |
| 3AD | 32,967 | 98,518 | 24.38% | 19.21% | 1.27 |
| 4AD | 13,720 | 41,866 | 10.14% | 8.16% | 1.24 |
| **5AD** | 3,566 | 8,936 | 2.64% | 1.74% | **1.51** |
| 6AD | 1,708 | 6,374 | 1.26% | 1.24% | 1.02 |

**Stated plainly, as the brief requires: the exclusion is reshaping the
dataset, not merely filtering it.** Both pooled TVDs sit an order of
magnitude above sampling noise. The filters preferentially remove long,
damaged, Greek-East and fragmentary material, and preferentially retain
short, well-preserved, western, early-imperial material.

### E.3 This is a direct, quantified threat to the candidate finding

**Flagged here because Phase 1 found it, tested in Phase 3f.** The candidate
finding is that abbreviation meaning tracks province and shifts across eras —
*V = vixit* in **Numidia**/Africa versus *votum* in Pannonia/Narbonensis;
*C = Caius* in **1AD** shifting to *clarissimo* by **5–6AD**.

The exclusion set is skewed along **exactly those two axes, in exactly the
direction that would manufacture the finding**:

- **Numidia is the least-excluded large province in the corpus (lift 0.53)** —
  the African funerary formulae where *vixit* lives survive filtering at
  roughly twice the rate of the corpus average.
- **1AD is under-excluded (0.71) while 5AD is over-excluded (1.51)** — a 2.1×
  differential across precisely the span over which `C` is claimed to shift.

This does not show the candidate finding is an artifact. It shows that the
finding is currently measured on a corpus whose relevant strata have been
unevenly thinned by a filter that was never designed with this hypothesis in
mind, and that **a reviewer can say so in one sentence**. Phase 3f must
re-derive the province and century signal on the pre-exclusion population as
well as the kept set, and report both. If the signal only exists in the kept
set, it belongs to the filter, not to Rome.

---

## Revised summary table

Changes from `reports/exclusion_audit.md` in **bold**. Recommendations are
unchanged except where noted; what changes is the count or the bias reading
attached to them.

| category | published count | true membership | recommendation | recoverable | bias vs null |
| --- | --- | --- | --- | --- | --- |
| `inside_bracket_markup` | 253,256 | 253,256 | NEEDS HUMAN REVIEW | 64,991 — **but only 3,401 at circularity 0.0** | 12.0× material |
| `editorial_marker_paren` | 58,720 | **88,339** | RECOVER AS SEPARATE CLASS | 42,805 → **45,288 max (+5.8%)** | 16.4× material |
| `non_alphabetic_abbrev` | 16,335 | 190,822 (mechanical) | RECOVER AS SEPARATE CLASS | 16,335 | 9.5× material |
| `greek_script` | 12,987 | **15,800 (+21.7%)** | RECOVER AS SEPARATE CLASS | 15,800 | **22.6× — most skewed in the dataset** |
| `unbalanced_parens` | 561 | 637 | RECOVER | 338 | **1.6× — weak, was "material"** |
| `no_letters_outside_parens` | 469 | 496 | KEEP EXCLUDED | 0 | 3.7× material |
| `token_carries_markup` | 339 | 186,946 (mechanical) | RECOVER — **plus the 2,230 `⟦⟧` rasura tokens misfiled under `non_alphabetic_abbrev`** | 339 → **2,569** | **2.1× — modest, was "a different population"** |
| `non_alphabetic_expansion` | 27 | 190,877 (mechanical) | KEEP EXCLUDED | 0 | **1.4× — noise, was "a different population"** |
| `contains_numeral` | 19 | 1,169 (mechanical) | KEEP EXCLUDED | 0 | **0.8× — indistinguishable from noise** |
| `nested_parens` | 1 | 1 | KEEP EXCLUDED | 0 | **1.0× — no claim possible at n=1** |

**New deliverable:** `|(...)` symbol resource — **376 distinct forms,
16,194 occurrences, ≥8 sign families, with its own plurality marking and its
own abstention class.** Recommendation: **RECOVER AS SEPARATE CLASS**, and
not as a lookup table.

---

## Self-adversarial pass (R7)

**Most likely reviewer attack:** *"You audited the exclusions with a mirror
of the code that made them. If the chain's ordering distorts the counts, the
audit inherits the distortion."*

**Run, and it landed.** That is section B: every published count is
first-match, and two categories are materially undercounted. The audit did
inherit the distortion. It is now corrected.

**Second attack:** *"Your bias verdicts are an unsourced rule of thumb."*
**Run, and it landed** — section E.1, six of ten bias readings in the prior
audit are not supportable at their sample size.

**Third attack, the one that damages me:** *"You went looking for the prior
audit to be wrong and found two real defects — did you check whether its
conclusions actually change?"* **Run, and it failed.** Section B.1: I
expected the 50% undercount to enlarge the abstention class by half; it
enlarges it by 5.8%, because the hidden mass is `(?)` marking the prior
audit had already excluded on the right grounds. **Not one of the ten
recommendations changes.** What changes are counts, bias readings, one
misfiled sub-class of 2,230 tokens, and a missing deliverable. Reported
because R7 requires the outcome even when it deflates the supplement.

**What remains genuinely unresolved:**
- Whether partly-restored abbreviations belong in the task at all — the
  64,991 question. Unchanged, still needs a Latinist. **[VERIFY — LATINIST]**
- Whether the 376 `|` forms group into the sign families as I ruled them.
  **[VERIFY — LATINIST]**
- Which of the 56 near-duplicate `|` forms are editor typos and which are
  ancient orthographic variants. **[VERIFY — LATINIST]**
- Whether the pooled province/century skew has manufactured the candidate
  finding. **Owned by Phase 3f. This is the most important open question in
  the project.**

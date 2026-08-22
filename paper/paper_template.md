# What the Parentheses Hide: Editorial Convention in EDCS and the Construction of a Latin Abbreviation-Expansion Benchmark

**[VERIFY — AUTHOR NAMES]**¹

¹ **[VERIFY — AFFILIATION, POSTAL ADDRESS]**

**Correspondence:** **[VERIFY — EMAIL]**

---

**Abstract.** Latin inscriptions abbreviate heavily, and expanding those
abbreviations is a precondition for reading the epigraphic record at scale. The
task has had no dataset. It has an unusual source of supervision: epigraphic
editions record expansions in round parentheses, and the Epigraphik-Datenbank
Clauss-Slaby (EDCS) applies the convention throughout, printing `D(is)
M(anibus)` where the stone reads `D M`. We mine those parentheses for
{{pairs}} (abbreviation, expansion) pairs from {{records}} records.

Building the dataset required auditing the source, and that audit is the larger
result. A complete character census of {{chars}} characters shows that EDCS
does not preserve the numeral vinculum: U+0305 occurs {{u0305}} times, U+0304
never, and the Unicode code points for Roman monetary signs go unused. EDCS
substitutes a supplied word instead, writing `HS X(milia)` for what the stone
marked with an overline; the construction occurs {{milia_occ}} times,
{{hs_pct}}% of them after the sestertius sign. A single ASCII `|` carries at
least eight unrelated epigraphic glyphs across {{pipe_forms}} distinct forms.
Plural-marking geminatio doubles an abbreviation's final letter, not its first,
and a leading-letter rule undercounts it by 41%. Line breaks split words into
spurious pairs. The extraction filter reshapes the corpus, differing from
retained material by {{prov_ratio}}× and {{cent_ratio}}× a bootstrap null in
province and century.

We then test the hypothesis that motivated the dataset: that abbreviation
meaning tracks monument type through province and shifts across eras. The
association holds. Normalised mutual information between province and expansion
for `V` is {{nmi_v}}, about {{nmi_ratio}}× an inscription-level permutation
null. The predictive value does not. Province conditioning gains {{m1_delta}}
for a lookup model on provinces seen in training, {{m1h_delta}} on provinces
withheld from it, and {{m2_delta}} and {{m3_delta}} for a feature-based
re-ranker and a fine-tuned Latin transformer that read the surrounding text.
The signal is memorised local convention, not knowledge about Latin. We report
the negative result and present the work as a resource paper.

**Keywords:** Latin epigraphy, abbreviation expansion, dataset construction,
editorial convention, EDCS, benchmark design

---

## 1. Introduction

Roman inscriptions are terse by design. Stone is expensive, cutting is slow, and
the surviving formulae became conventional enough that a reader recovered a
great deal from very little. A gravestone opening `D M` was legible to any
literate passer-by as *Dis Manibus*, "to the spirits of the dead". The
dedicatory inscription at the base of Trajan's Column compresses eleven of its
thirty-five tokens.

That terseness blocks computational reading. A carved `V` stands for *vixit*
("he lived"), *votum* ("a vow"), *vivus* ("while living"), *vir* ("man") or
*Victrix* (a legionary title), and the letter itself does not distinguish them.
Resolving such abbreviations precedes almost any downstream task: named entity
recognition, prosopography, dating, search. The task has no dataset. Epigraphic
NLP has instead converged on *restoration*, recovering characters lost to
damage, and has left the expansion of characters that survive untreated.

Supervision for it exists in an unusual form. Editorial practice records the
expansion of an ancient abbreviation in round parentheses, distinct from square
brackets, which mark text the editor restored because the stone no longer
carries it. EDCS, the largest machine-readable collection of Latin inscriptions,
applies the convention across its corpus. Every parenthesis is a labelled
example contributed by a professional epigrapher, at a scale no annotation
project could fund. From {{records}} records we extract {{pairs}} pairs.

A condition comes attached, and it shapes what follows. A parenthesis records
what an editor judged an abbreviation to mean. It is an interpretation, not an
attestation. A restored lacuna can in principle be checked against a
rediscovered fragment; no independent record exists of what a carver intended by
`V`. A model trained on this data learns the conventions of an edition. Whether
it thereby learns anything about Roman practice is a separate question, and we
take care not to assume it.

We built the benchmark to test a specific hypothesis: that abbreviation meaning
tracks monument type through province, so that `V` reads as *vixit* in the
funerary landscape of Numidia and *votum* in the votive landscape of Pannonia,
and that it shifts across eras, `C` moving from *Caius* in the first century to
*clarissimus* by the fifth.

Both halves of the work returned something, in unexpected proportions. The
hypothesis failed in a specific way. The preparation, establishing what EDCS's
transcription conventions encode before mining them, turned up six properties of
the database that appear undocumented and that silently distort any dataset
derived by this route. Those findings should outlast the benchmark.

### 1.1 Contributions

1. **A dataset.** {{pairs}} pairs over {{records}} records, with
   monument-grouped frozen splits, four difficulty-targeted test sets, and the
   ceilings the construction imposes on each (Section 5).
2. **Six measured properties of EDCS's transcription conventions**: the
   unpreserved vinculum and its lexical substitute, the eight-way collapse of
   distinct signs onto ASCII `|`, final-letter geminatio, line-break
   fragmentation, a corpus-reshaping exclusion filter, and a measured floor for
   editorial label noise (Sections 4 and 6).
3. **A benchmark and a negative result.** Three baselines under three context
   conditions, showing that province conditioning is memorisation rather than
   generalisation, with a held-out province split that makes the difference
   visible (Sections 7 and 8).
4. **An explicit account of what remains unresolved**, including the judgements
   that require a Latinist and the redistribution permission that blocks release
   (Section 9).

### 1.2 Relation to LatEpig

LatEpig [5] is a retrieval tool. It reproducibly executes a search against EDCS
and exports the matching records. EpigCorpus is a derived labelled dataset and
benchmark. It mines EDCS's editorial parentheses as ground truth and measures
the conventions that distort any such derivation. One retrieves what EDCS holds;
the other measures what EDCS's conventions do to anything built from it. A user
who retrieves reproducibly with LatEpig and then mines the parentheses inherits
every artifact documented here.

---

## 2. Related work

### 2.1 Restoration rather than expansion

Recent work on ancient inscriptions concerns restoration. Ithaca [1] restores
and attributes ancient Greek texts, improving historians' restoration accuracy
from 25% to 72% when used as an assistive tool. Aeneas [2] extends the approach
to Greek and Latin, handling arbitrary-length restoration alongside
geographical and chronological attribution. For Latin, Locaputo et al. [8] apply
deep learning to filling lacunae.

All three address text lost from the stone, where the label is a gap. Our task
concerns text the stone deliberately compressed, where the label is an editorial
parenthesis. The two have opposite epistemic profiles. A restoration proposes
what was once physically present and can be falsified by a better-preserved
duplicate. An expansion proposes what a carver meant by a sign they chose, and
no external check exists. That asymmetry is why Section 6 measures editorial
disagreement instead of treating the labels as ground truth.

Aeneas's attribution task is the closest precedent for our context conditions,
running in the opposite direction: Aeneas predicts place and date from the text,
while we supply them and ask whether they help.

### 2.2 EDCS as a source for machine learning

Kaše et al. [7] reconcile the incompatible inscription-type taxonomies of EDCS
and the Epigraphic Database Heidelberg with a learned classifier. Their unit is
the whole inscription and their labels are curatorial metadata; ours are tokens
and editorial markup inside the text. Their finding that an epigraphic
database's categories are not neutral is the lesson our exclusion audit repeats
one level down, at transcription rather than taxonomy.

Heřmánková et al. [6] argue for treating epigraphic editions as datasets and
release LIRE, an aggregate of EDH and EDCS restricted to inscriptions that are
geolocated, inside the Empire's maximum extent, and dated within 50 BC to 350
AD: 182,852 records. This frames our argument and also opposes it. LIRE filters
for completeness; we audit the filtering. Their restriction to dated and
geolocated records is the class of selection our pooled-bias analysis quantifies
in Section 6.

Cui and Ströbel [3] provide the closest contemporary EDCS derivative: 1,000
manually annotated inscriptions with a fine-grained BIO scheme for Roman
personal names, on which a fine-tuned BERT reaches weighted F1 of 91.1 against
macro F1 of 68.7. That gap reflects the head-tail problem our frequency bands
quantify. Their work does not address abbreviation expansion. The contrast in
construction is instructive: 1,000 hand-annotated inscriptions against
{{pairs}} derived labels, trading label reliability for scale, and Section 6
measures what that trade costs.

### 2.3 Abbreviation indices

Elliott [4] compiled a standing index of Latin epigraphic abbreviations from
digital texts of all inscriptions published in *L'Année Épigraphique* between
1888 and 1993, in two tiers separated by a threshold of ten occurrences. It
remains the reference resource.

An index reports the expansions an abbreviation can take. Our dataset reports
the distribution over expansions it does take, conditioned on province and
century, with the ambiguity structure measured. Our results also qualify the
ambition. {{lex_pct}}% of ambiguity among evaluable keys is lexical, involving
genuinely different words, rather than inflectional, so no list resolves it and
neither does a model that ignores context. Section 8 shows that conditioning on
province is memorisation, which limits how far context-conditioning can be
pushed as a remedy.

### 2.4 Abbreviation expansion elsewhere

Abbreviation expansion is well studied in clinical text, scientific documents,
and medieval manuscript traditions. We are not aware of a dataset or benchmark
for Latin epigraphic abbreviation expansion. We state this as a bounded
negative, reflecting the searches we ran rather than a systematic review.

---

## 3. The corpus

EDCS holds Latin inscriptions from across the Roman world in machine-readable
form. The snapshot used here comprises {{records}} records and {{chars}}
characters, pinned by SHA-256 (`{{corpus_hash}}`) because the database is live
and grows. A re-harvest reproduces none of the figures below.

The records are not {{records}} inscriptions, and the difference matters for
construction. Record identifiers take the form `EDCS-<8 digits>-<n>`, where the
trailing segment marks a face or panel of one monument. {{monuments}} distinct
monuments underlie the {{records}} records, and {{multiseg}} records share a
base identifier with at least one other. All multi-segment groups agree
internally on province, and several hundred carry byte-identical inscription
text across segments. Treating records as independent documents places two faces
of one stone on opposite sides of a train/test division. Every split reported
here groups by monument.

The segment structure also gives the first direct evidence that the labels are
editorial. In one monument, segments print the same imperial titulature
differently: one as `divi Traiani / Parthici nepos`, another as `Parthic(i)
nep(os)`. The same text is judged abbreviated in one editorial pass and written
in full in another. The boundary between what a stone compresses and what it
spells out is not always a property of the stone.

---

## 4. What the plain text encodes

Mining a convention requires knowing what the convention encodes. Three of our
findings came from establishing that, and each affects the resulting dataset.

### 4.1 The vinculum is not preserved

Roman numerals were multiplied by a thousand with a line drawn above them, so
that `X̄` denotes ten thousand. Over letters rather than numerals, the same
overline marked an abbreviation. Whether EDCS's plain-text field preserves
either determines whether numeral-bearing tokens can be interpreted at all.

We ran a complete character census over all {{records}} records, covering
{{chars}} characters and all {{codepoints}} distinct code points, with no
sampling. A targeted search for the code points we expected would have proved
nothing; reading the result out of a full inventory does.

**Table 1.** Overline-bearing code points and related Unicode blocks.

| code point / block | occurrences |
| --- | --- |
| U+0305 combining overline | {{u0305}} |
| U+0304 combining macron | {{u0304}} |
| Number Forms U+2150–U+218F (incl. U+2183 Ↄ, reversed C) | {{numberforms}} |
| Ancient Symbols U+10190–U+101CF (Roman denarius, sextans, uncia) | {{ancientsym}} |

The multiplicative vinculum has no attestations. Unicode provides dedicated code
points both for the reversed C and for the Roman monetary signs, and EDCS uses
none of them. Appendix A reproduces the two surviving U+0305 instances; neither
is a multiplicative vinculum. One is an abbreviation overline over a letter, the
other sits on a numeral used as a word prefix.

The encoding pipeline is not the cause. {{underdots}} instances of U+0323, the
Leiden underdot marking an uncertain letter, pass through the same field.
Combining marks are transmitted, and overlines are absent by transcription
convention. At {{underdots}} instances, though, no Leiden diacritic is
systematically preserved either. The plain-text field is in practice
diacritic-free.

**What replaced it.** EDCS renders the overline as a supplied word inside
parentheses. The construction `N(milia)` occurs {{milia_occ}} times, and
{{hs_pct}}% of those occurrences follow `HS`, the sestertius sign, with small
multiplier numerals: `HS X(milia)` is ten thousand sesterces. The device
generalises past currency to the weights-and-measures system, covering *librae*,
*sextarii*, *modii* and *iugera*, and to ordinals, as in `p(ro) p(arte)
IIII(quarta)`, "for a fourth part".

The effect on dataset construction is direct. {{fused}} such pairs sit in the
extracted set as strings of the form `X → Xmilia`, which is not a Latin word and
was never carved. They are not abbreviation expansions: the supplied word was
never present, because on the stone it was a line. We class them separately and
exclude them from the task, on this measurement rather than on a definitional
stipulation.

### 4.2 One character for at least eight signs

Epigraphic texts carry signs no keyboard represents: the reversed C for
*mulieris*, the centurial sign, monetary and milliary marks, the theta nigrum
denoting a deceased person, and the fractional weight system inherited from the
*as*. EDCS renders all of them as a single ASCII `|`.

We enumerate every distinct `|(...)` form in the corpus: {{pipe_forms}} forms
across {{pipe_occ}} occurrences. Grouping them by the sign they plausibly stand
for, a rule-based pass over surface strings that needs philological
confirmation, yields at least eight families spanning the centurial sign, the
reversed C, monetary and milliary signs, fractions and weights, the *obitus*
mark, Greek measures and Christian symbols. The enumeration itself does not
depend on the grouping.

Expanding `|` is therefore not a lookup. It is the same context-disambiguation
problem as expanding `V`, conducted over a symbol vocabulary. A resource
publishing a `|` → word mapping would be wrong for every form outside the
majority family.

Plurality is marked by repetition, as in
`||(mulierum)`, `||(centuriones)` and `||||(milia)`, which is the geminatio of
Section 4.3 applied to glyphs; the two are one phenomenon. The inflectional
ambiguity of Latin recurs inside the symbol set, with ten inflections of
*centurio* behind one glyph. And the inventory carries its own unresolvable
class, `|()`, where the sign is on the stone and the editor declined to supply a
word.

### 4.3 Geminatio doubles the final letter

Latin epigraphy marks certain plurals by doubling a letter of the abbreviation:
`Aug → Augg(ustorum)`, `Imp → Impp(eratoribus)`, `Cos → Coss(ulibus)`,
`Caes → Caess(aribus)`, `Nob → Nobb(ilissimis)`. Concatenating abbreviation and
parenthesis content naively yields *Auggustorum*, which is not a word.

The doubling falls on the final letter. For a one-letter abbreviation the
doubled letter is both first and last, as in `D → DD(ominis)` and
`N → NN(ostris)`, so a rule keyed on a leading run reproduces every headline
example correctly while missing the AUGG/IMPP/CONSS/CAESS/NOBB family entirely.
We made that error. Our first implementation found 5,293 cases; the trailing-run
rule finds {{gem}}, a 41% undercount. The corrected forms then agree with an
independently derived prior diagnosis to within 1% on each of its ten most
frequent forms.

### 4.4 Line breaks fragment words

EDCS marks line breaks with `/` and does not rejoin words divided across them. A
word split as `v/` at one line's end and `ix(it)` at the next produces, under
token-level extraction, the pair `ix → ixit`. We identify {{linebreak}} such
rows.

They poison downstream heuristics rather than adding noise. The forty-eight
spurious `ixit` expansions produced this way were enough to make an
attestation-based rule misclassify the genuine abbreviation `V(ixit)` as an
artifact. We flag rather than remove them, since judging whether a given
fragment is broken requires reading the Latin.

---

## 5. Dataset construction

### 5.1 Extraction

Extraction operates on whitespace tokens rather than individual parenthesis
groups, because an interior expansion such as `co(n)s(ul)` is one abbreviation
carrying two insertions and yields the single pair `cos → consul`. Square,
angle and curly brackets are masked as spans, since text inside them is
editorially restored rather than carved. A `/` is a hard token boundary, so no
abbreviation is read across a line break.

The accounting closes exactly. {{tokens_paren}} whitespace tokens contain an
opening parenthesis, of which {{pairs}} become pairs and {{dropped}} are
excluded for ten reasons, leaving no residual. At record level, {{recs_paren}}
records contain a parenthesis and {{recs_pair}} contribute at least one pair.

Re-running an extractor reproduces its bugs as faithfully as its successes, so
we verified the primary extractor against a second implementation written
independently from the editorial convention rather than from the first
implementation's code. The two agree on {{agree}} pairs, {{agree_pct}}% of the
primary output, and every disagreement is accounted for: the second
implementation's shortfalls trace to normalisation it lacks, and its surplus
consists of bracket-contaminated fragments the primary correctly rejects. A
misreading of the editorial convention shared by both implementations would be
invisible to this check.

### 5.2 Corrections

Corrections are added columns, never edits. The original abbreviation and
expansion are carried through unmodified, exclusions are flags rather than
deletions, and anything the explicit rules cannot reach is marked unresolved
rather than imputed. Every decision is reversible, and a consumer who disagrees
with one of our judgements can undo it without re-running extraction.

Geminatio is corrected by reducing the trailing doubled run to a single letter
in abbreviation and expansion together, arbitrated against a control lexicon
built only from tokens carrying no doubled run, so the test cannot feed on the
forms it judges. {{gem}} pairs are corrected and {{gem_unresolved}} are left
unresolved because neither reading is attested.

Numeral cases are decided by three explicit, printed word lists rather than by a
threshold.

**Table 2.** Numeral classes and their treatment.

| class | pairs | treatment |
| --- | --- | --- |
| Type 1, the numeral stands for the word (`XL(quadragesimae)`) | {{type1}} | expansion is the bracket content alone |
| Type 2, numeral plus supplied unit (`X(milia)`) | {{type2}} | numeral and unit stored separately, flagged out of the task |
| Type 3, numeral as word prefix (`VI(vir)`) | {{type3}} | gold label is the EDCS surface form |

For Type 3 the gold label is the surface form `VIvir`, never the Latin reading
*sevir*, which is confined to a normalisation column. A gold label must be
verifiable against the source and should not rest on a contested scholarly
reading. The same office is written both `VI(vir)` and `IIIIII(vir)`; we keep
those distinct in the label and merge them only in normalisation.

We did not discard records dated after 700 AD. Inspecting all records dated
after 1000 AD individually, {{late_ok}} of {{late_total}} prove correctly dated:
genuine early-medieval Christian epitaphs, a self-dated fifteenth-century Latin
inscription, an eighteenth-century Spanish one. Exactly two are demonstrably
mis-keyed, and each is flagged with its evidence rather than removed. A
threshold rule would have discarded nineteen genuine records.

### 5.3 Splits

The primary split is 80/10/10 ({{train_n}} / {{val_n}} / {{test_n}}), grouped by
monument. Assignment uses a stable hash of the monument identifier rather than a
shuffle, so it depends on nothing but the seed and the identifier. Zero of
{{groups}} monument groups straddle the division.

Three provinces, {{heldout_list}}, are withheld entirely for a second split. We
chose one per genre regime as separated by the dominant reading of `V`, across
three geographically separated zones, each with a same-regime sister province
left in training so the test measures transfer rather than absence.

Alongside the primary test set we release sets targeting specific difficulties:
a lexical set restricted to keys whose expansions differ as words; a
discriminating variant additionally requiring the majority baseline to be wrong;
a rare-form set for keys seen fewer than ten times in training; a set of forms
unseen in training; and a de-duplicated variant discussed next.

Monument grouping does not prevent all leakage. {{dup_rows}} test rows
({{dup_pct}}%) are byte-identical to a training row, context included, because
different monuments carry identical formulae and the context window is finite.
No identity-based grouping rule catches this, since the monuments genuinely
differ. Whether it constitutes leakage or is the domain, given that an
epigrapher reading `D(is) M(anibus)` for the thousandth time is also performing
lookup, we do not resolve. We publish both the full test set and the
de-duplicated variant, and report both.

### 5.4 Ceilings

Our models operate by candidate ranking: for a given abbreviation the candidates
are the expansions observed for it in training. This keeps a lookup baseline and
a transformer comparable, since all score the same set, and it imposes ceilings
that must be reported alongside any accuracy.

**Table 3.** Ceilings imposed by candidate ranking.

| test set | ceiling |
| --- | --- |
| primary | {{primary_ceiling}} |
| rare form | {{rare_ceiling}} |
| unseen form | {{unseen_ceiling}} |

The unseen-form set is unanswerable by construction. Every key in it is absent
from training, so no candidate exists. Reporting accuracy on these sets without
their ceilings misrepresents every model evaluated on them.

---

## 6. Artifacts and bias

### 6.1 The exclusion filter reshapes the corpus

{{dropped}} tokens are excluded. A filter that removes a random slice of a
corpus costs volume; one that removes a stratum changes what the dataset says
while looking like hygiene. Pooled across all ten exclusion reasons, the dropped
material differs from the retained material by province TVD {{prov_tvd}}
against a bootstrap null p95 of {{prov_null}} ({{prov_ratio}}×) and century TVD
{{cent_tvd}} against {{cent_null}} ({{cent_ratio}}×). Dropped texts average
{{drop_len}} characters against {{kept_len}} for retained ones.

The filter preferentially removes long, damaged, Greek-East and fragmentary
material and preferentially retains short, well-preserved, western,
early-imperial material. Any study of regional or diachronic variation conducted
on the retained material inherits that reshaping.

Two methodological traps are easy to fall into here. Total variation distance is
biased upward by sampling noise, severely so at small n: the null p95 in our
data is 0.012 at n = 253,256 and 0.586 at n = 19. An absolute threshold applied
uniformly declares small categories skewed when noise alone produces the
observed value, so bias must be assessed against a null computed at each
category's own sample size. Second, filter chains that stop at the first
matching reason report first-match counts rather than category membership, so a
category tested late is undercounted, in our case by 22% and 50% for two
categories. Most of that effect is mechanical and should not be reported as a
finding, but the two genuine undercounts would otherwise propagate silently.

### 6.2 Circularity, measured

For every token excluded as bracket markup we compute the share of the
abbreviation's own letters falling inside an editorial restoration.
{{circ_pct}}% of such tokens have a wholly editorial abbreviation, where the
editor inferred the letters and then the expansion of the letters they inferred,
and only {{attested_pct}}% are fully attested. Recovering this material into a
training set imports that circularity, and it must never enter a test set.

### 6.3 Editorial label noise

Holding context byte-identical on both sides, {{noise_rows}} rows
({{noise_pct}}%) carry an expansion differing from another row that presents the
same evidence. The disagreements mix apparent keying errors with genuine
orthographic variation, and the two cannot be separated mechanically. This is a
floor: the rule catches only disagreements where the surrounding context matches
exactly.

Separately, {{plain_pct}}% of distinct expansion forms also occur in the corpus
as uncontracted plain text. *Vixit* appears 31,388 times as an expansion and
25,244 times carved in full. That is variation between stones rather than
editorial inconsistency, and we resist the stronger reading. It does fix the
task's real definition. The dataset consists entirely of cases where an editor
judged an abbreviation present, so the task is properly stated as *given that an
editor marked this abbreviated, what did they expand it to*, a narrower and more
edition-dependent question than "expand Latin epigraphic abbreviations".

---

## 7. Experimental setup

We evaluate three baselines under three context conditions. All operate by
candidate ranking, so the context delta is measured identically across model
classes.

**M1**, a most-frequent-expansion lookup ({{m1_params}} parameters), is the
floor and trivially reproducible. **M2** is a logistic-regression candidate
re-ranker ({{m2_params}} parameters) over character n-grams of the abbreviation,
context words on both sides, and conditioning counts, all conjoined with the
candidate, since a feature that does not mention the candidate cannot
discriminate between candidates of the same row. **M3** fine-tunes a Latin
encoder, `bowphs/LaBerta` ({{m3_params}} parameters), with a mean-pooled
classification head scored at inference against the same candidate set.

The conditions are C1, local text only; C2, adding province; and C3, adding
province and century. For M3 the conditions are injected as text prefixes, so
the only thing that varies is what the model is told.

Two deviations from the intended protocol. M3 was trained on
{{m3_rows}} rows, {{m3_pct}}% of the training split, at a single seed, for
compute reasons; its seed variance is unmeasured and its accuracy is a lower
bound. And we rejected a pretrained encoder considered first because loading it
required executing code downloaded from the model repository.

---

## 8. Results

### 8.1 The context conditions

**Table 4.** Accuracy on the primary test set, mean over seeds.

| model | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1 lookup (cannot read text) | {{m1_c1}} | {{m1_c2}} | {{m1_c3}} | {{m1_delta}} |
| M2 linear re-ranker | {{m2_c1}} | {{m2_c2}} | {{m2_c3}} | {{m2_delta}} |
| M3 fine-tuned LaBerta | {{m3_c1}} | {{m3_c2}} | {{m3_c3}} | {{m3_delta}} |

The hypothesis fails. Province and century gain {{m1_delta}} for a model that
cannot read the surrounding text and nothing for either model that can. M1's
gain far exceeds its seed variance; both text-reading models show a small
negative delta.

Province acts as a proxy for textual context rather than as independent
information. A model with access only to the abbreviation and its metadata
benefits from being told the province, because province correlates with genre
and genre determines the reading. A model that sees the adjacent words already
has that information more directly, so the province feature contributes little
while costing capacity.

### 8.2 The association is real; the transfer is not

The province signal exists, and it is strong. Normalised mutual information
between province and expansion choice for `V` is {{nmi_v}}, about
{{nmi_ratio}}× a permutation null computed at the level of whole inscriptions
rather than pairs, which is the conservative choice since pairs from one stone
are not independent. The association survives both cleaning and the restoration
of the excluded strata described in Section 6.1. The underlying distributions
are unambiguous: in Numidia `V` is *vixit* in 93% of cases, and in Pannonia
superior it is *votum* in 67%.

The signal does not transfer. Retraining without the three withheld provinces
and testing on exactly those provinces:

**Table 5.** M1 trained without the withheld provinces, evaluated on them.

| | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1, withheld provinces | {{m1h_c1}} | {{m1h_c2}} | {{m1h_c3}} | {{m1h_delta}} |

Zero to four decimal places, across all seeds. M1's province conditioning is a
lookup table keyed on province, and for a province absent from training the
table is empty, so the conditioned model backs off to the unconditioned one. The
gain observed on the primary test set is memorisation of province-specific
distributions, not a transferable generalisation about Latin.

A held-out province split is required to see this. Evaluated pooled, province
conditioning looks like knowledge. Our own first version of this experiment
trained on a split containing the withheld provinces and reported the opposite
conclusion. We caught it only because held-out accuracy came out implausibly
above primary-test accuracy.

### 8.3 Cost

**Table 6.** Cost against accuracy at C1.

| model | parameters | training time | training rows | accuracy (C1) |
| --- | --- | --- | --- | --- |
| M1 | {{m1_params}} | {{m1_train}} s | {{train_n}} | {{m1_c1}} |
| M2 | {{m2_params}} | {{m2_train}} s | {{train_n}} | {{m2_c1}} |
| M3 | {{m3_params}} | {{m3_train}} s | {{m3_rows}} | {{m3_c1}} |

M2 attains {{recover_pct}}% of M3's accuracy at a small fraction of the
parameters and training time. M3 was trained on {{m3_pct}}% of the data
available to M2, so the gap understates what a fully trained encoder would
achieve and the comparison favours the cheaper model.

---

## 9. Limitations

1. Expansions are editorial interpretations, not attested text. A model trained
   here demonstrably learns EDCS conventions; that it learns Roman practice does
   not follow and is not shown.
2. The task is conditioned on the editor having judged an abbreviation present.
   {{plain_pct}}% of expansion forms also occur uncontracted.
3. Editorial disagreement is at least {{noise_pct}}%, and that figure is a
   floor.
4. The exclusion filter is bias-inducing, at {{prov_ratio}}× and
   {{cent_ratio}}× a null in province and century.
5. EDCS assigns a resolvable single-century date to {{dated_pct}}% of records,
   so every diachronic result rests on that third.
6. Survival and excavation bias are inherited from the epigraphic record and
   uncorrected here.
7. {{dup_pct}}% of the primary test split is byte-identical to a training row.
   We publish a de-duplicated variant but do not resolve whether the duplication
   is contamination or the domain.
8. The held-out province split confounds province with century. The withheld
   provinces are late-Romanised frontier territory, so results on it must be
   stratified by century rather than pooled.
9. Several judgements require a Latinist and have not received one: the numeral
   word lists, the `|` sign families, which near-duplicate forms are keying
   errors rather than ancient variants, whether the inflectional and lexical
   distinction is drawn correctly, and whether `X(milia)` renders a vinculum.
10. M3 used {{m3_rows}} training rows at one seed, so its result is a lower
    bound and its variance is unmeasured. M2's and M3's deltas agree at four
    decimal places, which at one seed is coincidence at that precision; the
    defensible claim is that both text-reading models show a small negative
    delta.
11. Redistribution permission from EDCS is unresolved and blocks release of the
    derived tables. Code, reports and measured statistics are unaffected.

---

## 10. Conclusion

The benchmark exists: {{pairs}} labelled pairs over {{records}} records, with
frozen monument-grouped splits, difficulty-targeted test sets, and the ceilings
its construction imposes. The hypothesis that motivated it failed. Province
predicts meaning in the data, with an association many times a conservative
null, yet transfers not at all to an unseen province and adds nothing to a model
that reads the surrounding Latin.

Preparing the dataset required auditing the source, and that audit produced the
findings we expect to be reused. EDCS does not preserve the numeral vinculum and
substitutes a supplied word. It collapses at least eight distinct epigraphic
signs onto one ASCII character. It marks plurality on an abbreviation's final
letter. It fragments words across line breaks into spurious pairs. And any
conventional extraction filters it in a way that reshapes the corpus along the
geographic and chronological axes one might wish to study. Each is invisible to a
consumer who mines the parentheses and starts training, and each has a measured
magnitude.

Anyone deriving a dataset from an epigraphic edition should budget for the same
audit. The conventions of a source edition are not neutral with respect to the
questions asked of the data, and the six documented here took longer to find
than the three baselines took to train.

---

## Acknowledgements

**[VERIFY — ACKNOWLEDGEMENTS, FUNDING]**

We thank the compilers and maintainers of EDCS, without whose database this work
would not exist.

## Data and code availability

Code, reports and all measured statistics are available at
**[VERIFY — REPOSITORY URL]**. The derived data tables are not released:
redistribution permission from EDCS has not been granted. Code is licensed MIT;
the derived data carries a separate licence pending that permission. The full
pipeline is reproduced by a single command, which verifies the corpus SHA-256
before running and refuses a different snapshot.

## References

[1] Y. Assael, T. Sommerschield, B. Shillingford, M. Bordbar, J. Pavlopoulos,
M. Chatzipanagiotou, I. Androutsopoulos, J. Prag, N. de Freitas, Restoring and
attributing ancient texts using deep neural networks, Nature 603 (2022)
280–283. doi:10.1038/s41586-022-04448-z.

[2] Y. Assael, T. Sommerschield, A. Cooley, B. Shillingford, J. Pavlopoulos,
P. Suresh, B. Herms, J. Grayston, B. Maynard, N. Dietrich, R. Wulgaert, J. Prag,
A. Mullen, S. Mohamed, Contextualizing ancient texts with generative neural
networks, Nature 645 (2025) 141–147. doi:10.1038/s41586-025-09292-5.

[3] W. Cui, P. B. Ströbel, Across generations: a comparative analysis of NER for
Latin inscriptions from classical machine learning to LLMs, in: Proceedings of
the Fourth Workshop on Language Technologies for Historical and Ancient
Languages (LT4HALA 2026), ELRA, 2026, pp. 112–124. doi:10.63317/2g99sovd35pj.

[4] T. Elliott, Abbreviations in Latin Inscriptions, American Society of Greek
and Latin Epigraphy, 1998. URL:
https://www.asgle.org/epigraphical-resources/abbreviations-in-latin-inscriptions/.

[5] P. Heřmánková, B. Ballsun-Stanton, R. Laurence, FAIR turn in epigraphy: low
barrier pathways to quantitative and reproducible research in Latin epigraphy,
in: Computational Humanities Research 2024, volume 3834 of CEUR Workshop
Proceedings, CEUR-WS.org, 2024, pp. 649–661. URL:
https://ceur-ws.org/Vol-3834/paper4.pdf.

[6] P. Heřmánková, V. Kaše, A. Sobotková, Inscriptions as data: digital
epigraphy in macro-historical perspective, Journal of Digital History 1 (2021)
99–141. doi:10.1515/jdh-2021-1004.

[7] V. Kaše, P. Heřmánková, A. Sobotková, Classifying Latin inscriptions of the
Roman Empire: a machine-learning approach, in: Proceedings of the Conference on
Computational Humanities Research 2021, volume 2989 of CEUR Workshop
Proceedings, CEUR-WS.org, 2021, pp. 123–135. URL:
https://ceur-ws.org/Vol-2989/short_paper12.pdf.

[8] A. Locaputo, B. Portelli, E. Colombi, G. Serra, Filling the lacunae in
ancient Latin inscriptions, in: Information and Research Science Connecting to
Digital and Library Science 2023, volume 3365 of CEUR Workshop Proceedings,
CEUR-WS.org, 2023, pp. 68–76. URL: https://ceur-ws.org/Vol-3365/short5.pdf.

---

## Appendix A. The surviving combining marks

{{appendix_marks}}

## Appendix B. Reproduction

Every numeric value in this paper is injected from a single results file by a
build script. None is typed by hand, and any unresolved value is emitted as a
visible marker rather than silently omitted. The pipeline is reproduced by one
command, which verifies the corpus SHA-256 (`{{corpus_hash}}`) before running
and refuses to proceed against a different snapshot. Decisions taken during
construction are logged with their evidence, the alternatives rejected, and what
would overturn them, in {{n_decisions}} entries.

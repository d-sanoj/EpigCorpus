# What the Parentheses Hide: Editorial Convention in EDCS and the Construction of a Latin Abbreviation-Expansion Benchmark

**[VERIFY — AUTHORS, AFFILIATIONS, CORRESPONDING ADDRESS]**

---

## Abstract

Latin inscriptions abbreviate heavily and inconsistently, and expanding those
abbreviations is a precondition for reading the epigraphic record at scale. The
task has had neither a dataset nor a benchmark. It has, however, an unusual
source of supervision: epigraphic editions record expansions in round
parentheses, and the Epigraphik-Datenbank Clauss-Slaby (EDCS) applies the
convention across its entire corpus, so that a stone reading `D M` is printed
`D(is) M(anibus)`. We mine those parentheses and obtain **1,424,314**
(abbreviation, expansion) pairs from **588,509** records.

Building the dataset turned out to require auditing the source, and the audit is
this paper's larger contribution. We report six previously undocumented
properties of EDCS's plain-text transcription, each measured rather than
asserted. A complete character census of **39,470,885** characters establishes
that the numeral vinculum is **not preserved**: U+0305 occurs 2 times
and U+0304 not at all, and the dedicated Unicode code points for Roman monetary
signs are unused. EDCS instead substitutes a supplied word, so that `HS
X(milia)` renders what the stone marked with an overline; the construction
occurs 1,875 times, 65.4% of them preceded by the sestertius sign.
A single ASCII `|` stands for at least eight unrelated epigraphic glyphs across
**376 distinct forms**. Plural-marking geminatio doubles an
abbreviation's *final* letter rather than its first, and the natural
leading-letter rule undercounts it by 41%. Line breaks fragment words into
spurious pairs. And the extraction filter *reshapes* the corpus rather than
merely filtering it, differing from the retained material at 11.1×
and 16.3× a bootstrap null in province and century respectively.

We then use the dataset to test a hypothesis that motivated its construction:
that abbreviation meaning tracks monument type through province, and shifts
across eras. The association is real and strong — normalised mutual information
between province and expansion for `V` is 0.3709, some 12.2× an
inscription-level permutation null. The predictive value is not. Province
conditioning is worth **+0.0833** to a lookup model on provinces seen in
training, **exactly +0.0000** on provinces withheld from it, and nothing
at all — -0.0070 and -0.0070 — to a feature-based re-ranker and a
fine-tuned Latin transformer that can read the surrounding text. The signal is
memorisation of local convention rather than knowledge about Latin. We report
this as a negative result and present the work as a resource paper.

**Keywords:** Latin epigraphy · abbreviation expansion · dataset construction ·
editorial convention · EDCS · benchmark design

---

## 1 Introduction

Roman inscriptions are terse by design. Stone is expensive, cutting is slow, and
the formulae that dominate the surviving record — funerary dedications, votive
offerings, building inscriptions, military diplomas — became conventional
enough that a reader could recover a great deal from very little. A gravestone
opening `D M` was legible to any literate passer-by as *Dis Manibus*, "to the
spirits of the dead". The dedicatory inscription at the base of Trajan's Column
compresses eleven of its thirty-five tokens.

That terseness is a problem for anyone reading the corpus computationally. A
single carved letter `V` may stand for *vixit* ("he lived"), *votum* ("a vow"),
*vivus* ("while living"), *vir* ("man") or *Victrix* (a legionary title), and
nothing in the letter itself distinguishes them. Resolving such abbreviations is
a precondition for almost any downstream task: named entity recognition,
prosopography, dating, or simply search. Yet the task has no dataset. The
epigraphic NLP literature has converged instead on *restoration* — recovering
characters lost to damage — leaving the expansion of characters that are
present unaddressed as a task in its own right.

Supervision for it nonetheless exists, in an unusual form. Epigraphic editorial
practice records the expansion of an ancient abbreviation in round parentheses,
distinguishing it from square brackets, which mark text the editor has restored
because the stone no longer carries it. EDCS, which holds the largest collection
of Latin inscriptions in machine-readable form, applies the convention
throughout. Every parenthesis is therefore a labelled example, contributed by a
professional epigrapher, at a scale no annotation project could fund. From
588,509 records we extract 1,424,314 such pairs.

The labels come with a condition attached, and it shapes everything that
follows. A parenthesis records **what an editor judged an abbreviation to
mean**. It is an interpretation, not an attestation. Unlike a restored lacuna,
which may in principle be checked against a rediscovered fragment, there is no
independent record of what a carver intended by `V`. A model trained on this
data learns the conventions of an edition; whether it thereby learns anything
about Roman practice is a separate question, and one we take some care not to
assume.

We set out to build a benchmark and to test a specific hypothesis: that
abbreviation meaning tracks monument type through province — `V` as *vixit* in
the funerary landscape of Numidia, as *votum* in the votive landscape of
Pannonia — and shifts across eras, `C` moving from *Caius* in the first century
to *clarissimus* by the fifth. The dataset was constructed to make that testable.

Both parts of the work returned something, but not in the proportions expected.
The hypothesis failed in a specific and interesting way. Meanwhile, the
preparation — establishing what EDCS's transcription conventions actually encode
before mining them — turned up six properties of the database that are, so far
as we have been able to determine, undocumented, and each of which silently
distorts any dataset derived by this route. Those findings are the contribution
we expect to be most durable.

**Contributions.**

1. **A dataset.** 1,424,314 (abbreviation, expansion) pairs over 588,509
   records, with monument-grouped frozen splits and four difficulty-targeted
   test sets, together with the ceilings the construction imposes on each
   (Section 5).
2. **Six measured properties of EDCS's transcription conventions**: the
   unpreserved vinculum and its lexical substitute; the eight-way collapse of
   distinct signs onto ASCII `|`; final-letter geminatio; line-break
   fragmentation; the corpus-reshaping exclusion filter; and a measured floor
   for editorial label noise (Sections 4 and 6).
3. **A benchmark and a negative result.** Three baselines under three context
   conditions, showing that province conditioning is memorisation rather than
   generalisation, with a held-out province split that makes the distinction
   visible (Sections 7 and 8).
4. **An explicit account of what is unresolved**, including the judgements that
   require a Latinist and the redistribution permission that blocks release
   (Section 9).

**Relation to LatEpig.** Because the question is inevitable, we answer it
directly: LatEpig is a *retrieval* tool, which reproducibly executes a search
against EDCS and exports the matching records. EpigCorpus is a *derived labelled
dataset and benchmark*, which mines EDCS's editorial parentheses as ground truth
and measures the conventions that distort any such derivation. One retrieves
what EDCS holds; the other measures what EDCS's conventions do to anything built
from it. The two are complementary, and a user who retrieves reproducibly with
LatEpig and then mines the parentheses will inherit every artifact documented
here.

---

## 2 Related work

### 2.1 Restoration rather than expansion

The most visible recent work on ancient inscriptions concerns restoration.
Ithaca (Assael et al., 2022) restores and attributes ancient Greek texts,
improving historians' restoration accuracy from 25% to 72% when used as an
assistive tool. Aeneas (Assael et al., 2025) extends the approach to Greek and
Latin, handling arbitrary-length restoration alongside geographical and
chronological attribution and parallel retrieval. For Latin specifically,
Locaputo et al. (2023) apply deep learning to filling lacunae.

All three address text **lost from the stone**, where the label is a gap. Our
task concerns text the stone **deliberately compressed**, where the label is an
editorial parenthesis. The distinction is not merely one of framing, because the
two have opposite epistemic profiles. A restoration proposes what was once
physically present and can, in principle, be falsified by the discovery of a
better-preserved duplicate. An expansion proposes what a carver *meant* by a
sign they chose deliberately, and no such external check exists. This asymmetry
is why Section 6 devotes attention to measuring editorial disagreement rather
than assuming the labels are ground truth.

Aeneas's attribution task is the closest precedent for the context conditions in
our experiment, but it runs in the opposite direction: Aeneas *predicts* place
and date from the text, whereas we *supply* them and ask whether they help.

### 2.2 EDCS as a source for machine learning

Kaše et al. (2021) address the incompatibility of the inscription-type
taxonomies used by EDCS and the Epigraphic Database Heidelberg, learning a
classifier that maps between them. Their unit is the whole inscription and their
labels are curatorial metadata; ours are tokens and editorial markup inside the
text. Their finding that the categories of an epigraphic database are not
neutral is precisely the lesson our exclusion audit repeats one level down, at
the level of transcription rather than taxonomy.

Heřmánková et al. (2021) argue for treating epigraphic editions as datasets in
their own right and release LIRE, an aggregate of EDH and EDCS restricted to
inscriptions that are geolocated, inside the Empire's maximum extent, and dated
within 50 BC–350 AD: 182,852 records. This is the framing paper for our
argument, and also its foil. LIRE filters for completeness; we audit the
filtering. Their restriction to dated and geolocated records is exactly the
class of selection our pooled-bias analysis quantifies in Section 6.

Cui and Ströbel (2026) provide the closest contemporary EDCS derivative: 1,000
manually annotated inscriptions with a fine-grained BIO scheme for Roman
personal names, on which a fine-tuned BERT reaches weighted F1 of 91.1 against
macro F1 of 68.7. The gap between those two figures reflects the same head-tail
problem our frequency bands quantify. Their work does not address abbreviation
expansion. The contrast in construction is instructive: 1,000 hand-annotated
inscriptions against 1,424,314 derived labels — manual precision against derived
scale, with the trade-off in label reliability that Section 6 measures.

### 2.3 Abbreviation indices

Elliott (1998) compiled a standing index of Latin epigraphic abbreviations from
digital texts of all inscriptions published in *L'Année Épigraphique* between
1888 and 1993, in two tiers separated by a threshold of ten occurrences. It
remains the reference resource.

An index reports the expansions an abbreviation *can* take. Our dataset reports
the distribution over expansions it *does* take, conditioned on province and
century, with the ambiguity structure measured. That is the gap the resource
fills. Our results, however, also qualify the ambition: **54.5%** of
ambiguity among evaluable abbreviation keys is *lexical* — genuinely different
words — rather than merely inflectional, so no list resolves it and neither does
a model that ignores context. And, as Section 8 shows, conditioning on province
proves to be memorisation rather than knowledge, which limits how far
context-conditioning can be pushed as a solution.

### 2.4 Abbreviation expansion elsewhere

Abbreviation expansion is well studied in clinical text and scientific
documents, and in medieval manuscript traditions where abbreviation is likewise
systematic. We are not aware of a dataset or benchmark for Latin *epigraphic*
abbreviation expansion. We state this as a bounded negative: it reflects the
searches we ran, not a systematic review.

---

## 3 The corpus

EDCS holds Latin inscriptions from across the Roman world in machine-readable
form. The snapshot used here comprises **588,509** records and
**39,470,885** characters, pinned by SHA-256 (`9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317`) because the
database is live and grows; a re-harvest reproduces none of the figures below.

The records are **not** 588,509 inscriptions, and the distinction matters
for dataset construction. Record identifiers take the form `EDCS-<8 digits>-<n>`,
and the trailing segment marks a face or panel of a single monument.
**542,854** distinct monuments underlie the 588,509 records, with
45,655 records sharing a base identifier with at least one other. All
multi-segment groups are internally consistent in province, and several hundred
carry byte-identical inscription text across segments. Treating records as
independent documents would therefore place two faces of one stone on opposite
sides of a train/test division. Every split reported here groups by monument.

The segment structure also provides the first direct evidence of the editorial
character of the labels. In one monument, segments print the same imperial
titulature differently — one as `divi Traiani / Parthici nepos`, another as
`Parthic(i) nep(os)` — so that the same text is, in one editorial pass, judged
to be abbreviated and in another judged to be written in full. The boundary
between what the stone compresses and what it spells out is not always a
property of the stone.

---

## 4 What the plain text does and does not encode

Before mining a convention it is worth establishing what the convention
encodes. Three of our findings emerged from doing so, and each affects the
resulting dataset materially.

### 4.1 The vinculum is not preserved

Roman numerals were multiplied by a thousand by drawing a line above them: `X̄`
denotes ten thousand. Over letters rather than numerals, the same overline
marked an abbreviation. Whether EDCS's plain-text field preserves either
determines whether numeral-bearing tokens can be interpreted at all.

We ran a complete character census over all 588,509 records — all
39,470,885 characters and all 414 distinct code points, with no
sampling — so that the result would be read out of a full inventory rather than
recovered by a targeted search for the code points we expected.

| code point / block | occurrences |
| --- | --- |
| U+0305 combining overline | 2 |
| U+0304 combining macron | 0 |
| Number Forms U+2150–U+218F (incl. U+2183 Ↄ, reversed C) | 0 |
| Ancient Symbols U+10190–U+101CF (Roman denarius, sextans, uncia) | 0 |

**The multiplicative vinculum has no attestations whatsoever.** Unicode provides
dedicated code points both for the reversed C and for the Roman monetary signs,
and EDCS uses none of them. The two surviving U+0305 instances are reproduced in
Appendix A; neither is a multiplicative vinculum — one is an abbreviation
overline over a letter, the other sits on a numeral used as a word prefix.

This is not a limitation of the encoding pipeline. 208 instances of
U+0323, the Leiden underdot marking an uncertain letter, pass through the same
field. Combining marks are transmitted; overlines are absent by transcription
convention. The honest qualifier is that at 208 instances no Leiden
diacritic is *systematically* preserved either: the plain-text field is, in
practice, diacritic-free.

**What replaced it.** The negative would be of limited interest if nothing had
taken the vinculum's place. Something has. EDCS renders the overline as a
supplied word inside parentheses. The construction `N(milia)` occurs
1,875 times, and **65.4% of those occurrences are immediately
preceded by `HS`**, the sestertius sign, with small multiplier numerals: `HS
X(milia)` is ten thousand sesterces. The device generalises beyond currency to
the weights-and-measures system — *librae*, *sextarii*, *modii*, *iugera* — and
to ordinals, as in `p(ro) p(arte) IIII(quarta)`, "for a fourth part".

The consequence for dataset construction is direct. **2,338** such pairs sit
in the extracted set as strings of the form `X → Xmilia`, which is not a Latin
word and was never carved on a stone. They are not abbreviation expansions at
all: the supplied word was never present, because on the stone it was a line.
We class them separately and exclude them from the task, and the empirical
grounding for that decision is this measurement rather than a definitional
stipulation.

### 4.2 One character for at least eight signs

Epigraphic texts carry signs that no keyboard represents: the reversed C for
*mulieris*, the centurial sign, monetary and milliary marks, the theta nigrum
denoting a deceased person, and the fractional weight system inherited from the
*as*. EDCS renders all of them as a single ASCII `|`.

We enumerate every distinct `|(...)` form in the corpus: **376
forms, 16,194 occurrences**. Grouping them by the sign they plausibly
stand for — a rule-based pass over the surface strings, requiring philological
confirmation, though the enumeration itself does not depend on it — yields at
least eight families spanning the centurial sign, the reversed C, monetary and
milliary signs, fractions and weights, the *obitus* mark, Greek measures and
Christian symbols.

The practical consequence is that expanding `|` is **not a lookup**. It is the
same context-disambiguation problem as expanding `V`, conducted over a symbol
vocabulary, and a resource that published a `|` → word mapping would be wrong
for every form outside the majority family.

Three further properties are worth recording. Plurality is marked by
**repetition** — `||(mulierum)`, `||(centuriones)`, `||||(milia)` — which is the
same principle as the letter geminatio of Section 4.3, applied to glyphs; the
two should be treated as one phenomenon. The inflectional ambiguity of Latin
recurs inside the symbol set, with ten inflections of *centurio* behind one
glyph. And the inventory carries **its own unresolvable class**, `|()`, where
the sign is on the stone and the editor declined to supply a word.

### 4.3 Geminatio doubles the final letter

Latin epigraphy marks certain plurals by doubling a letter of the abbreviation:
`Aug → Augg(ustorum)`, `Imp → Impp(eratoribus)`, `Cos → Coss(ulibus)`,
`Caes → Caess(aribus)`, `Nob → Nobb(ilissimis)`. Naively concatenating
abbreviation and parenthesis content yields *Auggustorum*, which is not a word.

The doubling falls on the abbreviation's **final** letter. This is easy to
mistake, because for a one-letter abbreviation the doubled letter is both first
and last — `D → DD(ominis)`, `N → NN(ostris)` — so a rule keyed on a *leading*
run reproduces every headline example correctly while missing the entire
AUGG/IMPP/CONSS/CAESS/NOBB family. We made exactly this error. Our first
implementation found 5,293 cases; the trailing-run rule finds **8,986**, a 41%
undercount, and the corrected forms then agree with an independently derived
prior diagnosis to within 1% on each of its ten most frequent forms.

### 4.4 Line breaks fragment words

EDCS marks line breaks with `/` and does not rejoin words divided across them.
A word split as `v/` at one line's end and `ix(it)` at the next produces, under
token-level extraction, the pair `ix → ixit`. We identify **9,506** such
rows.

Their significance exceeds their number, because they poison downstream
heuristics rather than merely adding noise. The forty-eight spurious `ixit`
expansions produced this way were sufficient to make an attestation-based rule
misclassify the genuine abbreviation `V(ixit)` as an artifact — a rule which was
otherwise sound. We flag rather than remove them, since whether a given fragment
is genuinely broken requires reading the Latin.

---

## 5 Dataset construction

### 5.1 Extraction

Extraction operates on whitespace tokens rather than on individual parenthesis
groups, because an interior expansion such as `co(n)s(ul)` is one abbreviation
carrying two insertions and yields the single pair `cos → consul`. Square,
angle and curly brackets are masked as spans, since text inside them is
editorially restored rather than carved; `/` is treated as a hard token
boundary, so that no abbreviation is read across a line break.

The accounting closes exactly: **1,767,028** whitespace tokens contain an
opening parenthesis, of which **1,424,314** become pairs and **342,714** are
excluded for ten reasons, with zero residual. At the record level, 380,282
records contain a parenthesis and 337,744 contribute at least one pair.

Because re-running an extractor reproduces its bugs as faithfully as its
successes, we verified the primary extractor against a **second implementation
written independently from the editorial convention rather than from the first
implementation's code**. The two agree on **1,424,238** pairs, 99.995% of
the primary output, and all disagreements are accounted for: the second
implementation's shortfalls trace to normalisation it lacks, and its surplus
consists of bracket-contaminated fragments the primary correctly rejects. We
note the limit of this check: a misreading of the editorial convention shared by
both implementations would be invisible to it.

### 5.2 Corrections

Corrections are applied as **added columns**, never as edits. The original
abbreviation and expansion are carried through unmodified, exclusions are flags
rather than deletions, and anything the explicit rules cannot reach is marked
unresolved rather than imputed. The result is that every decision is reversible
and auditable, and a consumer who disagrees with one of our judgements can undo
it without re-running extraction.

Geminatio is corrected by reducing the trailing doubled run to a single letter
in abbreviation and expansion together, arbitrated against a control lexicon
built only from tokens carrying no doubled run, so the test cannot feed on the
forms it judges. 8,986 pairs are corrected; 3,350 are left
unresolved because neither reading is attested.

Numeral cases are decided by three **explicit, printed word lists** rather than
by a threshold:

| class | pairs | treatment |
| --- | --- | --- |
| Type 1 — the numeral stands for the word (`XL(quadragesimae)`) | 338 | expansion is the bracket content alone |
| Type 2 — numeral plus supplied unit (`X(milia)`) | 2,571 | numeral and unit stored separately; flagged out of the task |
| Type 3 — numeral as word prefix (`VI(vir)`) | 508 | gold label is the EDCS surface form |

For Type 3 the gold label is the **surface form** `VIvir`, never the Latin
reading *sevir*, which is confined to a normalisation column. A gold label must
be verifiable against the source, and should not rest on a contested scholarly
reading; the same office is written both `VI(vir)` and `IIIIII(vir)`, and we
keep those distinct in the label and merge them only in normalisation.

We deliberately did **not** discard records dated after 700 AD. Inspecting all
records dated after 1000 AD individually, 19 of 21 prove to
be correctly dated — genuine early-medieval Christian epitaphs, a self-dated
fifteenth-century Latin inscription, an eighteenth-century Spanish one. Exactly
two are demonstrably mis-keyed, and each is flagged with its evidence rather
than removed. A threshold rule would have discarded nineteen genuine records.

### 5.3 Splits

The primary split is 80/10/10 (1,140,108 / 139,591 / 139,580), grouped by
monument. Assignment uses a stable hash of the monument identifier rather than a
shuffle, so it depends on nothing but the seed and the identifier. **Zero of
319,514 monument groups straddle** the division.

Three provinces — Britannia, Mauretania Caesariensis, Pannonia inferior — are withheld entirely for a second split,
chosen one per genre regime as separated by the dominant reading of `V`, across
three geographically separated zones, each with a same-regime sister province
left in training so that the test measures transfer rather than absence.

Alongside the primary test set we release sets targeting specific difficulties:
a lexical set, restricted to keys whose expansions differ as words; a
discriminating variant of it, additionally requiring the majority baseline to be
wrong; a rare-form set, for keys seen fewer than ten times in training; a set of
forms unseen in training entirely; and a de-duplicated variant discussed below.

**Monument grouping does not prevent all leakage, and we report the residue.**
18,381 test rows (13.17%) are byte-identical to a training row,
context included, because different monuments carry identical formulae and the
context window is finite. No identity-based grouping rule can catch this, since
the monuments genuinely differ. Whether it constitutes leakage or is simply the
domain — an epigrapher reading `D(is) M(anibus)` for the thousandth time is also
performing lookup — is a question we do not resolve. We therefore publish both
the full test set and the de-duplicated variant, and report both.

### 5.4 Ceilings

Our models operate by **candidate ranking**: for a given abbreviation the
candidates are the expansions observed for it in training. This keeps a lookup
baseline and a transformer comparable, since all score the same set, but it
imposes ceilings that must be reported alongside any accuracy:

| test set | ceiling |
| --- | --- |
| primary | 0.9529 |
| rare form | 0.4812 |
| unseen form | **0.0000** |

The unseen-form set is **unanswerable by construction**: every key in it is
absent from training, so no candidate exists. Reporting accuracy on these sets
without their ceilings would misrepresent every model evaluated on them.

---

## 6 Artifacts and bias

### 6.1 The exclusion filter reshapes the corpus

342,714 tokens are excluded. A filter that removes a random slice of a
corpus costs only volume; one that removes a stratum changes what the dataset
says while looking like hygiene. Pooled across all ten exclusion reasons, the
dropped material differs from the retained material by **province TVD
0.131** against a bootstrap null p95 of 0.012
(**11.1×**) and **century TVD 0.097** against 0.006
(**16.3×**). Dropped texts average 504 characters against
280 for retained ones.

The filter preferentially removes long, damaged, Greek-East and fragmentary
material, and preferentially retains short, well-preserved, western,
early-imperial material. **The exclusion is reshaping the dataset, not filtering
it**, and any study of regional or diachronic variation conducted on the
retained material inherits that reshaping.

We record one methodological trap encountered here, since it is easy to fall
into. Total variation distance is biased upward by sampling noise, severely so
at small n: the null p95 in our data is 0.012 at n = 253,256 but 0.586 at n =
19. An absolute threshold applied uniformly declares small categories
distributionally skewed when noise alone produces the observed value. Bias must
be assessed against a null computed at each category's own sample size.

A second trap concerns ordering. Filter chains that stop at the first matching
reason report **first-match** counts rather than category membership, so a
category tested late is systematically undercounted — in our case by 22% and
50% for two categories. Most of the apparent effect is mechanical and should not
be reported as a finding, but the two genuine undercounts would otherwise
propagate silently.

### 6.2 Circularity, measured

Since the labels are editorial, it is worth quantifying how editorial they are.
For every token excluded as bracket markup we compute the share of the
**abbreviation's own letters** that fall inside an editorial restoration.
**63.25%** of such tokens have a wholly editorial abbreviation — the
editor inferred the letters *and* the expansion of the letters they inferred —
and only **1.35%** are fully attested. Any recovery of this material
into a training set would import that circularity, and it must never enter a
test set.

### 6.3 Editorial label noise

Holding context byte-identical on both sides, **13,706 rows
(0.96%)** carry an expansion that differs from another row presenting
the same evidence. The disagreements mix apparent keying errors with genuine
orthographic variation, and the two cannot be separated mechanically. This is a
**floor**: the rule catches only disagreements where the surrounding context
matches exactly.

Separately, **66.4% of distinct expansion forms also occur in the
corpus as uncontracted plain text** — *vixit* appears 31,388 times as an
expansion and 25,244 times carved in full. This is variation between stones
rather than editorial inconsistency, and we resist the stronger reading. What it
does establish is the task's real definition. The dataset consists entirely of
cases where an editor judged an abbreviation to be present, so the task is
properly stated as *given that an editor marked this abbreviated, what did they
expand it to* — a narrower and more edition-dependent question than "expand
Latin epigraphic abbreviations", and one that should be stated as such.

---

## 7 Experimental setup

We evaluate three baselines under three context conditions. All operate by
candidate ranking, so the context delta is measured identically across model
classes.

- **M1**, a most-frequent-expansion lookup (57,054 parameters). The
  floor, and trivially reproducible.
- **M2**, a logistic-regression candidate re-ranker (1,048,580 parameters)
  over character n-grams of the abbreviation, context words on both sides, and
  conditioning counts, all conjoined with the candidate — a feature that does
  not mention the candidate cannot discriminate between candidates of the same
  row.
- **M3**, a fine-tuned Latin encoder, `bowphs/LaBerta` (137,180,135
  parameters), with a mean-pooled classification head scored at inference
  against the same candidate set.

The conditions are **C1**, local text only; **C2**, adding province; and **C3**,
adding province and century. For M3 the conditions are injected as text
prefixes, so the only thing that varies is what the model is told.

Two deviations from the intended protocol are recorded. M3 was trained on
**100,000** rows — 8.8% of the training split — at a single seed,
for compute reasons; its seed variance is therefore unmeasured and its accuracy
is a lower bound. And a pretrained encoder considered first was rejected because
loading it required executing code downloaded from the model repository.

---

## 8 Results

### 8.1 The context conditions

| model | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1 lookup (cannot read text) | 0.4582 | 0.5247 | 0.5414 | **+0.0833** |
| M2 linear re-ranker | 0.7376 | 0.7347 | 0.7305 | **-0.0070** |
| M3 fine-tuned LaBerta | 0.7575 | 0.7513 | 0.7505 | **-0.0070** |

**The hypothesis fails.** Province and century are worth +0.0833 to a model
that cannot read the surrounding text and nothing to either model that can. M1's
gain is large and far exceeds its seed variance; both text-reading models show a
small negative delta instead.

The interpretation is that province is a **proxy for textual context rather than
independent information**. A model with access only to the abbreviation and its
metadata benefits substantially from being told the province, because province
correlates with genre and genre determines the reading. A model that can see the
adjacent words already has that information more directly, and the province
feature then contributes little while costing capacity.

### 8.2 The association is real; the transfer is not

It would be wrong to conclude that the province signal does not exist. It does,
and strongly. Normalised mutual information between province and expansion
choice for `V` is 0.3709, some 12.2× a permutation null computed at
the level of whole inscriptions rather than pairs — the conservative choice,
since pairs from one stone are not independent. The association survives both
cleaning and the restoration of the excluded strata described in Section 6.1.
The underlying distributions are unambiguous: in Numidia `V` is *vixit* in 93%
of cases, while in Pannonia superior it is *votum* in 67%.

What the signal does not do is transfer. Retraining without the three withheld
provinces and testing on exactly those provinces:

| | C1 | C2 | C3 | C1→C3 |
| --- | --- | --- | --- | --- |
| M1, withheld provinces | 0.3665 | 0.3665 | 0.3665 | **+0.0000** |

**Exactly zero, to four decimal places, across all seeds.** The mechanism is
plain once stated: M1's province conditioning is a lookup table keyed on
province, and for a province absent from training the table is empty, so the
conditioned model backs off to the unconditioned one. The gain observed on the
primary test set is **memorisation of province-specific distributions**, not a
transferable generalisation about Latin.

This distinction is invisible without a held-out province split. Evaluated
pooled, province conditioning looks like knowledge. We note that our own first
version of this experiment trained on a split containing the withheld provinces
and reported the opposite conclusion; the error was detectable only because
held-out accuracy came out implausibly *above* primary-test accuracy.

### 8.3 Cost

| model | parameters | training time | training rows | accuracy (C1) |
| --- | --- | --- | --- | --- |
| M1 | 57,054 | 0 s | 1,140,108 | 0.4582 |
| M2 | 1,048,580 | 72 s | 1,140,108 | 0.7376 |
| M3 | 137,180,135 | 1078 s | 100,000 | 0.7575 |

M2 attains **97.4%** of M3's accuracy at a small fraction of the
parameters and training time. The comparison should be read with its caveat
stated plainly: M3 was trained on 8.8% of the data available to M2, so
the gap understates what a fully trained encoder would achieve, and the
comparison favours the cheaper model.

---

## 9 Limitations

1. **Expansions are editorial interpretations, not attested text.** A model
   trained here demonstrably learns EDCS conventions; that it learns Roman
   practice does not follow and is not shown.
2. **The task is conditioned on the editor having judged an abbreviation
   present** (66.4% of expansion forms also occur uncontracted).
3. **Editorial disagreement is ≥0.96%**, and that figure is a floor.
4. **The exclusion filter is bias-inducing** (11.1× and
   16.3× a null in province and century).
5. **EDCS assigns a resolvable single-century date to only 36.0% of
   records**; every diachronic result rests on that third.
6. **Survival and excavation bias** are inherited from the epigraphic record
   and uncorrected here.
7. **13.17% of the primary test split is byte-identical to a training
   row.** We publish a de-duplicated variant, but do not resolve whether the
   duplication is contamination or the domain.
8. **The held-out province split confounds province with century**: the
   withheld provinces are late-Romanised frontier territory, so results on it
   must be stratified by century rather than pooled.
9. **Several judgements require a Latinist and have not received one**: the
   numeral word lists, the `|` sign families, which near-duplicate forms are
   keying errors rather than ancient variants, whether the inflectional/lexical
   distinction is drawn correctly, and whether `X(milia)` renders a vinculum.
10. **M3 used 100,000 training rows at one seed**, so its result is a lower
    bound and its variance is unmeasured. The agreement between M2's and M3's
    deltas at four decimal places should be read as coincidence at that
    precision; the defensible claim is that both text-reading models show a
    small negative delta.
11. **Redistribution permission from EDCS is unresolved and blocks release** of
    the derived tables. Code, reports and measured statistics are unaffected.

---

## 10 Conclusion

We set out to build a benchmark for Latin abbreviation expansion and to test
whether abbreviation meaning tracks province and era. The benchmark exists:
1,424,314 labelled pairs over 588,509 records, with frozen monument-grouped
splits, difficulty-targeted test sets, and the ceilings its construction
imposes. The hypothesis failed, in an instructive way — province predicts
meaning in the data, with an association many times a conservative null, yet
transfers not at all to an unseen province and adds nothing to a model that can
read the surrounding Latin.

The more durable contribution is the audit that preparing the dataset required.
EDCS does not preserve the numeral vinculum, substituting a supplied word;
collapses at least eight distinct epigraphic signs onto a single ASCII
character; marks plurality on an abbreviation's final letter rather than its
first; fragments words across line breaks into spurious pairs; and is filtered,
by any conventional extraction, in a way that reshapes the corpus along
precisely the geographic and chronological axes one might wish to study. Each of
these is invisible to a consumer who mines the parentheses and begins training,
and each has a measured magnitude.

For the field, we would draw one methodological conclusion beyond the specifics.
A derived dataset inherits the conventions of its source edition, and those
conventions are not neutral with respect to the questions one asks of the data.
Auditing them is not preliminary work to be done quickly before the modelling
starts. In this project it was the modelling that proved quick, and the audit
that produced the findings.

---

## References

Assael, Y., Sommerschield, T., Shillingford, B., Bordbar, M., Pavlopoulos, J.,
Chatzipanagiotou, M., Androutsopoulos, I., Prag, J., & de Freitas, N. (2022).
Restoring and attributing ancient texts using deep neural networks. *Nature*,
603(7900), 280–283. https://doi.org/10.1038/s41586-022-04448-z

Assael, Y., Sommerschield, T., Cooley, A., Shillingford, B., Pavlopoulos, J.,
Suresh, P., Herms, B., Grayston, J., Maynard, B., Dietrich, N., Wulgaert, R.,
Prag, J., Mullen, A., & Mohamed, S. (2025). Contextualizing ancient texts with
generative neural networks. *Nature*, 645(8079), 141–147.
https://doi.org/10.1038/s41586-025-09292-5

Cui, W., & Ströbel, P. B. (2026). Across Generations: A Comparative Analysis of
NER for Latin Inscriptions from Classical Machine Learning to LLMs. In
*Proceedings of the Fourth Workshop on Language Technologies for Historical and
Ancient Languages (LT4HALA 2026)* (pp. 112–124). ELRA.
https://doi.org/10.63317/2g99sovd35pj

Elliott, T. (1998). *Abbreviations in Latin Inscriptions*. American Society of
Greek and Latin Epigraphy.
https://www.asgle.org/epigraphical-resources/abbreviations-in-latin-inscriptions/

Heřmánková, P., Ballsun-Stanton, B., & Laurence, R. (2024). FAIR Turn in
Epigraphy: Low Barrier Pathways to Quantitative and Reproducible Research in
Latin Epigraphy. In *Computational Humanities Research 2024* (CEUR Workshop
Proceedings, Vol. 3834, pp. 649–661). CEUR-WS.org.
https://ceur-ws.org/Vol-3834/paper4.pdf

Heřmánková, P., Kaše, V., & Sobotková, A. (2021). Inscriptions as data: digital
epigraphy in macro-historical perspective. *Journal of Digital History*, 1(1),
99–141. https://doi.org/10.1515/jdh-2021-1004

Kaše, V., Heřmánková, P., & Sobotková, A. (2021). Classifying Latin Inscriptions
of the Roman Empire: A Machine-Learning Approach. In *Proceedings of the
Conference on Computational Humanities Research 2021* (CEUR Workshop
Proceedings, Vol. 2989, pp. 123–135). CEUR-WS.org.
https://ceur-ws.org/Vol-2989/short_paper12.pdf

Locaputo, A., Portelli, B., Colombi, E., & Serra, G. (2023). Filling the Lacunae
in ancient Latin inscriptions. In *Information and Research Science Connecting
to Digital and Library Science 2023* (CEUR Workshop Proceedings, Vol. 3365, pp.
68–76). CEUR-WS.org. https://ceur-ws.org/Vol-3365/short5.pdf

---

## Appendix A — the surviving combining marks

| record | mark | context |
| --- | --- | --- |
| `EDCS-00000939-0` | U+0305 on `q` | `Augustal(i) Cumis, q̅(uaestori)` |
| `EDCS-05802229-0` | U+0305 on `I` | `Messal(l)ae II[I̅viro(?)` |
| `EDCS-25500308-0` | U+0332 on `τ` | `Διονύϲιοϲ / οπτο τ̲` |

None is a multiplicative vinculum: the first is the abbreviation overline on a letter, the second a numeral-prefix compound, the third Greek.

## Appendix B — reproduction

Every numeric value in this paper is injected from `results/all_results.json` by
a build script; none is typed by hand, and any unresolved value is emitted as a
visible marker rather than silently omitted. The full pipeline is reproduced by
a single command, which verifies the corpus SHA-256 (`9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317`) before
running and refuses to proceed against a different snapshot. Decisions taken
during construction are logged with their evidence, the alternatives rejected,
and what would overturn them, in 41 entries.

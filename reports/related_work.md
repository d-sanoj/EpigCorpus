# Related work — verified

**Verification status.** Network access was available at the time of writing
(2026-08-21). Every citation below was **retrieved**, not recalled: journal
articles through the Crossref API by DOI, proceedings through the publisher's
own volume index, and the Elliott index from the ASGLE site itself. Where a
detail could not be retrieved it is marked, not filled in.

Superseded file: the previous version of this report carried no DOIs and said
so. It is replaced entirely.

---

## The one-sentence differentiator (mandatory)

> **LatEpig is a *retrieval* tool — it reproducibly executes a search against
> EDCS and exports the matching records; EpigCorpus is a *derived labelled
> dataset and benchmark* — it mines EDCS's editorial parentheses as
> ground-truth labels for abbreviation expansion, and audits the transcription
> conventions that silently distort any such derivation.**

One retrieves what EDCS holds. The other measures what EDCS's conventions do to
anything built from it.

---

## Anchor works

### 1. Assael et al. 2025 — *Aeneas* — **primary contrast**

> Assael, Y., Sommerschield, T., Cooley, A., Shillingford, B., Pavlopoulos, J.,
> Suresh, P., Herms, B., Grayston, J., Maynard, B., Dietrich, N., Wulgaert, R.,
> Prag, J., Mullen, A., & Mohamed, S. (2025). Contextualizing ancient texts
> with generative neural networks. *Nature*, 645(8079), 141–147.
> **DOI: [10.1038/s41586-025-09292-5](https://doi.org/10.1038/s41586-025-09292-5)**

*What it does.* A generative model that contextualises Greek and Latin
inscriptions: restores arbitrary-length lost text, attributes geography and
date, and retrieves parallels.

*How this work differs.* Aeneas restores characters **lost from the stone** —
the label is a lacuna. EpigCorpus expands abbreviations the stone
**deliberately carries** — the label is an editorial parenthesis. Different
task, different label source, and crucially different failure mode: a lacuna
has one true answer that erosion destroyed, whereas an abbreviation has an
answer the editor inferred and may have inferred inconsistently (we measure
that at 0.96%).

*Why the difference matters.* The two tasks have opposite circularity
profiles. Restoration can be validated against a rediscovered fragment.
Abbreviation expansion cannot: there is no independent record of what the
carver meant.

### 2. Assael et al. 2022 — *Ithaca* — Greek precedent

> Assael, Y., Sommerschield, T., Shillingford, B., Bordbar, M., Pavlopoulos, J.,
> Chatzipanagiotou, M., Androutsopoulos, I., Prag, J., & de Freitas, N. (2022).
> Restoring and attributing ancient texts using deep neural networks.
> *Nature*, 603(7900), 280–283.
> **DOI: [10.1038/s41586-022-04448-z](https://doi.org/10.1038/s41586-022-04448-z)**

*What it does.* Textual restoration plus geographical and chronological
attribution for ancient Greek; historians assisted by Ithaca improved
restoration accuracy from 25% to 72%.

*How this work differs.* Greek, and again restoration rather than expansion.
Its attribution task is the closest precedent for our C2/C3 conditions — but
Ithaca *predicts* place and date, whereas we *supply* them and ask whether
they help. Our answer is that they do not, once a model can read the text.

### 3. Kaše, Heřmánková & Sobotková 2021 — prior ML on EDCS

> Kaše, V., Heřmánková, P., & Sobotková, A. (2021). Classifying Latin
> Inscriptions of the Roman Empire: A Machine-Learning Approach. In
> *Proceedings of the Conference on Computational Humanities Research 2021*
> (CEUR Workshop Proceedings, Vol. 2989, pp. 123–135). CEUR-WS.org.
> ISSN 1613-0073. URN: urn:nbn:de:0074-2989-2.
> **URL: [https://ceur-ws.org/Vol-2989/short_paper12.pdf](https://ceur-ws.org/Vol-2989/short_paper12.pdf)**

*What it does.* Reconciles the incompatible inscription-type taxonomies of
EDCS and EDH by learning a classifier that maps between them.

*How this work differs.* They classify **whole inscriptions** by type; we label
**tokens within** an inscription. Their labels are curatorial metadata; ours are
editorial markup inside the text itself.

*Why the difference matters.* Their work establishes EDCS as a tractable ML
source and demonstrates that its **categories are not neutral**. Our exclusion
audit is the same lesson at the level of transcription rather than taxonomy.

**[NEEDS VERIFICATION]** Whether they split by `record_id` or by monument, and
how the 45,655 multi-segment records were handled. Not stated in the abstract
and not retrieved from the full text; we do not attribute a position to them.

### 4. Heřmánková, Kaše & Sobotková 2021 — LIRE, inscriptions as data

> Heřmánková, P., Kaše, V., & Sobotková, A. (2021). Inscriptions as data:
> digital epigraphy in macro-historical perspective. *Journal of Digital
> History*, 1(1), 99–141.
> **DOI: [10.1515/jdh-2021-1004](https://doi.org/10.1515/jdh-2021-1004)**

*What it does.* Argues for treating epigraphic editions as datasets, and
introduces LIRE, an aggregate of EDH and EDCS restricted to inscriptions that
are geolocated, inside the Empire, and dated within 50 BC–350 AD — 182,852
records.

*How this work differs.* LIRE **filters for completeness**; we **audit the
filtering**. Their restriction to dated, geolocated records is exactly the kind
of selection our pooled-bias analysis quantifies: our filters reshape province
and century distributions at 10.9× and 16× a bootstrap null.

*Why the difference matters.* This is the framing paper for our whole
argument, and also its foil: an aggregate built for macro-history inherits
conventions that a token-level ML task cannot ignore.

### 5. Heřmánková, Ballsun-Stanton & Laurence 2024 — LatEpig

> Heřmánková, P., Ballsun-Stanton, B., & Laurence, R. (2024). FAIR Turn in
> Epigraphy: Low Barrier Pathways to Quantitative and Reproducible Research in
> Latin Epigraphy. In *Computational Humanities Research 2024* (CEUR Workshop
> Proceedings, Vol. 3834, pp. 649–661). CEUR-WS.org. ISSN 1613-0073.
> URN: urn:nbn:de:0074-3834-7.
> **URL: [https://ceur-ws.org/Vol-3834/paper4.pdf](https://ceur-ws.org/Vol-3834/paper4.pdf)**

> **Correction to the project brief.** The brief cites this as
> "Ballsun-Stanton, Heřmánková & Laurence 2024". The published author order is
> **Heřmánková, Ballsun-Stanton, Laurence**. Corrected here.

*What it does.* LatEpig v2.0 programmatically executes an EDCS search and
exports results as TSV or JSON, with mapping to Roman provinces, roads and
cities. A FAIR-practice contribution: reproducible retrieval.

*How this work differs.* See the one-sentence differentiator above. LatEpig
makes *retrieval* reproducible and stops there — deliberately, because that is
its purpose. It does not extract labels, does not model anything, and does not
examine what EDCS's transcription conventions do to derived data. EpigCorpus
begins where LatEpig ends.

*Why the difference matters.* A user who retrieves reproducibly with LatEpig
and then mines the parentheses inherits every artifact we document — the
unpreserved vinculum, the eight-way `|` ambiguity, trailing-letter geminatio,
line-break fragmentation. Reproducible retrieval of a distorting convention
reproduces the distortion.

### 6. Elliott 1998 — the standing abbreviation index

> Elliott, T. (1998). *Abbreviations in Latin Inscriptions*. American Society
> of Greek and Latin Epigraphy (ASGLE).
> **URL: [https://www.asgle.org/epigraphical-resources/abbreviations-in-latin-inscriptions/](https://www.asgle.org/epigraphical-resources/abbreviations-in-latin-inscriptions/)**
> Compiled from digital texts of all Latin inscriptions published in
> *L'Année Épigraphique*, 1888–1993. "Common" lists use a threshold of more
> than 10 occurrences. No version number or last-updated date is given on the
> resource. Copyright 1998 by Tom Elliott.

*What it does.* A static alphabetical index of abbreviations and their
expansions, in two tiers (common and complete).

*How this work differs.* Elliott is a **lookup list**; EpigCorpus is a
**frequency- and context-conditioned resource with a benchmark attached**. His
source is *L'Année Épigraphique* to 1993; ours is EDCS at 588,509 records. He
gives the expansions an abbreviation *can* take; we give the distribution over
expansions it *does* take, per province and per century, with the ambiguity
measured.

*Why the difference matters.* This is the gap the project fills. But our
results also qualify the ambition: 54.5% of ambiguity among evaluable keys is
lexical rather than inflectional, so a list cannot resolve it and neither can
a model that ignores context — while conditioning on province turns out to be
memorisation, not knowledge.

---

## Work published since, retrieved this session

### Cui & Ströbel 2026 — NER on EDCS

> Cui, W., & Ströbel, P. B. (2026). Across Generations: A Comparative Analysis
> of NER for Latin Inscriptions from Classical Machine Learning to LLMs. In
> *Proceedings of the Fourth Workshop on Language Technologies for Historical
> and Ancient Languages (LT4HALA 2026)* @ LREC 2026, pp. 112–124. ELRA.
> **DOI: [10.63317/2g99sovd35pj](https://doi.org/10.63317/2g99sovd35pj)**
> ACL Anthology: 2026.lt4hala-1.11

*Relevance.* The closest contemporary EDCS derivative: a manually annotated
set of **1,000 EDCS inscriptions** with a fine-grained BIO scheme for Roman
personal names; fine-tuned BERT reaches weighted F1 91.1 / macro F1 68.7.

*How this work differs.* NER, not abbreviation expansion — **the paper does not
address expansion at all**. Also 1,000 hand-annotated inscriptions against our
588,509 records with 1,424,314 automatically derived labels: manual precision
versus derived scale. Their macro-F1 of 68.7 against weighted 91.1 shows the
same head/tail problem our frequency bands quantify.

### Locaputo, Portelli, Colombi & Serra 2023 — filling lacunae

> Locaputo, A., Portelli, B., Colombi, E., & Serra, G. (2023). Filling the
> Lacunae in ancient Latin inscriptions. In *Information and Research Science
> Connecting to Digital and Library Science 2023* (CEUR Workshop Proceedings,
> Vol. 3365, pp. 68–76). CEUR-WS.org. ISSN 1613-0073.
> URN: urn:nbn:de:0074-3365-4.
> **URL: [https://ceur-ws.org/Vol-3365/short5.pdf](https://ceur-ws.org/Vol-3365/short5.pdf)**

*Relevance.* Deep-learning restoration of lacunae in Latin inscriptions —
the Latin analogue of Ithaca/Aeneas.

*How this work differs.* Again restoration, not expansion. Confirms the
pattern across the whole field: **the epigraphic NLP literature has converged
on restoring what is missing, and has not treated expanding what is present as
a task in its own right.** That is the gap this dataset occupies.

---

## What the search did not find

No dataset or benchmark for **Latin epigraphic abbreviation expansion** was
retrieved. The nearest neighbours are abbreviation expansion in *other*
domains — clinical text, scientific documents, medieval court hand — none
epigraphic, none Latin-inscriptional, none using editorial parentheses as
labels.

**Stated as a bounded negative, not a priority claim:** this is the outcome of
the searches run in this session, not an exhaustive survey. A claim of novelty
in the paper should be phrased as "we are not aware of" rather than "there is
no". **[VERIFY — a systematic literature search before submission]**

---

## Citations still outstanding

- **[NEEDS CITATION]** EDCS's own documentation of its transcription
  conventions — the round-parenthesis expansion rule, the `|` sign convention,
  and the `-N` record-id suffix. All three are inferred from the data's
  structure (D-0004, D-0013, D-0017). A published statement from EDCS would
  settle them directly and is the single most valuable missing source.
- **[NEEDS VERIFICATION]** Whether Kaše et al. 2021 split by record or by
  monument.

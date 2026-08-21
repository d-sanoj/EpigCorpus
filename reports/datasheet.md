# Datasheet for EpigCorpus

Following the datasheet structure of Gebru et al. Every figure in this
document is drawn from `results/all_results.json`, which is generated from the
committed artifacts by `scripts/build_all_results.py`.

**Status: NOT RELEASED.** Redistribution permission from EDCS has not been
sought or granted — see `reports/edcs_permission_request.md`. This datasheet
describes a dataset that exists locally and is blocked from release.

---

## Motivation

**For what purpose was the dataset created?**
To provide labelled training and evaluation data for the task *expand Latin
epigraphic abbreviations*. EDCS marks editorial expansions of ancient
abbreviations in round parentheses — the stone reads `D M`, the edition prints
`D(is) M(anibus)` — so the parentheses are free ground-truth labels for a task
that had no dataset. A secondary and, as it turned out, larger purpose: to
document the transcription conventions that distort any dataset derived this
way.

**Who created it and who funded it?**
Created in this project from EDCS. **[VERIFY — funding statement, if any]**

**Any other comments?**
The project began as a benchmark and became, in substantial part, an audit. The
benchmark's headline experiment returned a negative result; the audit returned
six novel findings about the source database.

---

## Composition

**What do the instances represent?**
Each instance is one (abbreviation, expansion) pair extracted from one token of
one inscription, with its surrounding context, province, and date range.

**How many instances are there?**

| | |
| --- | --- |
| source records | 588,509 |
| distinct monuments (record id minus segment suffix) | 542,854 |
| extracted pairs | 1,424,314 |
| distinct abbreviation keys | 37,526 |
| distinct (abbreviation, expansion) types | 73,105 |
| pairs flagged out of the task (numeral ellipsis) | 2,571 |

**Note on the unit.** 588,509 records are **not** 588,509 inscriptions. The
`-0`/`-1`/`-2` suffix marks segments — faces or panels — of one monument;
45,655 records share a base id with another. 100% of multi-segment groups are
in the same province and 299 carry byte-identical text. Any split must group by
monument. (D-0004)

**Does each instance consist of raw data or features?**
Raw text plus derived columns. The original `abbrev` and `expansion` are
carried through unmodified; all corrections are additional columns, so every
correction is reversible and auditable.

**Is there a label?**
Yes — the expansion, taken from the editor's parenthesis. **This is the
dataset's central caveat: see Limitations.**

**Is any information missing?**

| | |
| --- | --- |
| pairs with a province | 99.7% |
| pairs resolving to a single century | 36.0% |
| rows left UNRESOLVED by correction rules | 3,350 geminatio + 74 numeral |

Dating coverage of ~35–36% is a property of EDCS, not of our processing, and
constrains every century-conditioned result.

**Are there errors, sources of noise, or redundancies?**
Yes, and they are measured rather than asserted:

| artifact | scale |
| --- | --- |
| editorial disagreement under identical context | 13,706 rows (0.96%) |
| line-break fragmentation (`v/` + `ix(it)` → `ix → ixit`) | 9,506 rows |
| geminatio (plural marked by doubling the final letter) | 8,986 rows, corrected |
| numeral-ellipsis pairs (`X(milia)`, not abbreviations) | 2,571 rows, flagged out |
| test rows byte-identical to a training row | 18,381 (13.17% of primary test) |
| demonstrably mis-keyed dates | 2 records |

**Is the dataset self-contained?**
No. It derives from EDCS, which is live and grows. The exact input is pinned by
sha256 `9ebea1a7…`; a re-harvest will not reproduce it.

**Does it contain data that might be offensive or sensitive?**
The content is Roman funerary, votive and administrative inscriptions. It names
long-dead individuals, including enslaved and freed people, whose status is
often recorded (`\|(mulieris) l(ibertus)` — "freedman of a woman"). No living
person is identifiable.

---

## Collection

**How was the data acquired?**
Harvested from the EDCS web interface with rate limiting and without
circumventing access control. **[ACTION — PERMISSION]** Redistribution rights
were not obtained at harvest time; this is unresolved and blocks release.

**What was the sampling strategy?**
None — the full database was harvested, not sampled.

**Over what timeframe was the data collected?**
Harvest completed before this project's Phase 0. The pinned snapshot hashes to
`9ebea1a7…`. **[VERIFY — exact harvest dates from `data/edcs_harvest_manifest.json`]**

**Were ethical review processes conducted?**
Not applicable to a corpus of ancient inscriptions. The relevant obligation is
to EDCS as compiler, and it is unmet pending the permission request.

---

## Preprocessing / cleaning / labelling

**Was any preprocessing done?**
Yes, in four documented stages with 41 logged decisions (`reports/decisions.md`).

1. **Extraction** — per whitespace token, not per parenthesis group, so
   `co(n)s(ul)` yields one pair. Verified against a second, independently
   written extractor: **99.995% agreement, zero unexplained disagreements**.
2. **Exclusion** — 342,714 tokens dropped for ten reasons. The accounting
   closes exactly: 1,424,314 + 342,714 = 1,767,028 paren-bearing tokens.
3. **Correction** — geminatio collapse, numeral classification by three
   explicit printed word lists, date flagging. Every correction reversible.
4. **Splitting** — grouped by monument, seeded, frozen.

**Was the raw data saved?**
Yes. `data/edcs_inscriptions.jsonl.gz` is committed and hashes identically to
the working file. Nothing is deleted anywhere in the pipeline; exclusions are
flags.

**Is the software available?**
Yes, all of it, in `scripts/`.

---

## Uses

**What tasks could the dataset be used for?**
Abbreviation expansion; abbreviation-form frequency study; the `|` symbol
inventory as a standalone resource (376 forms); studies of editorial practice.

**Is there anything that should NOT be done with it?**

- **Do not use it to measure whether province predicts meaning without a
  held-out province split.** Province conditioning yields +8.3 points on seen
  provinces and **exactly 0.0000** on unseen ones. Pooled, it looks like
  knowledge; split properly, it is memorisation.
- **Do not report accuracy without the candidate-ranking ceiling.**
  `test_unseen_form` has a ceiling of **0.0000** and `test_rare_form` of
  **0.4812**.
- **Do not treat the expansions as attested text.** They are editorial
  interpretations.
- **Do not use `test_rare_form` results to claim disambiguation ability** —
  rare forms are mostly unambiguous (ambiguity rate 0.103 against 0.918
  overall), so that set measures coverage.

---

## Distribution

**How will it be distributed?**
**Undecided and blocked.** Code and reports can be released now; the derived
tables cannot until EDCS responds.

**Under what licence?**
Code: MIT (as the repository already carries). Derived data: **unresolved**,
pending EDCS. The two must be licensed separately.

**Are there IP or ToS restrictions?**
Yes — this is the blocker. See `reports/edcs_permission_request.md`.

---

## Maintenance

**Who will maintain it?**
**[VERIFY — maintainer and contact]**

**Will the dataset be updated?**
EDCS grows continuously. Any update produces a different corpus hash and
invalidates every number in the paper, which is why the hash is pinned in
D-0001.

**How will others extend or contribute?**
The extraction and correction code is committed and deterministic. A consumer
who obtains their own EDCS harvest can rebuild the tables with one command.

---

## Limitations — stated plainly, as required

**1. Expansions are editorial interpretations, not attested text.**
The parenthesis records what an editor judged the abbreviation to mean. There
is no independent record of what the carver intended. **A model trained here
learns EDCS conventions, and cannot be shown to have learned Roman practice.**

**2. The task is conditioned on the editor having decided an abbreviation is
present.** 66.4% of expansion forms also appear as uncontracted plain text —
`vixit` 31,388 times as an expansion and 25,244 times carved in full. The task
only ever sees the abbreviated half, so it is properly stated as *"given that
an editor marked this abbreviated, what did they expand it to"*.

**3. Editorial disagreement is measurable at 0.96%** and that is a floor — it
counts only cases where 80 characters of context are byte-identical.

**4. The exclusion filter reshapes the corpus.** Pooled across all ten filters,
province TVD 0.131 against a null p95 of 0.012 (10.9×) and century TVD 0.097
against 0.006 (16×). Long, damaged, Greek-East and fragmentary material is
preferentially removed.

**5. Survival and excavation bias.** What survives is not what was carved, and
what is excavated is not what survives. EDCS inherits both. Nothing here
corrects for it.

**6. EDCS dates only ~36% of records.** Every century-conditioned result rests
on that third.

**7. 13.17% of the primary test split is byte-identical to a training row.**
Different monuments carry identical formulae; monument grouping cannot prevent
it. `test_no_context_duplicate` isolates the effect at **+1.9 points**.

**8. The held-out province split confounds province with century.** The three
withheld provinces are frontier territory, Romanised late: 1AD is 26.8% of the
primary test and 5.7% of the held-out test. Results on it must be stratified by
century.

**9. Several judgements need a Latinist and have not had one.**
The three numeral word lists; the `|` sign-family grouping; which of 56
near-duplicate `|` forms are keying errors versus ancient orthographic
variants; whether the inflectional/lexical prefix rule is sound; whether
`X(milia)` really renders a vinculum. Each is marked **[VERIFY — LATINIST]**
in the reports.

**10. M3 was trained on 9% of the training data** (100,000 of 1,140,108 rows)
and at one seed, for compute reasons. Its accuracy is therefore a lower bound,
and the cost comparison in F8 flatters the cheaper models.

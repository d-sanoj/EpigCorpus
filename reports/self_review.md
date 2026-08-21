# Self-review

Written against the six questions in the brief. A self-review with no
admissions is a failed self-review, so the errors are listed first and in
detail, including the ones a reader would never have found.

---

## What did I get wrong during this project, and how did I catch it?

**1. The geminatio rule — a 41% undercount, my own.**
I implemented plural-marking doubling as a run at the *start* of the
abbreviation. It is at the *end*: `Aug → Augg`, `Imp → Impp`, `Cos → Coss`. A
one-letter abbreviation doubles a letter that is both first and last (`D → DD`),
so the rule looks correct on every headline example and silently misses the
entire AUGG/IMPP/CONSS/CAESS/NOBB family. First implementation: 5,293
collapses. Correct: 8,986.
**Caught by** comparing against `dd_diagnostic.md` §4b, whose top forms include
`augg` at 1,793 — a number my rule could not produce. Pinned by a regression
test that asserts the leading-run rule fails on `Augg`.

**2. A numeral rule that swallowed the praenomina — 268,253 rows.**
An early gate classified any single-character Roman-numeral abbreviation whose
expansion began with it as a numeral candidate. That is `M(anibus)`, `D(is)`,
`C(aius)`, `L(uci)` — the entire praenomen vocabulary of Latin epigraphy.
Correct count with the explicit-list gate: 712.
**Caught by** the number itself being absurd. Had it flagged 3,000 rows instead
of 268,000 it would have shipped.

**3. I talked the user out of a validation sweep, and was wrong.**
Asked to choose between a 2-hour hyperparameter sweep and a 60-minute fixed
choice, I argued four configurations were "the appearance of rigor" and set
`alpha=1e-5` from reasoning. It was the wrong regime and cost **7 accuracy
points** (0.777 → 0.701). The sweep I skipped then showed validation accuracy
rising monotonically as alpha *falls*.
**Caught by** the rerun being worse. The saving I promised did not exist: it
cost a 35-minute rerun plus the 15-minute sweep anyway. The lesson is not
"always sweep" — it is that I reasoned about a regime I had no measurement of
and presented the conclusion as sound.

**4. The held-out province experiment was invalid for its entire first run.**
I trained every model on `primary_train` — which *contains* Britannia,
Mauretania Caesariensis and Pannonia inferior — and then evaluated on
`heldout_province_test`. That measures performance on three provinces the model
was trained on. Phase 4 had built `heldout_province_train` for exactly this and
Phase 5 never used it.
**Caught by** the result being implausible: held-out scored *higher* than the
primary test. I had already flagged that as "needs explaining in Phase 6"
rather than treating it as the symptom it was. The repair inverted the finding
completely — from "+16.8, province transfers beautifully" to "+0.0000, province
does not transfer at all."
**This is the most serious error in the project.** It would have put a false
headline result in the paper.

**5. A confounded measure I nearly published.**
My same-monument editorial-inconsistency probe returned 228,813 rows — 16% of
the dataset, and it would have been the largest number in the circularity
section. It is wrong: a long list-type inscription legitimately uses `C` for
*Caius*, *Caio* and *Cai* in different grammatical cases. The measure counts
Latin inflection as editorial disagreement.
**Caught by** reading the top example instead of the total. Withdrawn in
D-0040, logged rather than deleted so nobody rebuilds it.

**6. An overclaim I made and then withdrew.**
I labelled the finding that 66.4% of expansion forms also appear as plain text
"the abbreviation boundary is editorial". It is not: some stones carve `VIXIT`
and others carve `V`, and both transcriptions are correct. The narrower true
claim is that the task is conditioned on the editor having judged an
abbreviation present. D-0041.

**7. A prediction of mine that failed.**
In Phase 0 I recorded one U+0305 sighting and wrote that it "raises the prior
that Phase 2 will not be a pure negative." It is a pure negative — n=2 in 39.5
million characters. Withdrawn in D-0019.

**8. Operational errors that wasted the user's time.**
I misread `ps` ELAPSED format and told the user a job had been "stuck for 68
minutes" when it had run for 68 *seconds*. I proposed a 9-cell neural sweep
without costing it, then measured 64 rows/s and found it was a 44-hour job. I
saved aggregate metrics per model cell but not per-row predictions, so a
10-minute diagnostic needed a re-inference. I launched two training processes
that deadlocked on the same HuggingFace cache lock.

---

## Which numbers here would not survive an independent re-run?

**Would survive, and have been checked.** The extraction (byte-identical under
three `PYTHONHASHSEED` values and from a foreign working directory, and
99.995% reproduced by a separately written second extractor); the character
census; the `|` inventory; the exclusion counts; the split assignment
(`blake2b`, not Python's salted `hash()`); M1 (seed sd 0.0001).

**Would survive with wider error bars.** M2. Seed sd is now 0.0002 with
averaged SGD, but the *level* moved 7 points across regularisation settings.
The **sign** of the C1→C3 delta held negative across all four settings tested,
which is what the paper claims — but anyone re-running with different
hyperparameters will not reproduce 0.7376 exactly.

**Would not survive as stated.** **M3.** One seed, 100,000 training rows (9% of
the split), one epoch. The paper reports 0.7505 and its seed variance is
**unmeasured**. A re-run will differ, and the C1→C3 delta of −0.0070 rests on a
single draw — the fact that it matches M2's to four decimals is suggestive, not
demonstrated. The brief required three seeds; we have one.

**Depends on a live source.** Everything. EDCS grows. The corpus hash is pinned
precisely because a re-harvest reproduces none of this.

---

## Which decisions rest on assumption rather than evidence?

- **That `X(milia)` renders a vinculum.** The plain text proves the letters are
  *supplied*; it does not prove *what glyph they replace*. Standard Roman
  practice makes the overline overwhelmingly likely. It remains an inference.
  (D-0017)
- **That the `-N` record-id suffix marks faces of one monument.** Inferred from
  100% province agreement and 299 byte-identical texts. EDCS documentation
  would settle it. (D-0004)
- **That `⟦ ⟧` marks rasura.** Inferred from Leiden convention. (D-0014)
- **The three numeral word lists.** Every entry is a philological judgement.
- **The inflectional/lexical rule** is a prefix proxy, not a lemmatiser. It
  reproduces the prior report's figure to within 0.7 points, which is
  reassuring but not validating — both could share the same bias.
- **That barring line-break fragments from test sets is right.** My decision,
  not the brief's, and stated as such.

---

## Which need a Latinist rather than an automated check?

1. Whether `X(milia)` renders the vinculum.
2. Whether the 376 `|` forms group into sign families as I ruled them.
3. Which of 56 near-duplicate `|` forms are keying errors and which are ancient
   orthographic variants (`|(muleris)`, `|(centvria)`).
4. Whether the three numeral word lists are semantically correct.
5. Whether the 64,991 partly-restored abbreviations belong in the task.
6. Whether Type 3 gold labels should be surface forms (`VIvir`) or Latin
   readings (`sevir`).
7. Whether formulaic recurrence across monuments is leakage or the domain.

None is resolvable by more computation.

---

## What is the strongest argument for rejecting this paper?

**"The labels are the editor's, so the benchmark measures EDCS's house style
rather than Latin — and the authors demonstrate this themselves."**

We show that 66.4% of expansion forms also occur uncontracted, that 0.96% of
rows disagree under identical evidence, that the task is conditioned on the
editor having judged an abbreviation present, and that the strongest signal we
found is memorisation of local convention. A reviewer can assemble our own
evidence into the case that the task is not well-posed as *"expand Latin
abbreviations"* but only as *"predict EDCS's expansions"*.

**The honest response** is that this is true and is the paper's subject rather
than its flaw — but it constrains the claims severely, and the paper must not
present the benchmark as measuring Latin competence. A reviewer who wanted to
reject on this ground could do so without misreading anything.

**Second strongest:** the model experiment is thin. Three baselines, one of
them at a single seed on 9% of the data, producing a negative result. A
reviewer may say the resource contribution is real but the benchmark is not yet
a benchmark.

---

## What is the weakest claim currently in the paper?

**"M2 and M3 agree to four decimal places" (both −0.0070).**

It is arithmetically true and rhetorically powerful, and it is **weaker than it
reads**. M3 is one seed; its delta is a single draw with no error bar. The
agreement is very likely coincidence at the fourth decimal. The defensible claim
is "both models that read text show a small negative delta, and neither shows
the positive delta the hypothesis predicts."

I have used the coincidence in the abstract. It should be softened before
submission, or three M3 seeds should be run so the claim earns its emphasis.

**Runner-up:** "to our knowledge the first dataset for Latin epigraphic
abbreviation expansion." That rests on searches run in one session, not a
systematic review. `related_work.md` already phrases it as a bounded negative;
the abstract should match.

---

## What would change my mind about the headline finding?

The finding is that province conditioning is memorisation. It would be
overturned by: a model that reads text showing a **positive** C1→C3 delta
exceeding seed variance on **held-out provinces** (not merely on the primary
test); or evidence that the three withheld provinces are unrepresentative in a
way that suppresses transfer — the century confound (1AD is 26.8% of the
primary test and 5.7% of the held-out test) is a live candidate that the
current experiment does not rule out.

I regard the second as the most likely route to a different answer, and it is
not yet closed.

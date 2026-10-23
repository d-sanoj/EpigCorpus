# Phase 2 — Overline / vinculum empirical check

**Question.** Does EDCS's plain-text transcription preserve the vinculum —
the overline that multiplied a Roman numeral by 1,000 (`X̄` = 10,000), and
that over letters marked an abbreviation?

**Answer: no. The vinculum is not preserved in EDCS plain-text
transcription.** In its multiplicative sense it is absent entirely. What
replaced it is a lexical device, and that device is currently sitting in the
kept dataset as corrupt abbreviation pairs.

Method: a **full character census** of all 588,509 records — no sampling and
no assumption about which code points might occur — followed by recovery of
the context of every mark found. `scripts/phase2_vinculum.py`,
`phase2_crossref.py`, `phase2_numeral_fusion.py`. Corpus fixed by D-0001.

---

## 2.1 The census

39,470,885 characters, 414 distinct code points.

| code point | occurrences | records |
| --- | --- | --- |
| **U+0305 COMBINING OVERLINE** | **2** | 2 |
| **U+0304 COMBINING MACRON** | **0** | 0 |
| **U+0332 COMBINING LOW LINE** | **1** | 1 |

Every combining mark in the corpus, without exception:

| code point | occurrences | records |
| --- | --- | --- |
| U+0323 COMBINING DOT BELOW | 208 | 75 |
| U+0301 COMBINING ACUTE ACCENT | 5 | 5 |
| U+0300 COMBINING GRAVE ACCENT | 2 | 1 |
| U+0305 COMBINING OVERLINE | 2 | 2 |
| U+0332 COMBINING LOW LINE | 1 | 1 |
| U+05B4, U+030A, U+0327, U+0302 | 1 each | 1 each |
| **total** | **222** | — |

222 combining marks in 39.5 million characters: **5.6 × 10⁻⁶ of the corpus.**

Blocks that could carry the job, checked because they exist rather than
because they were expected:

| block | occurrences |
| --- | --- |
| Number Forms U+2150–U+218F — includes **U+2183 REVERSED ROMAN NUMERAL ONE HUNDRED (Ↄ)**, the reversed C | **0** |
| Ancient Symbols U+10190–U+101CF — includes ROMAN DENARIUS, SEXTANS, UNCIA SIGN | **0** |
| Supplemental Punctuation U+2E00–U+2E7F | 8 (U+2E0C/U+2E0D raised omission brackets only, in 3 records) |
| precomposed macron letters | 28 — **all Greek** (ᾱ 25, ῡ 2, ῑ 1), marking vowel length in Greek text |

Unicode provides dedicated code points for the reversed-C sign and for the
Roman denarius, sextans and uncia signs. **EDCS uses none of them.** It uses
ASCII `|` — 16,465 occurrences — as catalogued in Phase 1 (D-0013).

## 2.2 The three sightings, in full

All three are reproduced because there are only three.

| record | mark | context |
| --- | --- | --- |
| `EDCS-00000939-0` | U+0305 on `q` | `Augustal(i) Cumis, q̅(uaestori), / qui vixit ann(is) XXXVIII` |
| `EDCS-05802229-0` | U+0305 on `I` | `Messal(l)ae II[I̅viro(?) a(ere) a(rgento) a(uro) f(lando) f(eriundo?)` |
| `EDCS-25500308-0` | U+0332 on `τ` | `Διονύϲιοϲ / οπτο τ̲<e=η>§Dionysos opto te` |

**None of the three is a multiplicative vinculum.**

- `q̅(uaestori)` is the **abbreviation overline** over a letter, not a numeral.
- `I̅` in `III̅viro` sits on a numeral, but `IIIvir` is the *triumvir* office —
  a numeral used as a word-prefix (the Phase 3b Type 3 construction), and the
  overline marks the compound as abbreviated, not as ×1000.
- `τ̲` is a **Greek** tau in a Greek text.

So the multiplicative vinculum has **zero** attestations, and the
abbreviation overline survives in **two** Latin records out of 588,509 —
a rate of 3.4 × 10⁻⁶ of records. Both are best read as accidental survivals
of source-edition formatting rather than as a convention.

**This is not an encoding limitation.** EDCS does retain 208 instances of
U+0323, the Leiden underdot for an uncertain letter (`ṛẹṣṭịṭuerunt`,
`dilạp̣ṣọṣ`) — so combining marks pass through the pipeline. Overlines are
absent by transcription convention, not because diacritics are stripped.
Equally, 208 in 75 records is itself vanishingly rare: **no Leiden diacritic
is systematically preserved.** The correct statement is that EDCS's
plain-text field is essentially diacritic-free.

## 2.3 What EDCS uses instead — the cross-reference to the `|(...)` inventory

The negative only matters if something took the vinculum's place. Two
devices do, and they are not the same device.

### Device A — numeral + a supplied word in parentheses

`HS X(milia)` — 1,875 occurrences in the raw text.

| property | value |
| --- | --- |
| occurrences of `N(milia/mille/milli-)` with a Roman numeral before the paren | 1,875 |
| **preceded by `HS` (sestertius sign)** | **1,226 — 65.4%** |
| numerals used | II 187 · III 158 · X 151 · V 107 · L 100 · IIII 93 · VI 92 · XX 90 · C 73 |

Contexts: `HS III(milia) mutuor(um)`, `HS X(milia)`, `HS L(milia) C(milia)`,
`C(milia) / X(milia) / C(milia)`.

**The numerals are small multipliers, two thirds of them monetary.** This is
precisely the job the vinculum did: `X̄` = ten thousand sesterces. EDCS
renders the overline as a **supplied word inside parentheses**.

**Consequence for Phase 3b, and it is the substantive result of this phase.**
The brief stipulates a Type 2 class — *numeral + supplied unit, NOT an
abbreviation, the word is nowhere on the stone.* Phase 2 supplies the reason
the stipulation is correct: **the word is nowhere on the stone because on
the stone it was an overline, not a word.** Type 2 changes from a
definitional choice into a documented transcription convention.

The device generalises past *milia* to the whole weights-and-measures
system — appended words in the kept set include **librae 361, sextarii 76,
modii 24, libras 24, iugera 9** — and to ordinals: **quarta 12, tertia 11**,
as in `p(ro) p(arte) IIII(quarta)`, "for a fourth part".

### Device B — the `|` sign plus a supplied word

`|(miliaria)` and kin — 1,381 occurrences. Cross-referenced against the
Phase 1 inventory (D-0013), this is the milliary sign family, 1,429
occurrences across 22 forms.

Token immediately preceding: `HS` 136 · `Batavor(um)` 33 · `coh(ortis)` 25 ·
`Fl(avia)` 19 · `Britt(onum)` 19 · `|(mille)` 41.

**Device B is predominantly unit-strength, not currency.** `Coh(ors) I
F(lavia) Dam(ascenorum) |(miliaria)` is a thousand-strong cohort — an
adjective describing a military unit, not a multiplier on a number. But the
136 `HS`-preceded cases show the two uses are **not cleanly separated in
EDCS**, and `|(mille)|(mille)|(mille)DC` (= 3,600) shows the sign being
**repeated to multiply** — the same repetition-as-quantity principle
recorded for `||(mulierum)` in D-0013.

## 2.4 The device is currently in the dataset as corrupt pairs

Applying a stated rule to the kept 1,424,314 — abbreviation is entirely
Roman-numeral characters, and the expansion is that string with a complete
word appended that occurs ≥20 times in its own right as an expansion:

| | pairs | types |
| --- | --- | --- |
| fused numeral pairs in the kept set | **2,338** | 446 |
| of which the thousands family (*milia*, *milibus*, *mille*) | **1,607** | — |

Top offenders: `II → IImilia` 124 · `III → IIImilia` 122 · `X → Xmilia` 107 ·
`L → Lmilia` 84 · `V → Vmilia` 66 · `III → IIIlibrae` 57 · `II → IIlibrae` 46 ·
`II → IIduorum` 40 · `VI → VIvir` 31.

`X → Xmilia` is not a Latin word and was never on a stone. **These pairs are
in the released dataset today.**

### The rule's false positives, stated

The rule is a bounded probe for Phase 2, **not** the Phase 3b taxonomy, and
it misfires in two identifiable ways. Both are reported rather than quietly
filtered:

- **`V → Vixit` (13) is a genuine abbreviation** — `V(ixit)` = *vixit*. The
  rule fired because `ixit` occurs 48 times as a standalone expansion. Those
  48 are themselves an artifact: EDCS breaks lines mid-word, so `v/` ends one
  line and `ix(it)` begins the next, producing the spurious pair
  `ix → ixit`. **This is a line-break fragmentation artifact not previously
  catalogued in any report** and it is handed to Phase 3.
- **`D → Diae` (6) is a genuine name** — `d(eae) D(iae)`, the goddess Dea Dia.

The **thousands family (1,607) contains no visible false positives**:
*milia*, *milibus* and *mille* are supplied units under any reading. The
wider 2,338 figure is a **candidate set** for Phase 3b, which the brief
requires to be resolved against explicit, printed ordinal and unit word
lists with reported coverage. Phase 2 supplies the evidence base for those
lists; it does not pre-empt them.

---

## Self-adversarial pass (R7)

**Most likely reviewer attack:** *"You searched for the code points you
expected to find. Absence of evidence in a targeted search is not evidence of
absence."*

**Answered by construction.** No search was targeted: the method is a full
census of all 39,470,885 characters, enumerating all 414 distinct code points
present. The three named code points were then read out of a complete
inventory rather than grepped for. Any overline-like mark in any block would
appear in `all_combining_marks`; the complete list is 222 marks, reproduced
above in full.

**Second attack:** *"EDCS may strip all diacritics, making this trivial."*
**Run, and it fails** — 208 U+0323 underdots pass through. The pipeline
transmits combining marks; overlines are absent by convention. But the
honest qualifier is stated in 2.2: at 208 instances no Leiden diacritic is
*systematically* preserved either.

**Third attack, the one that damages me:** *"D-0008 recorded a U+0305
sighting and you wrote that it raises the prior that Phase 2 will not be a
pure negative. Did you mark that prediction?"*

**The prediction was wrong and is withdrawn.** One sighting in a corpus of
39.5 million characters is n = 2, not a signal. Phase 2 *is* a pure negative
on the vinculum, and a near-pure negative on the abbreviation overline. The
Phase 0 caution was right for the wrong reason: the phase was worth running
not because overlines would be found, but because the census surfaced the
replacement device — which no amount of expecting a null would have.

**What remains unresolved.**
- That `X(milia)` renders a vinculum is an **inference** from the
  parenthesis convention (letters in parentheses are supplied, not carved)
  plus standard Roman numeral practice. The plain text alone establishes
  that the letters are supplied, **not what glyph they replace**. Status:
  ASSUMPTION. **[VERIFY — LATINIST]** and **[NEEDS CITATION]** — EDCS's own
  published statement of its transcription conventions, which would settle
  it directly.
- Whether Device A and Device B are one convention or two. The 136
  `HS`-preceded `|(miliaria)` cases argue they overlap. **[VERIFY — LATINIST]**
- The 2,338 − 1,607 = 731 non-thousands fused pairs are unclassified here by
  design. Owned by Phase 3b.

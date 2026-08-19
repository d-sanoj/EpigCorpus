# Cleaning rules: LatEpig 2.0 vs EpigCorpus

Prepared 2026-08-19, before any Phase 1 change. Compares
`lat_epig/text_parse.py` (commit `91559166`) against `src/edcs_cleaner.py`,
rule by rule, and measures the disagreement on the harvested corpus.

LatEpig defines **27** conservative and **25** interpretive rules (not 25/24). Full
rule-by-rule mapping in §7.

**Headline: the two pipelines disagree on 31.1% of conservative and 30.3% of
interpretive output** (60,000-inscription sample). Four differences account for
almost all of it, and **LatEpig is right on all four**. One of them is a
corpus-corrupting bug not listed in the remediation brief.

---

## 1. The four substantive differences

### D1 — `<X=Y>` substitution direction is INVERTED (5.98% of the corpus)

EDCS marks an editorial substitution as `<intended=inscribed>`: the left side is
the reading, the right side is what the stone actually carries.

| Input | Correct reading | LatEpig cons | EpigCorpus cons | LatEpig interp | EpigCorpus interp |
|---|---|---|---|---|---|
| `Aurelia <Z=S>osime` | Zosime | `Sosime` ✓ | `Zosime` ✗ | `Zosime` ✓ | `Sosime` ✗ |
| `<v=B>i<x=S>it` | vixit | `BiSit` ✓ | `vixit` ✗ | `vixit` ✓ | `BiSit` ✗ |
| `sua pe<c=Q>unia` | pecunia | `peQunia` ✓ | `pecunia` ✗ | `pecunia` ✓ | `peQunia` ✗ |
| `ann<o=U>s` | annos | `annUs` ✓ | `annos` ✗ | `annos` ✓ | `annUs` ✗ |

Conservative should reproduce **what is on the stone** (the right side);
interpretive should give **the editorial reading** (the left side). EpigCorpus
has both backwards:

```python
# conservative — keeps the LEFT side, i.e. the interpretation
re.sub(r"<([^=>]*)=[^>]*>", r"\1", text)
# interpretive — keeps the RIGHT side, i.e. the raw stone letters
re.sub(r"<[^=><]*=([^>]*)>", r"\1", text)
```

The interpretive column currently emits non-words — `BiSit`, `peQunia`, `annUs`,
`noBAE` — for **35,178 inscriptions**. Any lexical statistic over that column is
wrong for those rows, and any conservative statistic is silently reporting the
editor's reading as if it were the stone.

This is not in the brief. It is more damaging than T05.

### D2 — Removing `[...]` fuses adjacent words (5.76%)

LatEpig replaces a restoration with a **space**; EpigCorpus replaces it with
**nothing**.

| Input | LatEpig | EpigCorpus |
|---|---|---|
| `D[ivi f(ilius) / Aug]ustus` | `D ustus` | `Dustus` |
| `maxim[us / tribuni]c(ia)` | `maxim c` | `maximc` |
| `de[p]os(i)t(us)` | `de ost` | `deost` |

EpigCorpus invents tokens (`Dustus`, `maximc`, `deost`) that appear nowhere in
Latin, inflating the type count and corrupting frequency lists.

### D3 — Mid-word line breaks are split rather than rejoined (12.45%)

EDCS marks line breaks with `/`, and words routinely break across lines.
LatEpig deletes `/` outright, which rejoins the halves; EpigCorpus replaces it
with a space, which cements the split.

| Input | LatEpig | EpigCorpus |
|---|---|---|
| `Antonio Mar/tiali` | `Martiali` | `Mar tiali` |
| `[co]niu/gi su[o du]lcisimo` | `coniugi` | `coniu gi` |

This is the **largest** affected class: 73,254 inscriptions. Deleting `/` is
safe for the common ` / ` case too, because the surrounding spaces survive.

### D4 — Illegible-letter markers survive (0.34%)

LatEpig includes `+` and `=` in its interpunction class; EpigCorpus omits both,
so `+++` becomes a token. This is **T07**, already on the list, and LatEpig
already solves it.

---

## 2. Where EpigCorpus is better, or equal

- **Nested parentheses.** LatEpig's `\([^(]*\)` and EpigCorpus's `\([^)]*\)`
  both mis-handle nesting, differently: on `(A(uli) f(ilius))` LatEpig yields
  `A f`, EpigCorpus `f`. LatEpig is closer, but neither is correct — **T09
  stands regardless of which ruleset we adopt.**
- **Multi-character substitutions.** LatEpig's `<X=Y>` rules only match *single*
  characters, so `no<vem=BAE>` leaves debris (`novemAE`). EpigCorpus's
  `[^>]*` form handles multi-character spans cleanly. **Adopting LatEpig
  verbatim would import this bug.**
- **`[1]` gap marker.** LatEpig's pattern is `re.compile(r'[1]')` — a character
  class, so it deletes *every digit 1* anywhere in the text, not the `[1]`
  marker. EpigCorpus's `\[1\]` is correct.
- **Apostrophes.** EpigCorpus strips `'`; LatEpig does not.

## 3. Bugs both share

Adopting LatEpig's rules verbatim would **not** fix these:

- **T05** — both split every `-que`: `atque` → `at que`, `neque` → `ne que`.
  LatEpig's `(\w+)(que)\b` is, if anything, broader than ours.
- **T06** — both leave `sevir` and `duovir` unsplit. LatEpig's
  `([I|V|X])(vir*)` is narrower still: single `I`/`V`/`X` only, no `L`/`C`/`D`/`M`,
  and `vir*` actually means `vi` plus zero-or-more `r`. Its character class also
  literally contains `|`.
- **T08** — both strip Arabic digits and keep Roman numerals.
- **T09** — neither parses balanced delimiters.

## 4. Symbol coverage

LatEpig strips four epigraphic symbols EpigCorpus misses: `𐆖` (sestertius),
`⏑`, `⏓`, `⏕` (metrical marks). LatEpig also treats `=` as interpunction.

---

## 5. Options

### Option A — Adopt LatEpig's ruleset verbatim
Maximum comparability with published LatEpig-derived datasets.
*Imports* the multi-character substitution bug, the `[1]` digit-deletion bug,
and all four shared bugs. Not recommended as an end state, but defensible as a
**benchmarking baseline** for T24.

### Option B — Fix the four substantive differences, keep our better rules (recommended)
Take LatEpig's behaviour on D1–D4; keep our correct handling of multi-character
substitutions, `[1]`, and apostrophes; then proceed with Phase 1 (T05–T09) as
planned. Yields a defensible corpus that is *close* to LatEpig without
inheriting its defects.

### Option C — Implement both, and report agreement
Ship `clean_*_latepig()` alongside our own and publish the agreement rate as a
validation result. This is what makes T24's benchmark meaningful and is the
strongest option for the paper — it costs one extra module and turns a
methodological objection into a measured finding.

**Recommendation: B for the corpus, plus C for the paper.** They compose: fix
the rules, keep a faithful LatEpig implementation in the test suite as a
comparison baseline.

---

## 6. Reproducing this

The LatEpig rules were imported directly from a clone of
`mqAncientHistory/Lat-Epig` at commit `91559166` and applied with its own
`clean()` loop, so no transcription error is possible. Disagreement was measured
over the first 59,987 non-empty inscriptions of the 2026-08-19 harvest.

| Measurement | Value |
|---|---|
| Conservative disagreement | 18,635 / 59,987 (31.1%) |
| Interpretive disagreement | 18,200 / 59,987 (30.3%) |
| Rows containing `<X=Y>` | 35,178 (5.98%) |
| Rows at risk of word fusion | 33,874 (5.76%) |
| Rows with mid-word `/` | 73,254 (12.45%) |
| Rows containing `+` | 1,990 (0.34%) |

---

## 7. Rule-by-rule mapping (complete)

LatEpig defines **27 conservative** and **25 interpretive** rules, applied in
dict-insertion order. EpigCorpus defines 13 named steps, several of which
contain more than one substitution — 24 regex operations for conservative,
24 for interpretive. Below, every LatEpig rule is matched to its EpigCorpus
counterpart.

Legend: **=** identical · **~** equivalent effect · **!** differs · **X** absent.

### 7.1 Conservative — 27 LatEpig rules

| # | LatEpig rule | LatEpig pattern → replacement | EpigCorpus counterpart | EpigCorpus pattern → replacement | | Consequence |
|--:|---|---|---|---|:-:|---|
| 1 | `dubious_dot_subscript` | `̣` → `` | `step1_dubious_dot` | `̣` → `` | **=** | — |
| 2 | `edcs_number_three_both` | `\[3\]` → `[-] ` | `step2_edcs_gaps` | `\[3\]` → `[-]` | **!** | they add a trailing space |
| 3 | `edcs_number_three_right` | `3\]` → `-] ` | — | — | **X** | unclosed `3]` gap not handled |
| 4 | `edcs_number_three_left` | `\[3` → ` [-` | — | — | **X** | unclosed `[3` gap not handled |
| 5 | `edcs_number_three_middle` | `(\[\w+)( [3] )(\w+\])` → ` \1 \3 ` | — | — | **X** | mid-gap form not handled |
| 6 | `edcs_number_six_both` | `\[6\]` → `[-] ` | `step2_edcs_gaps` | `\[6\]` → `[-]` | **!** | trailing space |
| 7 | `edcs_number_one` | `[1]` → ` ` | `step2_edcs_gaps` | `\[1\]` → ` ` | **!** | **ours is correct**; theirs is a character class that deletes every digit `1` |
| 8 | `edcs_quotes` | `"` → ` ` | `step3_quotes_backslashes` | `"` → `` | **!** | space vs nothing |
| 9 | `edcs_backslashes` | `\\\\` → ` ` | `step3_quotes_backslashes` | `\` → `` | **!** | they match a *double* backslash; we also strip `'` |
| 10 | `expanded_abbreviations_conservative` | `\([^(]*\)` → `` | `step4_conservative` | `\([^)]*\)` → `` | **!** | negated class differs → different nesting behaviour (T09) |
| 11 | `suppresion_superscripts_conservative` | `{[^}]*}[⁰¹²³⁴⁵⁶⁷⁸⁹]+` → `` | — | — | **X** | superscript-indexed suppressions not handled |
| 12 | `suppresion_conservative` | `[\{*\}]` → `` | `step4_conservative` | `\{([^}]*)\}` → `\1` | **~** | same for braces; theirs also deletes `*` |
| 13 | `restoration_conservative` | `\[[^[]*\]` → **` `** | `step4_conservative` | `\[[^\]]*\]` → **``** | **!** | **D2** — we fuse adjacent words |
| 14 | `substitution_edh_conservative` | `(<)(α)=(α)(>)` → **`\3`** | `step4_conservative` | `<([^=>]*)=[^>]*>` → **`\1`** | **!** | **D1** — direction inverted |
| 15 | `substitution_edh_conservative_missing` | `(<)(α)*=(α)(>)` → `\3` | *(same rule)* | *(same)* | **!** | **D1** |
| 16 | `substitution_conservative` | `\<[^<]*\>` → `` | `step4_conservative` | `<[^>]*>` → `` | **~** | — |
| 17 | `new_line` | `[\|/]` → **``** | `step5_line_breaks` | `/` → **` `** | **!** | **D3** — we split words; we also ignore `\|` |
| 18 | `interpunction_symbols` | `[=+,.․:⋮⁙;!\-—–#%^&~@]` → ` ` | `step6_punctuation_symbols` | `[,.\-—:;!#%^&~@]` → `` | **!** | **D4** — we miss `= + ․ ⋮ ⁙ –`; space vs nothing |
| 19 | `epigraphic_symbols` | `[❦·∙𐆖⏑⏓⏕]` → `` | `step6_punctuation_symbols` | `[❦·∙]` → `` | **!** | we miss `𐆖 ⏑ ⏓ ⏕` |
| 20 | `uncertainty_symbols` | `[\\?]` → `` | `step7_uncertainty` | `?` → `` | **~** | theirs also strips a backslash here |
| 21 | `arabic_numerals` | `[0-9]+` → `` | `step8_arabic_numerals` | `[0-9]` → `` | **~** | identical effect |
| 22 | `unclosed_brackets` | `[\[\{\(\)\}\]\|]` → `` | `step9_unclosed_brackets` | `[\[\]\{\}()]` → `` | **~** | theirs also drops `\|` |
| 23 | `edcs_que` | `(\w+)(que)\b` → `\1 \2` | `step10_que_enclitic` | `(?<=[A-Za-z])(que)(?=\s\|$)` → ` \1` | **!** | **both wrong (T05)**; theirs fires more broadly |
| 24 | `edcs_vir` | `([I\|V\|X])(vir*)` → `\1 \2` | `step11_numeral_vir` | `([IVXLCDMivxlcdm]+)(vir\w*)` → `\1 \2` | **!** | **both wrong (T06)**; ours is broader and better |
| 25 | `extra_blank` | `[ ]+` → ` ` | — | — | **X** | em-space only; effectively a no-op |
| 26 | `multi_whitespace` | `\s+` → ` ` | `step12_collapse_spaces` | `\s+` → ` ` | **=** | — |
| 27 | `whitespace_endline` | `(^\s\|\s$)` → `` | `step13_strip` | `.strip()` | **~** | — |

### 7.2 Interpretive — 25 LatEpig rules

Rules 1–8 and 15–25 are identical to the conservative list above (except that
the gap replacements omit LatEpig's trailing space). Only the five
markup-resolution rules differ:

| # | LatEpig rule | LatEpig pattern → replacement | EpigCorpus counterpart | EpigCorpus pattern → replacement | | Consequence |
|--:|---|---|---|---|:-:|---|
| 9 | `expanded_abbreviations_interpretive` | `[\(*\)]` → `` | `step4_interpretive` | `\(([^)]*)\)` → `\1` | **~** | both keep the content |
| 10 | `suppresion_remove_interpretive` | `{[^}]*}` → `` | `step4_interpretive` | `\{[^}]*\}` → `` | **=** | — |
| 11 | `restoration_interpretive` | `[\[*\]]` → `` | `step4_interpretive` | `\[([^\]]*)\]` → `\1` | **~** | both keep the content |
| 12 | `substitution_edh_interpretive` | `(α)=(α)` → **`\1`** | `step4_interpretive` | `<[^=><]*=([^>]*)>` → **`\1`** (right side) | **!** | **D1** — inverted. **Ours handles multi-character spans; theirs mangles them** (`no<vem=BAE>` → `novemAE`) |
| 13 | `substitution_edh_interpretive_missing` | `(α)*=(α)` → `\2` | *(same rule)* | *(same)* | **!** | **D1** |
| 14 | `substitution_interpretive` | `[\<*\>]` → `` | `step4_interpretive` | `<([^>]*)>` → `\1` | **~** | both keep the content |

### 7.3 Summary of the mapping

| Verdict | Conservative | Interpretive |
|---|--:|--:|
| Identical (**=**) | 2 | 3 |
| Equivalent (**~**) | 6 | 9 |
| Differs (**!**) | 15 | 11 |
| Absent from EpigCorpus (**X**) | 4 | 2 |
| **LatEpig rules total** | **27** | **25** |

Of the 15 conservative differences, four change the corpus materially (D1–D4 in
§1). Four more are LatEpig defects we should *not* copy: its `[1]` digit
deletion, its single-character substitution limit, its em-space-only blank
rule, and its `([I|V|X])` class containing a literal pipe. The rest are
cosmetic (space vs empty replacement) but still shift tokenisation.

---

## 8. The authoritative EDCS conventions

Source: EDCS's own **"Explanation of the Presentation of Inscription Texts"**,
§7 of its search help, retrieved 2026-08-19 from
`/data/indexes/i18n/en.json` (key `search_help_html`) on
`edcs.hist.uzh.ch`. This is the primary source — the site's own documentation,
served to every user — and it supersedes inference from examples.

### 8.1 What EDCS documents

| Marker | EDCS's own wording |
|---|---|
| `/` | line break |
| `( )` | expansion of abbreviated texts, insertion of missing letters |
| `[ ]` | supplement of lost text passages |
| `[3]` | gap of indeterminate length within a line |
| `[6]` | gap the length of a line |
| `]` | gap of indeterminate length at the beginning |
| `[` | gap of indeterminate length at the end |
| `⟦ ⟧` | erasure |
| `<e=F>` | "correction of an error in the inscription by the editors or normalization of spelling (example `f<e=F>cit` for **FFCIT on the stone**)" |
| `«abc»` | texts inserted in antiquity in place of deleted passages |
| `«⟦ ⟧»` | text first deleted in antiquity, then re-carved |
| `{ }` | deletion by the editors |
| `ạ` | letters read with uncertainty, inferred from context |
| `\|` | denotes special symbols such as `\|(centurio)`, `\|(centuria)`, `\|(denarius)`, `\|(semuncia)` |
| `*` | inscription considered forged or post-antique. **For CIL the `*` precedes the number** (`CIL 06, *03231`); **for others it follows** (`RIB-03, 03534*`). Inclusion of *falsae* is not systematic and their texts are not consistently expanded or categorised |
| `(!)` | directly after a word, marks an unusual spelling (`Aemiliaes(!)`); standing alone, marks a missing word |

### 8.2 D1 is settled: our substitution direction is backwards

EDCS's example is unambiguous. The stone carries **FFCIT**; the corrected
reading is **fecit**; EDCS writes `f<e=F>cit`. Therefore in `<X=Y>`:

- **`X` (left) = the editors' correction / normalised spelling**
- **`Y` (right) = what is actually carved on the stone**

It follows that:

| Pipeline | Should keep | We keep | LatEpig keeps |
|---|---|---|---|
| Conservative (what the stone says) | **right** (`Y`) | left ✗ | right ✓ |
| Interpretive (editorial reading) | **left** (`X`) | right ✗ | left ✓ |

**Both of our pipelines are inverted**, confirmed against the primary source and
consistent with every corpus example (`f<e=F>cit`→fecit, `<v=B>i<x=S>it`→vixit,
`pe<c=Q>unia`→pecunia, `ann<o=U>s`→annos, `<Z=S>osime`→Zosime). Affects
**35,178 inscriptions (5.98%)**.

The standard Leiden system uses `⟨abc⟩` for letters wrongly omitted and restored
by the editor; EDCS's `<X=Y>` is a database-specific extension of it, which is
why the Leiden literature alone does not resolve the direction — only EDCS's own
worked example does.

### 8.3 Documented conventions NEITHER pipeline handles

Measured over 588,349 inscriptions:

| Marker | Rows | % | Current behaviour |
|---|--:|--:|---|
| `\|` special-symbol marker | 10,628 | 1.81% | **LatEpig deletes it as a line break — wrong.** We ignore it, so it survives into output |
| `(!)` unusual-spelling marker | 10,626 | 1.81% | Both treat it as an abbreviation expansion, so conservative silently drops it and interpretive reduces it to `!` |
| `⟦ ⟧` erasure (U+27E6/27E7) | 4,733 | 0.80% | Unhandled by both; the characters survive cleaning |
| `«abc»` antique insertion | 484 | 0.08% | Unhandled by both; the characters survive cleaning |
| `‹X=Y›` guillemet substitution | 7 | <0.01% | Unhandled by both |
| `]` gap at beginning | 79,283 | 13.48% | Dropped as a stray bracket; the "text is missing here" signal is lost |
| `[` gap at end | 88,101 | 14.97% | Same |

The ASCII `[[ ]]` form that both pipelines assume for erasure occurs in only
**72** rows. The real erasure marker is `⟦ ⟧`, which neither strips.

### 8.4 Gap-code inventory

`[N]` is a family, of which EDCS documents only two members:

| Code | Rows | Status |
|---|--:|---|
| `[3]` | 282,608 | documented — gap within a line |
| `[6]` | 7,355 | documented — gap the length of a line |
| `[1]` | 4,928 | **undocumented** |
| `[2]` | 903 | **undocumented** |
| `[4]`, `[5]`, `[21]`, `[24]`, `[29]`, `[33]` | <25 each | **undocumented** |

Both pipelines special-case `[1]`, `[3]` and `[6]` and let the rest fall through
to generic `[...]` removal, which is a reasonable outcome but is not grounded in
any documented meaning. Worth an email to the EDCS editors before publication.

### 8.5 `(( ))` is a red herring

The brief's T09 example, `((sestertium))`, occurs in **2 inscriptions** in the
entire corpus. Nested-delimiter parsing still matters for ordinary nested
expansions such as `(A(uli) f(ilius))`, but the double-parenthesis form should
not drive the design.

### 8.6 Confirmed correct

Our `is_forged` test — a substring search for `*` in the citation — matches
EDCS's documented convention in both its forms, since the marker may precede
(CIL) or follow (others) the number. **T04's mechanism is sound.** Note EDCS's
own caveat that *falsae* coverage "is not systematic", which limits what any
forgery statistic can claim.

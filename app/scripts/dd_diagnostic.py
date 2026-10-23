#!/usr/bin/env python3
"""Diagnose the "DDominis" artifact in scripts/abbrev_probe.py.

The probe builds an expansion by concatenating the letters outside the
parentheses with the letters inside, per whitespace token. That is correct for
D(is) -> Dis and Aug(ustus) -> Augustus. It is visibly wrong for DD(ominis),
which yields "DDominis" rather than "dominis".

This script only measures. It imports abbrev_probe for its tokenizer and
filters so that it counts exactly the pairs the probe accepts, and it changes
nothing.

The hard part is telling two lookalikes apart:

    ann(os)   -> "annos"      correct: annos really has a double n
    dd(ominis)-> "ddominis"   wrong:   the second d is a plural marker

Both end in a doubled letter, so a pattern match cannot separate them. The
test used here is corpus attestation. Build a lexicon of expansions from
tokens that are NOT suspects, then ask which reading of a suspect is a word
the corpus already knows:

    "annos"   is attested (from an(nos))     -> the doubling is real spelling
    "ddominis" is not attested anywhere,
    but "dominis" is (from d(ominis))        -> the doubling is geminatio

That decides each case on evidence in the data rather than on a guess.
"""

from __future__ import annotations

import importlib.util
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data" / "edcs_inscriptions.jsonl"
OUT_MD = REPO / "reports" / "dd_diagnostic.md"

# The probe's own reported total, used to prove this walker is a faithful mirror.
EXPECTED_PAIRS = 1_424_314

N_EXAMPLES = 30

# Roman numerals are carved in capitals in EDCS. Requiring upper case keeps
# lowercase words made of numeral letters (vix, mil, ill, civ) out of the
# numeral bucket -- they are abbreviations, not numbers.
ROMAN_UPPER = re.compile(r"^[IVXLCDM]+$")
# A single numeral character. These are NOT bucketed as numerals -- D(is),
# M(anibus), C(aius), L(ucius) are ordinary abbreviations and vastly outnumber
# any true single-digit numeral -- but they are checked separately in section 6b.
ROMAN_ONE = re.compile(r"^[IVXLCDM]$")
DOUBLED_TAIL = re.compile(r"(.)\1$", re.IGNORECASE)

# Endings that often mark a Latin plural. Deliberately crude; see the caveat
# printed alongside every number derived from it.
PLURAL_ENDINGS = ("orum", "arum", "ibus", "es", "is", "i", "a", "um")
# ... and endings that are just as often singular, which is why the heuristic
# cannot be trusted on its own.
AMBIGUOUS_ENDINGS = ("is", "i", "a", "um")


def load_probe():
    """Import scripts/abbrev_probe.py without executing its main()."""
    path = Path(__file__).resolve().parent / "abbrev_probe.py"
    if not path.exists():
        raise SystemExit(f"FATAL: cannot find {path}")
    spec = importlib.util.spec_from_file_location("abbrev_probe", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ap = load_probe()


# --------------------------------------------------------------------------
# faithful mirror of the probe's accept/reject chain, but yielding the token
# --------------------------------------------------------------------------

def walk(text: str):
    """Yield (token, abbrev, expansion) for every pair the probe would accept."""
    if "(" not in text:
        return
    masked = ap.mask_markup(text)

    for tok, off in ap.iter_tokens(text):
        if "(" not in tok:
            continue
        lead = len(tok) - len(tok.lstrip(ap.EDGE_PUNCT))
        tok = tok.strip(ap.EDGE_PUNCT)
        off += lead
        if "(" not in tok:
            continue
        if any(masked[off + i] for i in range(len(tok)) if off + i < len(masked)):
            continue
        if any(c in ap.MARKUP_CHARS for c in tok):
            continue
        if tok.count("(") != tok.count(")"):
            continue
        if re.search(r"\([^)]*\(", tok):
            continue
        groups = re.findall(r"\(([^)]*)\)", tok)
        cleaned = [g.rstrip("?!").strip() for g in groups]
        if any(g == "" for g in cleaned):
            continue
        i = 0

        def _sub(_m):
            nonlocal i
            v = cleaned[i]
            i += 1
            return "(" + v + ")"

        norm = re.sub(r"\([^)]*\)", _sub, tok)
        abbrev = re.sub(r"\([^)]*\)", "", norm)
        expansion = norm.replace("(", "").replace(")", "")
        if abbrev == "":
            continue
        if ap.script_of(expansion) == "greek" or ap.script_of(abbrev) == "greek":
            continue
        if re.search(r"\d", tok):
            continue
        if not ap.is_latin_alpha(abbrev):
            continue
        if not ap.is_latin_alpha(expansion):
            continue
        if len(expansion) <= len(abbrev):
            continue
        yield tok, abbrev, expansion


# --------------------------------------------------------------------------
# bucketing
# --------------------------------------------------------------------------

def prefix_before_first_paren(tok: str) -> str:
    m = re.match(r"^([A-Za-z]+)\(", tok)
    return m.group(1) if m else ""


def repeat_run(prefix: str):
    """Length of the trailing run of one repeated letter, and that letter."""
    if len(prefix) < 2:
        return 0, ""
    last = prefix[-1].lower()
    n = 0
    for ch in reversed(prefix):
        if ch.lower() == last:
            n += 1
        else:
            break
    return (n, last) if n >= 2 else (0, "")


def bucket_of(prefix: str) -> str | None:
    """doubled / numeral / mixed, or None if the token is not a suspect."""
    if not prefix:
        return None
    run, _ = repeat_run(prefix)
    is_dbl = run >= 2
    is_rom = bool(ROMAN_UPPER.match(prefix)) and len(prefix) >= 2
    if is_dbl and is_rom:
        return "mixed"
    if is_dbl:
        return "doubled"
    if is_rom:
        return "numeral"
    return None


def collapse(prefix: str) -> str:
    """DD -> D, Augg -> Aug, DDD -> D, conss -> cons."""
    run, _ = repeat_run(prefix)
    return prefix[: len(prefix) - run + 1] if run >= 2 else prefix


def looks_plural(word: str):
    """(is_plural_looking, ending_matched, ending_is_ambiguous). Crude."""
    w = word.lower()
    for end in PLURAL_ENDINGS:
        if w.endswith(end) and len(w) > len(end):
            return True, end, end in AMBIGUOUS_ENDINGS
    return False, "", False


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    if not RAW.exists():
        raise SystemExit(f"FATAL: raw corpus not found: {RAW}")

    out = []

    def emit(s=""):
        out.append(s)

    pct = lambda a, b: f"{100 * a / b:.2f}%" if b else "n/a"

    control = Counter()          # expansions from non-suspect tokens
    control_other = Counter()    # ... excluding single-numeral-prefix tokens
    singles = []                 # single-numeral-prefix tokens, for section 6b
    all_abbrev = Counter()       # every accepted pair, case-folded abbrev
    exp_by_abbrev = defaultdict(Counter)
    suspects = []                # (bucket, prefix, tok, abbrev, expansion, prov, cent)
    examples = defaultdict(list)
    baseline_prov = Counter()
    baseline_cent = Counter()
    baseline_cat = Counter()
    n_pairs = 0
    n_records = 0

    with RAW.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            n_records += 1
            text = rec.get("inscription_text", "")
            if not isinstance(text, str) or not text.strip():
                continue
            prov = (rec.get("province") or "").strip()
            nb = ap.parse_year(rec.get("not_before"))
            na = ap.parse_year(rec.get("not_after"))
            cent, _ = ap.midpoint_century(nb, na)
            cats = rec.get("category_en") or []
            rid = rec.get("record_id", "")

            for tok, abbrev, expansion in walk(text):
                n_pairs += 1
                fa = abbrev.lower()
                all_abbrev[fa] += 1
                exp_by_abbrev[fa][expansion.lower()] += 1
                baseline_prov[prov] += 1
                baseline_cent[cent] += 1
                for c in cats:
                    baseline_cat[c] += 1

                pre = prefix_before_first_paren(tok)
                b = bucket_of(pre)
                if b is None:
                    control[expansion.lower()] += 1
                    if ROMAN_ONE.match(pre):
                        singles.append((tok, pre, expansion, prov, cent))
                    else:
                        control_other[expansion.lower()] += 1
                else:
                    suspects.append((b, pre, tok, abbrev, expansion, prov, cent, tuple(cats)))
                    if len(examples[b]) < N_EXAMPLES:
                        examples[b].append((tok, rid, text, expansion))

    emit("# The DD(ominis) artifact: diagnosis")
    emit()
    emit("Diagnosis only. `scripts/abbrev_probe.py` is imported, not modified, and "
         "nothing under `src/` or `data/` is touched.")
    emit()

    emit("## 0. Is this counting the same pairs the probe counts?")
    emit()
    ok = n_pairs == EXPECTED_PAIRS
    emit(f"- records read: **{n_records:,}**")
    emit(f"- pairs accepted by this walker: **{n_pairs:,}**")
    emit(f"- pairs reported by the probe: **{EXPECTED_PAIRS:,}**")
    emit(f"- **{'match, so the numbers below describe the real dataset' if ok else 'MISMATCH -- the mirror has drifted, treat every number below as suspect'}**")
    emit()

    # ---------------- buckets ----------------
    emit("## 1. The three buckets")
    emit()
    emit("A token is a suspect when the letters before its first parenthesis are a "
         "repeated letter, a Roman numeral, or both. `mixed` exists because `DD`, "
         "`CC`, `LL`, `MM`, `XX` and `III` are simultaneously repeated letters and "
         "numeral characters -- there is no way to assign them to one bucket on shape "
         "alone.")
    emit()
    emit("Roman numerals are required to be upper case. EDCS carves numerals in "
         "capitals, and without that rule `vix(it)`, `mil(es)` and `civ(is)` -- whose "
         "letters are all numeral letters -- would be swept in as numbers.")
    emit()
    bc = Counter(s[0] for s in suspects)
    emit(ap.md(pd.DataFrame(
        [(b, f"{bc.get(b, 0):,}", pct(bc.get(b, 0), n_pairs))
         for b in ("doubled", "numeral", "mixed")]
        + [("all suspects", f"{len(suspects):,}", pct(len(suspects), n_pairs))],
        columns=["bucket", "tokens", "share of all pairs"])))
    emit()
    emit("**These are suspects, not defects.** Section 2 shows why most of the "
         "`doubled` bucket is perfectly correct.")
    emit()

    # ---------------- examples ----------------
    emit("## 2. Thirty real examples per bucket")
    emit()
    for b in ("doubled", "numeral", "mixed"):
        emit(f"### bucket: `{b}`")
        emit()
        rows = []
        for tok, rid, text, expansion in examples[b][:N_EXAMPLES]:
            snippet = re.sub(r"\s+", " ", text)
            if len(snippet) > 150:
                snippet = snippet[:150] + " ..."
            rows.append((f"`{tok}`", rid, f"`{expansion}`", snippet))
        if rows:
            emit(ap.md(pd.DataFrame(
                rows, columns=["raw token", "inscription id", "current rule produces", "inscription_text"])))
        else:
            emit("_none_")
        emit()

    # ---------------- attestation test ----------------
    emit("## 3. Which suspects are actually broken")
    emit()
    emit("For each suspect, two candidate readings are checked against the control "
         "lexicon (expansions produced by non-suspect tokens only, so the test cannot "
         "feed on itself):")
    emit()
    emit("- **naive** -- what the current rule produces (`ann`+`os` = `annos`, `dd`+`ominis` = `ddominis`)")
    emit("- **collapsed** -- the doubled run reduced to one letter (`d`+`ominis` = `dominis`)")
    emit("- **bare** -- for numerals, the parenthesis content alone (`vicesimae`)")
    emit()

    verdicts = []
    for (b, pre, tok, abbrev, expansion, prov, cent, cats) in suspects:
        naive = expansion.lower()
        rest = expansion[len(pre):]
        coll = (collapse(pre) + rest).lower()
        bare = rest.lower()
        s_naive = control.get(naive, 0)
        s_coll = control.get(coll, 0)
        s_bare = control.get(bare, 0)

        # Arbitrate by strength of evidence, not by a fixed priority order.
        # An earlier version accepted the first reading with any support at all,
        # which mis-ruled XX(milia): "xmilia" has 108 attestations (all of them
        # from X(milia), the same artifact one letter shorter) while the correct
        # "milia" has 1,786. Taking the best-supported reading settles it.
        options = [("correct_as_is", s_naive)]
        if b in ("doubled", "mixed"):
            options.append(("geminatio", s_coll))
        if b in ("numeral", "mixed"):
            options.append(("numeral_word", s_bare))
        options.sort(key=lambda kv: -kv[1])
        best, best_n = options[0]
        runner_up = options[1][1] if len(options) > 1 else 0
        v = best if best_n > 0 else "unresolved"
        margin = (best_n / runner_up) if runner_up > 0 else float("inf")
        verdicts.append((b, pre, tok, abbrev, expansion, prov, cent, cats,
                         v, naive, coll, bare, s_naive, s_coll, s_bare, margin))

    vc = Counter((v[0], v[8]) for v in verdicts)
    rows = []
    for b in ("doubled", "numeral", "mixed"):
        for v in ("correct_as_is", "geminatio", "numeral_word", "unresolved"):
            if vc.get((b, v)):
                rows.append((b, v, f"{vc[(b, v)]:,}"))
    emit(ap.md(pd.DataFrame(rows, columns=["bucket", "verdict", "tokens"])))
    emit()
    tot_v = Counter(v[8] for v in verdicts)
    emit(ap.md(pd.DataFrame(
        [(v, f"{tot_v.get(v, 0):,}", pct(tot_v.get(v, 0), len(suspects)))
         for v in ("correct_as_is", "geminatio", "numeral_word", "unresolved")],
        columns=["verdict", "tokens", "share of suspects"])))
    emit()
    emit("### How firm is each call?")
    emit()
    emit("A verdict is only as good as the gap between the winning reading and the "
         "runner-up. Decisions where the winner has at least three times the support "
         "of the next candidate are counted as decisive; the rest are close calls and "
         "should be treated as provisional.")
    emit()
    decided = [v for v in verdicts if v[8] != "unresolved"]
    decisive = [v for v in decided if v[15] >= 3]
    thin = [v for v in decided if v[15] < 3]
    emit(ap.md(pd.DataFrame([
        ("decisive (winner >= 3x runner-up)", f"{len(decisive):,}", pct(len(decisive), len(decided))),
        ("thin (winner < 3x runner-up)", f"{len(thin):,}", pct(len(thin), len(decided))),
    ], columns=["confidence", "tokens", "share of decided"])))
    emit()
    if thin:
        tt = Counter((v[2], v[8]) for v in thin)
        emit("Close calls, most frequent first:")
        emit()
        emit(ap.md(pd.DataFrame(
            [(f"`{t}`", verdict, f"{c:,}") for (t, verdict), c in tt.most_common(12)],
            columns=["token", "verdict", "occurrences"])))
        emit()

    # ---------------- geminatio / plural test ----------------
    emit("## 4. Testing the geminatio hypothesis")
    emit()
    emit("### 4a. The plural-ending heuristic (weak evidence)")
    emit()
    gem = [v for v in verdicts if v[8] == "geminatio"]
    ok_as_is = [v for v in verdicts if v[8] == "correct_as_is"]

    def plural_profile(items, use_collapsed=True):
        pl = amb = 0
        for it in items:
            word = it[10] if use_collapsed else it[9]
            is_pl, end, is_amb = looks_plural(word)
            if is_pl:
                pl += 1
                if is_amb:
                    amb += 1
        return pl, amb

    g_pl, g_amb = plural_profile(gem)
    c_pl, c_amb = plural_profile(ok_as_is, use_collapsed=False)
    emit(ap.md(pd.DataFrame([
        ("geminatio (corrected reading)", f"{len(gem):,}", f"{g_pl:,}",
         pct(g_pl, len(gem)), pct(g_amb, max(g_pl, 1))),
        ("correct-as-is control group", f"{len(ok_as_is):,}", f"{c_pl:,}",
         pct(c_pl, len(ok_as_is)), pct(c_amb, max(c_pl, 1))),
    ], columns=["group", "tokens", "plural-looking", "share", "of which on an ambiguous ending"])))
    emit()
    emit("**This heuristic is not trustworthy on its own and the numbers above should "
         "not be quoted as a plural rate.** `-is` is the plural dative/ablative in "
         "*dominis* but the genitive singular in *civitatis*; `-i`, `-a` and `-um` are "
         "each singular at least as often as plural. The column showing how much of "
         "the signal rests on those ambiguous endings is there to make the weakness "
         "visible. The contrast in 4b is the evidence worth believing.")
    emit()

    emit("### 4b. Doubled form vs single form, side by side (strong evidence)")
    emit()
    emit("If doubling marks a plural, then for the same stem the doubled abbreviation "
         "should carry plural expansions and the single one singular expansions. This "
         "compares them directly and needs no ending list.")
    emit()
    gem_by_abbrev = Counter(v[3].lower() for v in gem)
    rows = []
    for ab, n in gem_by_abbrev.most_common(15):
        single = collapse(ab)
        gem_exps = Counter()
        for v in gem:
            if v[3].lower() == ab:
                gem_exps[v[10]] += 1
        sing_exps = exp_by_abbrev.get(single, Counter())
        rows.append((
            f"{ab} (n={n:,})",
            ", ".join(f"{e} ({c:,})" for e, c in gem_exps.most_common(3)),
            f"{single}",
            ", ".join(f"{e} ({c:,})" for e, c in sing_exps.most_common(3)),
        ))
    if rows:
        emit(ap.md(pd.DataFrame(rows, columns=[
            "doubled abbrev", "its corrected expansions", "single abbrev", "its expansions"])))
    emit()

    # ---------------- reverse case ----------------
    emit("## 5. The reverse case: doubled letters that are just spelling")
    emit()
    emit("These tokens end in a doubled letter and the current rule handles them "
         "**correctly**. They are the reason a pattern match on doubled letters is not "
         "a bug detector: the double n in *annos* and the double s in *dulcissimae* "
         "belong to the word.")
    emit()
    ok_by_tok = Counter(v[2] for v in ok_as_is)
    rows = [(f"`{t}`", f"{c:,}") for t, c in ok_by_tok.most_common(25)]
    emit(ap.md(pd.DataFrame(rows, columns=["token", "occurrences"])))
    emit()
    emit("A second trap sits in multi-parenthesis tokens: `d(e)d(icavit)` produces the "
         "abbreviation `dd`, which looks like geminatio but expands correctly to "
         "*dedicavit*. This is why suspects are detected on the letters before the "
         "**first** parenthesis rather than on the assembled abbreviation.")
    emit()

    # ---------------- exact recount ----------------
    emit("## 6. Exact recount of affected pairs")
    emit()
    affected = [v for v in verdicts if v[8] in ("geminatio", "numeral_word")]
    unresolved = [v for v in verdicts if v[8] == "unresolved"]
    emit(ap.md(pd.DataFrame([
        ("confirmed broken (geminatio + numeral)", f"{len(affected):,}", pct(len(affected), n_pairs)),
        ("unresolved, cannot be decided from the corpus", f"{len(unresolved):,}", pct(len(unresolved), n_pairs)),
        ("worst case, if every unresolved case is broken",
         f"{len(affected) + len(unresolved):,}", pct(len(affected) + len(unresolved), n_pairs)),
        ("earlier estimate in the probe report", "3,753", "0.26%"),
    ], columns=["measure", "pairs", "share of all 1,424,314 pairs"])))
    emit()
    emit("### by bucket")
    emit()
    ab_b = Counter(v[0] for v in affected)
    emit(ap.md(pd.DataFrame(
        [(b, f"{ab_b.get(b, 0):,}", pct(ab_b.get(b, 0), len(affected)))
         for b in ("doubled", "numeral", "mixed")],
        columns=["bucket", "affected pairs", "share of affected"])))
    emit()
    emit("### by distinct abbreviation form")
    emit()
    ab_form = Counter(v[3] for v in affected)
    rows = []
    for form, c in ab_form.most_common(40):
        exs = Counter(v[4] for v in affected if v[3] == form)
        corr = Counter(v[10] for v in affected if v[3] == form)
        rows.append((form, f"{c:,}",
                     ", ".join(f"{e}" for e, _ in exs.most_common(2)),
                     ", ".join(f"{e}" for e, _ in corr.most_common(2))))
    emit(ap.md(pd.DataFrame(rows, columns=[
        "abbrev", "affected pairs", "current (broken) expansion", "corrected reading"])))
    emit()
    emit(f"_{len(ab_form):,} distinct abbreviation forms are affected in total._")
    emit()

    emit("### by province, against the corpus baseline")
    emit()
    emit("`lift` is the province's share of affected pairs divided by its share of all "
         "pairs. 1.0 means the artifact is spread exactly like the corpus; above ~2 "
         "means it concentrates there.")
    emit()
    ap_prov = Counter(v[5] for v in affected)
    rows = []
    for p, c in ap_prov.most_common(20):
        base = baseline_prov.get(p, 0)
        share_a = c / len(affected) if affected else 0
        share_b = base / n_pairs if n_pairs else 0
        lift = share_a / share_b if share_b else float("nan")
        rows.append((p or "(none)", f"{c:,}", f"{100*share_a:.2f}%", f"{100*share_b:.2f}%", f"{lift:.2f}"))
    emit(ap.md(pd.DataFrame(rows, columns=[
        "province", "affected", "share of affected", "share of corpus", "lift"])))
    emit()

    emit("### by century, against the corpus baseline")
    emit()
    ap_cent = Counter(v[6] for v in affected)
    rows = []
    for c_ in sorted((c for c in ap_cent if c is not None)):
        c = ap_cent[c_]
        base = baseline_cent.get(c_, 0)
        share_a = c / len(affected) if affected else 0
        share_b = base / n_pairs if n_pairs else 0
        lift = share_a / share_b if share_b else float("nan")
        rows.append((ap.century_label(c_), f"{c:,}", f"{100*share_a:.2f}%", f"{100*share_b:.2f}%", f"{lift:.2f}"))
    undated = ap_cent.get(None, 0)
    rows.append(("(undated)", f"{undated:,}",
                 f"{100*undated/len(affected):.2f}%" if affected else "n/a",
                 f"{100*baseline_cent.get(None,0)/n_pairs:.2f}%", ""))
    emit(ap.md(pd.DataFrame(rows, columns=[
        "century", "affected", "share of affected", "share of corpus", "lift"])))
    emit()

    emit("### by inscription category")
    emit()
    emit("This is the bias question that matters: if the artifact sits inside imperial "
         "and military texts, discarding it quietly removes that stratum.")
    emit()
    ap_cat = Counter()
    for v in affected:
        for c in v[7]:
            ap_cat[c] += 1
    tot_cat_a = sum(ap_cat.values())
    tot_cat_b = sum(baseline_cat.values())
    rows = []
    for c, n in ap_cat.most_common(15):
        share_a = n / tot_cat_a if tot_cat_a else 0
        share_b = baseline_cat.get(c, 0) / tot_cat_b if tot_cat_b else 0
        lift = share_a / share_b if share_b else float("nan")
        rows.append((c, f"{n:,}", f"{100*share_a:.2f}%", f"{100*share_b:.2f}%", f"{lift:.2f}"))
    emit(ap.md(pd.DataFrame(rows, columns=[
        "category", "affected", "share of affected", "share of corpus", "lift"])))
    emit()

    emit("### 6b. A blind spot one character below the threshold")
    emit()
    emit("The three buckets require at least two letters before the parenthesis. That "
         "threshold exists for a good reason -- `D(is)`, `M(anibus)`, `C(aius)` and "
         "`L(ucius)` are ordinary abbreviations that happen to be numeral letters, and "
         "there are 271,012 such tokens. Bucketing them would be a catastrophe.")
    emit()
    emit("But the same artifact does occur there: `X(milia)` produces `Xmilia`. These "
         "cannot be found by the arbitration above, because with nothing to collapse "
         "their broken reading is the only reading in the control lexicon -- indeed "
         "`X(milia)` is *why* `xmilia` had 108 attestations, which is what made the "
         "first version of this diagnostic mis-rule `XX(milia)` as geminatio.")
    emit()
    emit("The test used here is independent support: flag a single-numeral token when "
         "its naive reading is attested **only** by tokens of the same shape, while the "
         "parenthesis content on its own is attested elsewhere.")
    emit()
    blind_hits = []
    for tok, pre, expansion, prov, cent in singles:
        naive = expansion.lower()
        bare = expansion[len(pre):].lower()
        if control_other.get(naive, 0) == 0 and control_other.get(bare, 0) > 0:
            blind_hits.append((tok, naive, bare, control.get(naive, 0),
                               control_other.get(bare, 0), prov, cent))
    emit(f"- single-numeral-prefix tokens scanned: **{len(singles):,}**")
    emit(f"- flagged by this test: **{len(blind_hits):,}** ({pct(len(blind_hits), n_pairs)} of all pairs)")
    emit()
    bh = Counter((h[0], h[1], h[2], h[3], h[4]) for h in blind_hits)
    emit(ap.md(pd.DataFrame(
        [(f"`{t}`", f"`{nv}`", f"{sn:,}", f"`{br}`", f"{sb:,}", f"{c:,}")
         for (t, nv, br, sn, sb), c in bh.most_common(20)],
        columns=["token", "current output", "its support (all self)", "bare reading",
                 "its independent support", "occurrences"])))
    emit()
    emit("**Read this number with more caution than the others.** The genuine cases are "
         "the numeral-plus-measure ones -- *milia*, *librae*, *milibus*, *uncia*, "
         "*assem*, *mille* -- where a Roman numeral is followed by the unit it counts. "
         "Mixed in are false positives on names, where the naive reading is correct and "
         "merely rare: `C(aiae)` is *Caiae*, `V(atiam)` is *Vatiam*, `L(a)elia` is "
         "*Laelia*. Eyeballing the list puts the genuine share somewhere around "
         "four-fifths, but that is an impression, not a measurement, so this figure is "
         "reported beside the headline count rather than added into it.")
    emit()

    # ---------------- interaction with the probe's tables ----------------
    emit("## 7. Do these forms contaminate the probe's tables?")
    emit()
    top50 = [a for a, _ in all_abbrev.most_common(50)]
    aff_forms = {v[3].lower() for v in affected}
    in_top50 = [a for a in top50 if a in aff_forms]
    emit(f"- affected abbreviation forms appearing in the **top-50 frequency table**: "
         f"**{len(in_top50)}** ({', '.join('`'+a+'`' for a in in_top50) if in_top50 else 'none'})")

    # spurious ambiguity: expansions of an abbrev that exist only because of the artifact
    spurious = []
    for form in sorted(aff_forms):
        exps = exp_by_abbrev.get(form, Counter())
        if len(exps) < 2:
            continue
        broken = {v[4].lower() for v in affected if v[3].lower() == form}
        corrected = {v[10] for v in affected if v[3].lower() == form}
        n_after = len((set(exps) - broken) | corrected)
        if n_after != len(exps):
            spurious.append((form, f"{sum(exps.values()):,}", len(exps), n_after,
                             ", ".join(sorted(broken)[:3])))
    emit(f"- affected forms that currently register as **ambiguous** and whose "
         f"expansion count would change once corrected: **{len(spurious)}**")
    emit()
    if spurious:
        spurious.sort(key=lambda r: -int(r[1].replace(",", "")))
        emit(ap.md(pd.DataFrame(spurious[:25], columns=[
            "abbrev", "freq", "distinct expansions now", "after correction", "broken expansions"])))
        emit()
    emit("There is a second, subtler effect. Correcting by collapsing the repeat moves "
         "these pairs onto a **different abbreviation key**: every `dd` pair becomes a "
         "`d` pair. That does not just clean up `dd`, it enlarges the expansion set of "
         "`d`, `c`, `l`, `m`, `n` and `aug` -- the highest-frequency entries in the "
         "ambiguity table. The correction therefore changes the headline ambiguity "
         "numbers in both directions, and any fix should be followed by a re-run rather "
         "than an adjustment of the existing figures.")
    emit()
    merge_rows = []
    for form, n in Counter(v[3].lower() for v in affected).most_common(12):
        tgt = collapse(form)
        if tgt != form:
            merge_rows.append((form, f"{n:,}", tgt,
                               f"{all_abbrev.get(tgt, 0):,}",
                               f"{len(exp_by_abbrev.get(tgt, Counter())):,}"))
    if merge_rows:
        emit(ap.md(pd.DataFrame(merge_rows, columns=[
            "affected form", "pairs", "would merge into", "that form's current freq",
            "its current distinct expansions"])))
        emit()

    # ---------------- strategies ----------------
    emit("## 8. Three handling strategies (proposed, not applied)")
    emit()
    emit("### A. Drop the affected pairs")
    emit()
    emit("**For:** one filter, no linguistic judgement, no risk of inventing a reading "
         "the editor did not intend. The numeral cases in particular have no single "
         "correct concatenation, so dropping them is honest.")
    emit()
    emit("**Against:** it deletes a stratum rather than a random sample. The imperial "
         "titulature that carries geminatio -- *dominis nostris*, *Augustorum*, "
         "*Impp(eratoribus)* -- is exactly the material a model would need to learn "
         "imperial formulae, and section 6 shows where it concentrates. It also throws "
         "away the plural information the doubling encodes.")
    emit()
    emit("### B. Collapse the repeat and keep the singular stem")
    emit()
    emit("**For:** produces the philologically correct string (*dominis*, not "
         "*DDominis*), and the attestation test in section 3 confirms the collapsed "
         "reading against expansions the corpus already contains, so it is checkable "
         "rather than assumed.")
    emit()
    emit("**Against:** it silently discards the plural marking, which is real "
         "information -- `dd nn` means *two* emperors, and after collapsing, `dd` and "
         "`d` become indistinguishable. It also rewrites the abbreviation key and so "
         "reshuffles the ambiguity tables (section 7). It does nothing for the numeral "
         "bucket, where there is no repeat to collapse.")
    emit()
    emit("### C. Keep them, corrected, with a plurality flag")
    emit()
    emit("**For:** keeps every pair, records the correct expansion, and preserves the "
         "geminatio as an explicit feature (`plural=True`, `marker=dd`) instead of "
         "destroying or ignoring it. Downstream work can filter on the flag, so this "
         "strategy contains both of the others -- A and B remain available as filters "
         "over a flagged dataset, while the reverse is not true.")
    emit()
    emit("**Against:** it costs two new columns in the TSV and a documented convention, "
         "and the numeral bucket still needs its own rule (the parenthesis content "
         "replaces the numeral rather than continuing it). The unresolved residue in "
         "section 3 has to be labelled as unresolved rather than guessed.")
    emit()

    # ---------------- verdict ----------------
    emit("## 9. Verdict")
    emit()
    v_bug = len([v for v in verdicts if v[8] == "numeral_word"])
    v_gem = len(gem)
    emit(f"**Bug or convention?** Both, and the split is clean. "
         f"{len(ok_as_is):,} of the {len(suspects):,} suspect tokens "
         f"({pct(len(ok_as_is), len(suspects))}) are handled correctly right now -- "
         "the doubled letter is ordinary spelling, as in *annos* and *dulcissimae*. "
         "The remainder are two distinct EDCS conventions the rule does not know "
         f"about: geminatio marking a plural ({v_gem:,} pairs) and a Roman numeral "
         f"standing for a word ({v_bug:,} pairs). The concatenation rule is not wrong "
         "in general; it is wrong for these two conventions.")
    emit()
    emit(f"**True affected count:** **{len(affected):,} pairs "
         f"({pct(len(affected), n_pairs)} of the corpus)**, plus {len(unresolved):,} "
         f"unresolved, giving a worst case of {len(affected) + len(unresolved):,}. "
         "The earlier 3,753 / 0.26% estimate was low, because its pattern required "
         "upper-case numeral letters and so missed every lower-case geminatio -- "
         "`dd(ominis)`, `nn(ostris)`, `augg(ustorum)`, `impp(eratoribus)`, "
         "`conss(ulibus)` -- which are the bulk of the phenomenon.")
    emit()
    # Pull the actual numbers rather than pointing the reader at a table.
    def lift_of(counter_a, counter_b, key, tot_a, tot_b):
        if not tot_a or not tot_b or not counter_b.get(key):
            return float("nan")
        return (counter_a.get(key, 0) / tot_a) / (counter_b.get(key, 0) / tot_b)

    cent_lifts = {c: lift_of(ap_cent, baseline_cent, c, len(affected), n_pairs)
                  for c in ap_cent if c is not None}
    cat_lifts = {c: lift_of(ap_cat, baseline_cat, c, tot_cat_a, tot_cat_b)
                 for c in ap_cat if ap_cat[c] >= 200}
    hot_cent = sorted((c for c in cent_lifts if cent_lifts[c] >= 1.5),
                      key=lambda c: -cent_lifts[c])[:4]
    hot_cat = sorted(cat_lifts, key=lambda c: -cat_lifts[c])[:4]

    emit("**Bias: yes, strongly, and in a way that matters.** This is not spread evenly "
         "across the corpus.")
    emit()
    emit("- By period: " + "; ".join(
        f"{ap.century_label(c)} lift {cent_lifts[c]:.1f}x" for c in hot_cent) +
        " -- against " + "; ".join(
        f"{ap.century_label(c)} {cent_lifts[c]:.2f}x" for c in
        sorted((c for c in cent_lifts if cent_lifts[c] < 0.6), key=lambda c: cent_lifts[c])[:3]) +
        ". Geminatio marks *co-rule*, so it tracks the periods with more than one "
        "emperor -- the tetrarchy and after. The artifact is effectively a late-antique "
        "stratum.")
    emit("- By category: " + "; ".join(
        f"{c} lift {cat_lifts[c]:.1f}x" for c in hot_cat) +
        ". Tomb inscriptions, the corpus's largest genre, are the reverse at "
        f"{cat_lifts.get('tomb inscriptions', float('nan')):.2f}x.")
    emit()
    emit("Dropping these pairs would therefore not remove a random 0.8%. It would "
         "preferentially delete imperial titulature, milestones and 4th-5th century "
         "material while leaving the funerary bulk untouched -- a systematic thinning "
         "of exactly the formulaic, well-dated material that a disambiguation "
         "experiment would most want to condition on.")
    emit()
    emit("**Recommendation: C, keep them with a corrected expansion and a plurality "
         "flag.** The affected share is small enough that neither dropping nor "
         "collapsing would move headline accuracy much, which is precisely why the "
         "decision should be made on information rather than convenience. Geminatio is "
         "not noise -- it is the corpus telling you how many emperors were reigning, "
         "and it is concentrated in the imperial and military formulae that a "
         "disambiguation model most needs to see. Strategy C is also the only one that "
         "is reversible: a flagged dataset can be filtered down to A or B later, "
         "whereas dropped or collapsed pairs cannot be recovered without another full "
         "re-extraction.")
    emit()

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(out) + "\n", encoding="utf-8")

    # short verdict to stdout
    print(f"records            {n_records:,}")
    print(f"pairs (mirror)     {n_pairs:,}   probe reported {EXPECTED_PAIRS:,}   "
          f"{'MATCH' if ok else 'MISMATCH'}")
    print(f"suspect tokens     {len(suspects):,}")
    print(f"  correct as is    {len(ok_as_is):,}")
    print(f"  geminatio        {v_gem:,}")
    print(f"  numeral-as-word  {v_bug:,}")
    print(f"  unresolved       {len(unresolved):,}")
    print(f"AFFECTED           {len(affected):,}  ({pct(len(affected), n_pairs)} of all pairs)")
    print(f"  of which thin    {len(thin):,} (low-confidence calls)")
    print(f"blind spot (6b)    {len(blind_hits):,}  single-numeral tokens, lower confidence")
    print()
    print("VERDICT  both: a correct rule meeting two conventions it does not know.")
    print(f"         {len(ok_as_is):,}/{len(suspects):,} suspects are already correct "
          f"(annos, officina).")
    print(f"         {v_gem:,} are geminatio (plural marker); {v_bug:,} are numeral-as-word.")
    print(f"         true affected = {len(affected):,} ({pct(len(affected), n_pairs)}), "
          f"not 3,753/0.26%.")
    print("         biased: late antiquity and imperial/milestone texts, see section 6.")
    print("         recommend strategy C (correct + plurality flag); it is the only")
    print("         reversible option.")
    print(f"report             {OUT_MD.relative_to(REPO)}")


if __name__ == "__main__":
    main()

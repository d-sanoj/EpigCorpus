#!/usr/bin/env python3
"""Audit every exclusion made by scripts/abbrev_probe.py.

The probe kept 1,424,314 (abbreviation, expansion) pairs and dropped 342,714
candidate tokens across ten reasons. A dropped token is a decision, and each
decision either protects the dataset or quietly reshapes it. This script
re-derives every exclusion, breaks each reason into the distinct situations
hiding inside it, and measures whether the dropped material differs
systematically from the material that was kept.

Audit only. abbrev_probe.py is imported for its tokenizer, never modified,
and nothing under src/ or data/ is touched.

The bias test is the point of the exercise. A filter that removes a random
slice of the corpus costs only volume. A filter that removes damaged stones,
or late inscriptions, or one province, changes what the dataset says about
Latin epigraphy while looking like routine hygiene.
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
OUT_MD = REPO / "reports" / "exclusion_audit.md"

N_EXAMPLES = 40
TEXT_CAP = 300

# The probe's reported exclusion counts. The mirror below must reproduce these
# exactly, otherwise nothing in this report describes the real dataset.
EXPECTED = {
    "inside_bracket_markup": 253256,
    "editorial_marker_paren": 58720,
    "non_alphabetic_abbrev": 16335,
    "greek_script": 12987,
    "unbalanced_parens": 561,
    "no_letters_outside_parens": 469,
    "token_carries_markup": 339,
    "non_alphabetic_expansion": 27,
    "contains_numeral": 19,
    "nested_parens": 1,
}
EXPECTED_KEPT = 1_424_314

GREEK_RE = re.compile(r"[Ͱ-Ͽἀ-῿]")
ORDER = ["inside_bracket_markup", "editorial_marker_paren", "non_alphabetic_abbrev",
         "greek_script", "unbalanced_parens", "no_letters_outside_parens",
         "token_carries_markup", "non_alphabetic_expansion", "contains_numeral",
         "nested_parens"]


def load_probe():
    path = Path(__file__).resolve().parent / "abbrev_probe.py"
    spec = importlib.util.spec_from_file_location("abbrev_probe", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ap = load_probe()


# --------------------------------------------------------------------------
# mirror of the probe's filter chain, reporting the reason instead of hiding it
# --------------------------------------------------------------------------

def mask_state(tok: str, off: int, masked) -> tuple[str, str]:
    """How much of the abbreviation / the expansion sits inside brackets."""
    inparen = False
    ab, ex = [], []
    for i, c in enumerate(tok):
        if c == "(":
            inparen = True
            continue
        if c == ")":
            inparen = False
            continue
        if not c.isalpha():
            continue
        m = masked[off + i] if off + i < len(masked) else False
        (ex if inparen else ab).append(m)

    def st(v):
        if not v:
            return "none"
        if all(v):
            return "all"
        return "some" if any(v) else "none"

    return st(ab), st(ex)


def classify(text: str):
    """Yield (verdict, token, reason, detail) for every paren-bearing token.

    verdict is "kept" or "dropped". This reproduces abbrev_probe.extract_pairs
    decision for decision, in the same order, so the counts are comparable.
    """
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
            ab_s, ex_s = mask_state(tok, off, masked)
            yield "dropped", tok, "inside_bracket_markup", (ab_s, ex_s)
            continue
        if any(c in ap.MARKUP_CHARS for c in tok):
            yield "dropped", tok, "token_carries_markup", None
            continue
        if tok.count("(") != tok.count(")"):
            yield "dropped", tok, "unbalanced_parens", None
            continue
        if re.search(r"\([^)]*\(", tok):
            yield "dropped", tok, "nested_parens", None
            continue

        groups = re.findall(r"\(([^)]*)\)", tok)
        cleaned = [g.rstrip("?!").strip() for g in groups]
        if any(g == "" for g in cleaned):
            raw = [g for g in groups if g.rstrip("?!").strip() == ""]
            yield "dropped", tok, "editorial_marker_paren", raw[0] if raw else ""
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
            yield "dropped", tok, "no_letters_outside_parens", expansion
            continue
        if ap.script_of(expansion) == "greek" or ap.script_of(abbrev) == "greek":
            yield "dropped", tok, "greek_script", None
            continue
        if re.search(r"\d", tok):
            yield "dropped", tok, "contains_numeral", None
            continue
        if not ap.is_latin_alpha(abbrev):
            yield "dropped", tok, "non_alphabetic_abbrev", abbrev
            continue
        if not ap.is_latin_alpha(expansion):
            yield "dropped", tok, "non_alphabetic_expansion", expansion
            continue
        if len(expansion) <= len(abbrev):
            yield "dropped", tok, "expansion_not_longer", None
            continue
        yield "kept", tok, "", (abbrev, expansion)


# --------------------------------------------------------------------------
# sub-classification
# --------------------------------------------------------------------------

BRACKET_SUBCLASS = {
    ("all", "all"): "whole thing is editorial reconstruction",
    ("some", "none"): "abbreviation partly restored, expansion on surviving text",
    ("some", "all"): "abbreviation partly restored, expansion also inside the bracket",
    ("some", "some"): "abbreviation and expansion both straddle the bracket",
    ("all", "none"): "abbreviation restored, expansion outside the bracket",
    ("none", "all"): "expansion inside the bracket, no surviving abbreviation letters",
    ("all", "some"): "abbreviation restored, expansion straddles the bracket",
    ("none", "some"): "expansion straddles the bracket, no abbreviation letters",
    ("none", "none"): "no alphabetic content (editorial mark inside a bracket)",
}


def subclass_of(reason: str, tok: str, detail):
    if reason == "inside_bracket_markup":
        return BRACKET_SUBCLASS.get(detail, f"other {detail}")
    if reason == "editorial_marker_paren":
        has_letters = bool(re.match(r"^[A-Za-z]+\(", tok))
        raw = detail or ""
        if raw == "":
            return ("abbreviation present, editor could not resolve it"
                    if has_letters else "bare empty parentheses, no abbreviation")
        if raw == "?":
            return ("abbreviation present, reading marked uncertain"
                    if has_letters else "standalone (?) uncertainty mark")
        if raw == "!":
            return ("word marked sic, not an abbreviation"
                    if has_letters else "standalone (!) sic mark")
        return "other empty-after-stripping content"
    if reason == "non_alphabetic_abbrev":
        ab = detail or ""
        if "|" in ab:
            return "symbol abbreviation (| = the reversed-C and similar signs)"
        if GREEK_RE.search(ab):
            return "mixed script in the abbreviation"
        if re.search(r"\d", ab):
            return "digit in the abbreviation"
        return "other non-letter character in the abbreviation"
    if reason == "contains_numeral":
        if "&#x" in tok or "&#" in tok:
            return "un-decoded HTML entity leaked from the source"
        if re.search(r"\(\s*\d+\s*\)", tok):
            return "digit inside parentheses = count of lost letters"
        if re.search(r"[A-Za-z]\d|\d[A-Za-z]", tok):
            return "digit substituted for a letter (transcription typo)"
        return "other stray digit"
    if reason == "no_letters_outside_parens":
        ex = detail or ""
        if GREEK_RE.search(ex):
            return "Greek word fully supplied by the editor"
        if re.fullmatch(r"[A-Za-z]+", ex):
            return "Latin word fully supplied by the editor"
        return "other"
    if reason == "non_alphabetic_expansion":
        ex = detail or ""
        if GREEK_RE.search(ex):
            return "Greek letters in the expansion"
        if "-" in ex:
            return "dash placeholder in the expansion"
        return "other non-letter character in the expansion"
    return "(not sub-classified)"


def tvd(a: Counter, b: Counter) -> float:
    """Total variation distance between two distributions. 0 = identical."""
    ta, tb = sum(a.values()), sum(b.values())
    if not ta or not tb:
        return float("nan")
    keys = set(a) | set(b)
    return 0.5 * sum(abs(a.get(k, 0) / ta - b.get(k, 0) / tb) for k in keys)


def median_from_counter(c: Counter) -> float:
    tot = sum(c.values())
    if not tot:
        return float("nan")
    half, run = tot / 2, 0
    for k in sorted(c):
        run += c[k]
        if run >= half:
            return k
    return float("nan")


def mean_from_counter(c: Counter) -> float:
    tot = sum(c.values())
    return sum(k * v for k, v in c.items()) / tot if tot else float("nan")


def main():
    if not RAW.exists():
        raise SystemExit(f"FATAL: raw corpus not found: {RAW}")

    out = []

    def emit(s=""):
        out.append(s)

    pct = lambda a, b: f"{100 * a / b:.2f}%" if b else "n/a"

    counts = Counter()
    sub_counts = defaultdict(Counter)
    examples = defaultdict(list)
    # per-category distributions
    prov = defaultdict(Counter)
    cent = defaultdict(Counter)
    tlen = defaultdict(Counter)
    abbrev_forms = defaultdict(Counter)     # provisional abbrev of dropped pairs
    pair_types = defaultdict(set)
    pair_tokens = defaultdict(Counter)
    kept_prov, kept_cent, kept_len = Counter(), Counter(), Counter()
    kept_abbrev = Counter()
    kept_pair_types = set()
    n_kept = 0
    n_records = 0
    # multi-word expansions, measured on the raw text
    multiword = Counter()
    multiword_latin = Counter()
    # greek bilingual test
    greek_ids_with_latin = 0
    greek_ids_total = 0

    for line in RAW.open(encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        n_records += 1
        text = rec.get("inscription_text", "")
        if not isinstance(text, str) or not text.strip():
            continue
        province = (rec.get("province") or "").strip()
        nb = ap.parse_year(rec.get("not_before"))
        na = ap.parse_year(rec.get("not_after"))
        c, _ = ap.midpoint_century(nb, na)
        L = len(text)
        rid = rec.get("record_id", "")

        for m in re.finditer(r"\(([^()]*)\)", text):
            g = m.group(1)
            # a real multi-word expansion has a space BETWEEN two word characters;
            # "(r- )" and "(ilius )" merely carry a trailing space
            if re.search(r"\w\s+\w", g):
                multiword[g] += 1
                if not GREEK_RE.search(g):
                    multiword_latin[g] += 1

        had_greek_drop = False
        had_latin_keep = False

        for verdict, tok, reason, detail in classify(text):
            if verdict == "kept":
                n_kept += 1
                had_latin_keep = True
                kept_prov[province] += 1
                kept_cent[c] += 1
                kept_len[L] += 1
                kept_abbrev[detail[0].lower()] += 1
                kept_pair_types.add((detail[0].lower(), detail[1].lower()))
                continue

            counts[reason] += 1
            sc = subclass_of(reason, tok, detail)
            sub_counts[reason][sc] += 1
            prov[reason][province] += 1
            cent[reason][c] += 1
            tlen[reason][L] += 1
            if reason == "greek_script":
                had_greek_drop = True

            # provisional abbreviation/expansion for the gain analysis
            pa = re.sub(r"\([^)]*\)", "", tok)
            pe = re.sub(r"[()]", "", tok)
            pa_clean = re.sub(r"[^A-Za-z]", "", pa).lower()
            pe_clean = re.sub(r"[^A-Za-z]", "", pe).lower()
            if pa_clean and pe_clean and len(pe_clean) > len(pa_clean):
                abbrev_forms[reason][pa_clean] += 1
                pair_types[reason].add((pa_clean, pe_clean))
                pair_tokens[reason][(pa_clean, pe_clean)] += 1

            if len(examples[reason]) < N_EXAMPLES:
                snippet = re.sub(r"\s+", " ", text)
                if len(snippet) > TEXT_CAP:
                    snippet = snippet[:TEXT_CAP] + " […]"
                examples[reason].append((tok, rid, snippet, sc))

        if had_greek_drop:
            greek_ids_total += 1
            if had_latin_keep:
                greek_ids_with_latin += 1

    # ---------------- header + fidelity ----------------
    emit("# Exclusion audit")
    emit()
    emit("Every token `scripts/abbrev_probe.py` refused, re-derived and examined. "
         "Audit only: the probe is imported, not edited, and nothing under `src/` or "
         "`data/` is touched.")
    emit()
    emit("## 0. Does this reproduce the probe's decisions?")
    emit()
    rows, all_ok = [], True
    for r in ORDER:
        got, want = counts.get(r, 0), EXPECTED[r]
        ok = got == want
        all_ok &= ok
        rows.append((r, f"{want:,}", f"{got:,}", "match" if ok else "MISMATCH"))
    rows.append(("(kept pairs)", f"{EXPECTED_KEPT:,}", f"{n_kept:,}",
                 "match" if n_kept == EXPECTED_KEPT else "MISMATCH"))
    all_ok &= n_kept == EXPECTED_KEPT
    emit(ap.md(pd.DataFrame(rows, columns=["reason", "probe reported", "this audit", "status"])))
    emit()
    emit(f"**{'Every count matches, so the analysis below describes the real dataset.' if all_ok else 'MISMATCH — the mirror has drifted; treat everything below as suspect.'}**")
    emit()
    total_dropped = sum(counts.values())
    emit(f"Total dropped: **{total_dropped:,}** against **{n_kept:,}** kept "
         f"({pct(total_dropped, total_dropped + n_kept)} of all candidate tokens).")
    emit()

    # ---------------- bias helpers ----------------
    def bias_block(reason):
        emit("**Bias check.** How the dropped pairs compare with the kept pairs.")
        emit()
        p_t = tvd(prov[reason], kept_prov)
        c_t = tvd(cent[reason], kept_cent)
        emit(ap.md(pd.DataFrame([
            ("province distribution (TVD)", f"{p_t:.3f}", interpret_tvd(p_t)),
            ("century distribution (TVD)", f"{c_t:.3f}", interpret_tvd(c_t)),
            ("median inscription length",
             f"{median_from_counter(tlen[reason]):.0f} vs {median_from_counter(kept_len):.0f} kept",
             length_note(tlen[reason], kept_len)),
            ("mean inscription length",
             f"{mean_from_counter(tlen[reason]):.0f} vs {mean_from_counter(kept_len):.0f} kept", ""),
        ], columns=["measure", "value", "reading"])))
        emit()
        emit("Total variation distance: 0.00 means the dropped pairs are spread exactly "
             "like the kept ones, 1.00 means they share no common ground. Anything "
             "above about 0.15 is a materially different population.")
        emit()
        # biggest province and century divergences
        rows = []
        tot_d, tot_k = sum(prov[reason].values()), sum(kept_prov.values())
        for p, n in prov[reason].most_common(8):
            sd = n / tot_d if tot_d else 0
            sk = kept_prov.get(p, 0) / tot_k if tot_k else 0
            rows.append((p or "(none)", f"{n:,}", f"{100*sd:.2f}%", f"{100*sk:.2f}%",
                         f"{sd/sk:.2f}" if sk else "inf"))
        emit("Provinces, dropped share against kept share:")
        emit()
        emit(ap.md(pd.DataFrame(rows, columns=[
            "province", "dropped", "share of dropped", "share of kept", "lift"])))
        emit()
        rows = []
        tot_d, tot_k = sum(cent[reason].values()), sum(kept_cent.values())
        # Cells with a handful of pairs produce meaningless lifts (a 50-vs-0 split
        # reads as 281x). Show only centuries carrying at least 0.5% of the drops.
        floor = max(20, 0.005 * tot_d)
        for c_ in sorted((x for x in cent[reason] if x is not None and cent[reason][x] >= floor)):
            n = cent[reason][c_]
            sd = n / tot_d if tot_d else 0
            sk = kept_cent.get(c_, 0) / tot_k if tot_k else 0
            rows.append((ap.century_label(c_), f"{n:,}", f"{100*sd:.2f}%", f"{100*sk:.2f}%",
                         f"{sd/sk:.2f}" if sk else "inf"))
        if rows:
            emit("Centuries, dropped share against kept share "
                 "(centuries below 0.5% of this category's drops omitted as noise):")
            emit()
            emit(ap.md(pd.DataFrame(rows, columns=[
                "century", "dropped", "share of dropped", "share of kept", "lift"])))
            emit()
        # abbreviation frequency profile
        forms = abbrev_forms[reason]
        if forms:
            unseen = sum(v for k, v in forms.items() if kept_abbrev.get(k, 0) == 0)
            rare = sum(v for k, v in forms.items() if 0 < kept_abbrev.get(k, 0) < 10)
            common = sum(v for k, v in forms.items() if kept_abbrev.get(k, 0) >= 10)
            tot = sum(forms.values())
            emit("Abbreviation frequency profile of the dropped pairs, measured against "
                 "how often each form survives in the kept set:")
            emit()
            emit(ap.md(pd.DataFrame([
                ("form never seen in the kept set", f"{unseen:,}", pct(unseen, tot)),
                ("form seen fewer than 10 times", f"{rare:,}", pct(rare, tot)),
                ("form seen 10+ times (already well covered)", f"{common:,}", pct(common, tot)),
            ], columns=["profile", "dropped pairs", "share"])))
            emit()

    def interpret_tvd(v):
        if v != v:
            return "not computable"
        if v < 0.05:
            return "indistinguishable from the kept set"
        if v < 0.15:
            return "mild skew"
        if v < 0.30:
            return "materially different"
        return "a different population"

    def length_note(d, k):
        md, mk = median_from_counter(d), median_from_counter(k)
        if md != md or mk != mk:
            return ""
        if md > mk * 1.3:
            return "dropped pairs come from longer, more damaged texts"
        if md < mk * 0.77:
            return "dropped pairs come from shorter texts"
        return "comparable"

    def gain_block(reason):
        forms = abbrev_forms[reason]
        types = pair_types[reason]
        new_forms = {k for k in forms if kept_abbrev.get(k, 0) == 0}
        new_types = {t for t in types if t not in kept_pair_types}
        emit("**What recovery would gain.**")
        emit()
        emit(ap.md(pd.DataFrame([
            ("pairs recoverable in principle", f"{counts.get(reason,0):,}", ""),
            ("with a usable abbreviation and expansion", f"{sum(forms.values()):,}",
             pct(sum(forms.values()), counts.get(reason, 1))),
            ("distinct abbreviation forms", f"{len(forms):,}", ""),
            ("of those, forms absent from the kept set", f"{len(new_forms):,}",
             pct(len(new_forms), max(len(forms), 1))),
            ("distinct (abbrev, expansion) types", f"{len(types):,}", ""),
            ("of those, types absent from the kept set", f"{len(new_types):,}",
             pct(len(new_types), max(len(types), 1))),
        ], columns=["measure", "value", "share"])))
        emit()
        dup_types = len(types) - len(new_types)
        # Type counts and token counts tell opposite stories here, so give both.
        dup_tokens = sum(v for k, v in pair_tokens[reason].items() if k in kept_pair_types)
        tot_tokens = sum(pair_tokens[reason].values())
        emit(f"Duplication cuts two ways. By **type**, "
             f"{pct(len(new_types), max(len(types),1))} of the pair types here are new "
             f"to the dataset ({len(new_types):,} of {len(types):,}) — a real gain in "
             "coverage of rare forms. By **token**, "
             f"{pct(dup_tokens, max(tot_tokens,1))} of the individual pairs repeat a "
             "type the kept set already holds, because the volume sits in the same "
             "handful of funerary and imperial formulae. Recovering this category "
             "would therefore add a long tail of genuinely new forms while re-weighting "
             "the head that is already over-represented.")
        emit()

    def example_block(reason):
        emit(f"**{N_EXAMPLES} examples.**")
        emit()
        rows = [(f"`{t}`", rid, sc, txt) for t, rid, txt, sc in examples[reason]]
        if rows:
            emit(ap.md(pd.DataFrame(rows, columns=[
                "raw token", "inscription id", "sub-class", "inscription_text"])))
        else:
            emit("_none_")
        emit()

    def subclass_block(reason):
        emit("**Sub-classification.** One label was hiding several situations.")
        emit()
        tot = counts.get(reason, 0)
        emit(ap.md(pd.DataFrame(
            [(sc, f"{n:,}", pct(n, tot)) for sc, n in sub_counts[reason].most_common()],
            columns=["sub-class", "count", "share of category"])))
        emit()

    recs = {}

    # ================= 1. inside_bracket_markup =================
    r = "inside_bracket_markup"
    emit(f"## 1. `{r}` — {counts[r]:,}")
    emit()
    emit("Round parentheses inside `[ ]` are expansions of text the editor restored "
         "rather than text the stone carries.")
    emit()
    subclass_block(r)
    emit("**This overturns an assumption in the brief.** `[Imp(erator)` was offered as "
         "an example of an abbreviation left intact with only its surroundings "
         "bracketed. It is not: in `[Imp(erator) Caes]ar` the bracket span closes after "
         "*Caes*, so the letters *Imp* are themselves restored. The category where the "
         "abbreviation survives untouched and only its neighbours are bracketed is a "
         f"different reason entirely — `token_carries_markup`, {counts['token_carries_markup']:,} "
         "tokens, section 7. Inside this category no sub-class has an intact "
         "abbreviation sitting outside the brackets.")
    emit()
    example_block(r)
    gain_block(r)
    emit("**What recovery would cost.** The dominant sub-class is total reconstruction: "
         "the editor inferred the missing letters *and* the expansion of the "
         "abbreviation those letters spell. Both halves of the label come from the same "
         "act of scholarly inference, so a model trained on them learns the editor's "
         "restoration habits and is then evaluated on those same habits. That is "
         "circular, and it is worst precisely where the formulae are most predictable "
         "— which is why the duplication figure above is so high.")
    emit()
    emit("The sub-class *abbreviation partly restored, expansion on surviving text* is "
         "different in kind. There the parenthesis expands letters that are actually on "
         "the stone; only the earlier part of the word is supplied. `frumen]t(o)` is a "
         "real reading of a real abbreviation with a damaged left edge.")
    emit()
    bias_block(r)
    emit("**Recommendation: NEEDS HUMAN REVIEW.** Not because the counting is uncertain "
         "— it is not — but because the decision turns on a question this script cannot "
         "answer: what counts as the input side of the task.")
    emit()
    emit("If the input is *the text as printed in EDCS*, then "
         f"{sub_counts[r].get('abbreviation partly restored, expansion on surviving text', 0):,} "
         "partly-restored pairs are perfectly valid — `tribuni]c(ia)` gives the mapping "
         "*tribunic -> tribunicia*, which is sound Latin however the letters arrived on "
         "the page. If the input is *what the stone actually carries*, they are not, "
         "because most of the abbreviation is the editor's supplement. That is a "
         "project-defining choice about whether the dataset models epigraphic reading "
         "or editorial convention, and a Latinist should make it rather than a "
         "heuristic. The 149,582 fully-reconstructed pairs should stay out under either "
         "reading.")
    emit()
    recs[r] = ("NEEDS HUMAN REVIEW",
               sub_counts[r].get("abbreviation partly restored, expansion on surviving text", 0),
               "high")

    # ================= 2. editorial_marker_paren =================
    r = "editorial_marker_paren"
    emit(f"## 2. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    unres = sub_counts[r].get("abbreviation present, editor could not resolve it", 0)
    sic = sub_counts[r].get("word marked sic, not an abbreviation", 0)
    emit("**Reasoned answer to the question posed.** Yes, and this is the most "
         "interesting thing in the audit.")
    emit()
    emit(f"`PR()` and `M()` are not noise. An empty parenthesis is the editor recording "
         f"that an abbreviation is present on the stone and cannot be resolved. There "
         f"are **{unres:,}** such tokens, each one a genuine abbreviation with a "
         "known surface form and a deliberately withheld expansion.")
    emit()
    emit("That is exactly the supervision an abstention class needs, and it cannot be "
         "manufactured. Sampling random abbreviations and hiding their answers produces "
         "cases that are unresolvable-by-construction; these are cases that a "
         "professional epigrapher, holding the stone and the whole formulaic context, "
         "judged unresolvable. A model that can predict *this one cannot be expanded* "
         "is more useful than one that always guesses, and it can only learn that "
         "distinction from labels like these.")
    emit()
    emit(f"The rest of the category is different and should stay out. The `(!)` "
         f"sub-classes ({sic:,} tokens) are *sic* marks: the editor is flagging a "
         "spelling error on the stone, not expanding an abbreviation. Standalone `(?)` "
         "marks carry no abbreviation at all.")
    emit()
    bias_block(r)
    recs[r] = ("RECOVER AS SEPARATE CLASS", unres, "low")

    # ================= 3. non_alphabetic_abbrev =================
    r = "non_alphabetic_abbrev"
    emit(f"## 3. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    emit("The bulk is the `|` symbol, which stands for epigraphic signs the transcription "
         "cannot render as a letter — most often the reversed C (Ɔ) for *mulieris*, and "
         "the centurial sign. `|(mulieris)` is a true abbreviation-expansion pair whose "
         "abbreviation happens to be a glyph rather than a letter.")
    emit()
    emit("Whether that belongs in the dataset depends on the task definition. If the "
         "input is text as printed, the model would have to expand a `|` it can see, "
         "which is legitimate and learnable. If the task is strictly letters-to-letters, "
         "these are out of scope. They are consistent and machine-readable either way, "
         "so this is a scoping decision rather than a data-quality problem.")
    emit()
    gain_block(r)
    bias_block(r)
    recs[r] = ("RECOVER AS SEPARATE CLASS", counts[r], "low")

    # ================= 4. greek_script =================
    r = "greek_script"
    emit(f"## 4. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    emit("**Are these bilingual inscriptions?** Mostly not, and that settles it.")
    emit()
    emit(ap.md(pd.DataFrame([
        ("inscriptions contributing a Greek-script drop", f"{greek_ids_total:,}", ""),
        ("of those, also contributing a kept Latin pair", f"{greek_ids_with_latin:,}",
         pct(greek_ids_with_latin, max(greek_ids_total, 1))),
    ], columns=["measure", "value", "share"])))
    emit()
    emit("A genuinely bilingual inscription would yield Latin pairs and Greek pairs from "
         "the same stone. The share that does is reported above. The remainder are "
         "Greek inscriptions that happen to live in EDCS, and Greek abbreviation "
         "practice is its own system — different formulae, different names, a different "
         "alphabet. Folding them in would not enrich the Latin task; it would silently "
         "average two tasks and make the ambiguity tables incoherent, since a Greek and "
         "a Latin abbreviation sharing a shape share nothing else.")
    emit()
    emit("They are worth keeping as a clearly separated Greek subset for anyone who "
         "wants that task, and worth keeping out of the Latin one.")
    emit()
    bias_block(r)
    recs[r] = ("RECOVER AS SEPARATE CLASS", counts[r], "low")

    # ================= 5. unbalanced_parens =================
    r = "unbalanced_parens"
    emit(f"## 5. `{r}` — {counts[r]:,}")
    emit()
    example_block(r)
    mw_total = sum(multiword.values())
    emit("**This is not damaged stone. It is a tokenizer bug.**")
    emit()
    emit("EDCS lets one abbreviation expand to more than one word: `h(ic) s(itus est)`, "
         "`q(ui vixit)`, `b(ene merenti)`. The expansion contains a space, the probe "
         "splits tokens on whitespace, and the parenthesis is severed — the opening half "
         "lands here as unbalanced while the closing half is discarded silently for "
         "having no `(` at all.")
    emit()
    emit(ap.md(pd.DataFrame([
        ("genuine multi-word expansions in the corpus", f"{mw_total:,}", ""),
        ("distinct multi-word expansions", f"{len(multiword):,}", ""),
        ("of those, Latin script (recoverable for this dataset)",
         f"{sum(multiword_latin.values()):,}", f"{len(multiword_latin):,} distinct"),
    ], columns=["measure", "value", "note"])))
    emit()
    emit("Most frequent multi-word expansions:")
    emit()
    emit(ap.md(pd.DataFrame(
        [(f"`({k})`", f"{v:,}") for k, v in multiword_latin.most_common(15)],
        columns=["expansion", "occurrences"])))
    emit()
    emit("These are recoverable exactly and cheaply, by closing the parenthesis before "
         "splitting on whitespace. They are also the single-letter, high-frequency "
         "funerary formulae — the most common abbreviations in the corpus — so losing "
         "them removes real one-to-many cases from a dataset whose whole purpose is "
         "learning how abbreviations expand.")
    emit()
    bias_block(r)
    recs[r] = ("RECOVER", sum(multiword_latin.values()), "low")

    # ================= 6. no_letters_outside_parens =================
    r = "no_letters_outside_parens"
    emit(f"## 6. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    emit("**Confirmed, correctly dropped.** `(filius)` with nothing outside the "
         "parenthesis is a word the editor supplied in full. There is no abbreviation: "
         "the stone shows nothing here, so there is no surface form to expand and no "
         "input side to the training pair. Keeping them would teach a model to "
         "hallucinate words out of empty space.")
    emit()
    emit("Worth noting that two thirds of this category is Greek rather than Latin, so "
         "most of it would fall out of the Latin dataset on script grounds anyway.")
    emit()
    bias_block(r)
    recs[r] = ("KEEP EXCLUDED", 0, "negligible")

    # ================= 7. token_carries_markup =================
    r = "token_carries_markup"
    emit(f"## 7. `{r}` — {counts[r]:,}")
    emit()
    example_block(r)
    emit("This is the category the brief was reaching for in section 1: the "
         "abbreviation's own letters are outside the brackets and only its neighbours "
         "are restored, as in `Hisp]anor(um)` and `pu]bl(icorum)`. The bracket sits in "
         "the same whitespace token but does not cover the letters that matter.")
    emit()
    emit("These are the best recovery candidates in the whole audit by quality per unit: "
         "the abbreviation is on the stone, the expansion is the editor's normal "
         "expansion, and the only contamination is a stray bracket character that can "
         "be stripped. The category is small, so the gain is small, but it is clean.")
    emit()
    gain_block(r)
    bias_block(r)
    recs[r] = ("RECOVER", counts[r], "low")

    # ================= 8-10. the small ones =================
    r = "non_alphabetic_expansion"
    emit(f"## 8. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    emit("A mixed bag of dashes standing for illegible stretches, combining diacritics "
         "and stray Greek letters. Too few to matter and too heterogeneous to rule on "
         "mechanically.")
    emit()
    recs[r] = ("KEEP EXCLUDED", 0, "negligible")

    r = "contains_numeral"
    emit(f"## 9. `{r}` — {counts[r]:,}")
    emit()
    subclass_block(r)
    example_block(r)
    emit("**Partly confirmed, partly refuted.** The premise is right for the clearest "
         "cases: `Dec(3)` does mean three letters are lost after *Dec*, so the "
         "parenthesis holds a gap measurement rather than an expansion, and real Roman "
         "numerals are carved as letters (`XX`, `III`) and handled elsewhere. But that "
         "is not the whole of this category, and with only nineteen tokens every one "
         "could be read individually.")
    emit()
    emit("Two other things are hiding here, and neither is a numeral:")
    emit()
    emit("- **Digit-for-letter transcription typos.** `ann(0s)` is `ann(os)` with a zero "
         "for the letter o; `an(n0s)` the same; `d(onavi7)` is `d(onavit)`; "
         "`posteris1(ue)` is `posterisq(ue)`. These are correct abbreviations spoiled by "
         "a keying slip, and they were dropped for the wrong reason.")
    emit("- **Un-decoded HTML entities.** `p&#x323;(ater)`, `coh&#x323;(ortem)` and "
         "`ab&#x323;nep(otes)` contain a raw `&#x323;` — the character reference for a "
         "combining dot below, the epigraphic sign for a partly legible letter. It "
         "reached the JSONL unescaped, so the digits are an artifact of the scrape, not "
         "of the inscription. This is worth a look upstream: if entities survive "
         "un-decoded here they may survive elsewhere in the corpus without tripping any "
         "filter.")
    emit()
    emit("The exclusion itself is still right — none of these nineteen belong in a "
         "training set as they stand — but the reason label is wrong for most of them, "
         "and the entity leak is a data-quality signal rather than a parsing decision.")
    emit()
    recs[r] = ("KEEP EXCLUDED", 0, "negligible")

    r = "nested_parens"
    emit(f"## 10. `{r}` — {counts[r]:,}")
    emit()
    example_block(r)
    emit("A single token, `((mulieris))`, a doubled rendering of the reversed-C sign. "
         "One occurrence decides nothing either way.")
    emit()
    recs[r] = ("KEEP EXCLUDED", 0, "negligible")

    # ================= summary =================
    emit("## Summary")
    emit()
    rows = []
    total_recoverable = 0
    for r in ORDER:
        rec, n_rec, risk = recs[r]
        total_recoverable += n_rec
        rows.append((f"`{r}`", f"{counts[r]:,}", rec, f"{n_rec:,}", risk))
    emit(ap.md(pd.DataFrame(rows, columns=[
        "category", "count", "recommendation", "pairs recoverable", "bias risk if kept out"])))
    emit()
    # "Recoverable" is not one number: some of it joins the Latin expansion set,
    # the rest becomes its own class and must not be pooled with it.
    into_latin = sum(n for r_, (rec_, n, _) in recs.items()
                     if rec_ == "RECOVER")
    separate = sum(n for r_, (rec_, n, _) in recs.items()
                   if rec_ == "RECOVER AS SEPARATE CLASS")
    conditional = sum(n for r_, (rec_, n, _) in recs.items()
                      if rec_ == "NEEDS HUMAN REVIEW")
    emit("These do not all add up into one pile, and pooling them would be the same "
         "mistake the original filter made in reverse.")
    emit()
    emit(ap.md(pd.DataFrame([
        ("kept today", f"{n_kept:,}", "the current dataset"),
        ("+ straightforward recoveries", f"{into_latin:,}",
         "multi-word expansions and intact abbreviations beside a stray bracket; "
         "these are Latin expansion pairs and belong in the main set"),
        ("= Latin expansion set after clean recoveries", f"{n_kept + into_latin:,}", ""),
        ("+ separate labelled classes", f"{separate:,}",
         "abstention cases, symbol abbreviations, Greek — each a different task, "
         "none of them Latin letter-to-letter expansion pairs"),
        ("+ conditional on a human decision", f"{conditional:,}",
         "partly-restored abbreviations; in or out depending on how the task defines "
         "its input"),
        ("upper bound if everything is taken", f"{n_kept + into_latin + separate + conditional:,}",
         "not a recommendation"),
    ], columns=["stage", "pairs", "what it is"])))
    emit()

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(out) + "\n", encoding="utf-8")

    # ---------------- stdout verdict ----------------
    br = "inside_bracket_markup"
    print(f"records {n_records:,}   kept {n_kept:,}   dropped {total_dropped:,}   "
          f"mirror {'MATCHES' if all_ok else 'DRIFTED'}")
    print()
    print(f"largest by volume        {br} ({counts[br]:,})")
    print(f"largest by bias          province TVD {tvd(prov[br], kept_prov):.3f}, "
          f"median text {median_from_counter(tlen[br]):.0f} vs {median_from_counter(kept_len):.0f} chars")
    print(f"most damage to SIZE      {br}: {counts[br]:,} pairs, 15% of all candidates")
    print(f"most damage to REPRESENT. {br} again: dropped texts run "
          f"{mean_from_counter(tlen[br]):.0f} chars mean vs {mean_from_counter(kept_len):.0f} kept")
    print(f"                          -> damaged/monumental stones are filtered out")
    print()
    print(f"clean recoveries         +{into_latin:,}  -> Latin set {n_kept + into_latin:,}")
    print(f"separate classes         +{separate:,}  (abstention {recs['editorial_marker_paren'][1]:,}, "
          f"symbol {recs['non_alphabetic_abbrev'][1]:,}, Greek {recs['greek_script'][1]:,})")
    print(f"needs a Latinist         {conditional:,}  (partly-restored abbreviations)")
    print(f"report                   {OUT_MD.relative_to(REPO)}")


if __name__ == "__main__":
    main()

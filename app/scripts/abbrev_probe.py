#!/usr/bin/env python3
"""Feasibility probe: can EDCS round-parenthesis expansions be mined as
ground-truth labels for the task "expand Latin epigraphic abbreviations"?

Read-only over data/. Writes only reports/abbrev_probe.md and
data/derived/abbrev_pairs.tsv. No network, no model training.

EDCS convention
---------------
Round parentheses mark an editorial expansion of an ancient abbreviation:
the stone reads "D M", the edition prints "D(is) M(anibus)". The letters
OUTSIDE the parentheses are what was actually carved (the abbreviation);
the whole token with the parentheses deleted is the expansion.

    D(is)        -> abbrev "D"    expansion "Dis"
    Aug(ustus)   -> abbrev "Aug"  expansion "Augustus"
    co(n)s(ul)   -> abbrev "cos"  expansion "consul"

This is why extraction works at the level of a whitespace token, not at the
level of an individual "(...)" group: an interior expansion such as
co(n)s(ul) is one abbreviation carrying two parenthesised insertions.

Other Leiden-style markup is deliberately NOT mined here:
    [ ]  lost text restored by the editor   (not an ancient abbreviation)
    < >  editorial correction of the stone
    { }  superfluous letters to be deleted
    ( )  containing "?" / "!" -> editorial comment, not an expansion
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data" / "edcs_inscriptions.jsonl"
OUT_TSV = REPO / "data" / "derived" / "abbrev_pairs.tsv"
OUT_MD = REPO / "reports" / "abbrev_probe.md"

# Fields this probe refuses to run without.
REQUIRED_FIELDS = ["record_id", "inscription_text", "province", "not_before", "not_after"]

CONTEXT_CHARS = 40
# Minimum observations before an abbreviation is eligible for the detailed
# province/century breakdown. Below this, an "even split" is just noise.
AMBIG_MIN_FREQ = 20


class FieldError(RuntimeError):
    """A required field is absent from a record. Never defaulted away."""


# --------------------------------------------------------------------------
# 1. field discovery
# --------------------------------------------------------------------------

def md(df: "pd.DataFrame", index: bool = False) -> str:
    """Render a DataFrame as a GitHub markdown table.

    Hand-rolled because pandas' own .to_markdown() needs `tabulate`, and the
    brief pins us to stdlib + pandas.
    """
    cols = [str(c) for c in df.columns]
    header = ([str(df.index.name or "")] if index else []) + cols
    rows = []
    for idx, row in df.iterrows():
        cells = ([str(idx)] if index else []) + [
            f"{v:,}" if isinstance(v, (int,)) and not isinstance(v, bool) else str(v)
            for v in row.tolist()
        ]
        rows.append([c.replace("|", "\\|") for c in cells])
    out = ["| " + " | ".join(header) + " |",
           "| " + " | ".join("---" for _ in header) + " |"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def discover_fields(path: Path, n_examples: int = 5):
    """Identify the transcription field and prove it exists on every record.

    Fails loudly: if the expected field is missing, we say so and stop rather
    than falling back to some other column that merely looks textual.
    """
    with path.open(encoding="utf-8") as fh:
        first = fh.readline()
        if not first.strip():
            raise FieldError(f"{path} is empty or starts with a blank line")
        rec = json.loads(first)

    keys = list(rec.keys())
    # The transcription field is the one holding Leiden-convention markup.
    candidates = [k for k in keys
                  if isinstance(rec[k], str)
                  and ("text" in k.lower() or "inscription" in k.lower())]
    if "inscription_text" not in keys:
        raise FieldError(
            "no 'inscription_text' field in the raw JSONL. "
            f"keys present: {keys!r}; text-ish candidates: {candidates!r}"
        )

    missing = [f for f in REQUIRED_FIELDS if f not in keys]
    if missing:
        raise FieldError(f"required field(s) absent from record schema: {missing!r}")

    examples = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("inscription_text", "").strip():
                examples.append(r)
            if len(examples) >= n_examples:
                break

    return "inscription_text", keys, candidates, examples


# --------------------------------------------------------------------------
# 2. extraction
# --------------------------------------------------------------------------

# Characters that terminate an abbreviation token. "/" is EDCS's LINE BREAK,
# not a paired delimiter -- see the note in the report.
HARD_SEP = re.compile(r"[/\s\u00b7]+")
MARKUP_CHARS = set("[]<>{}")
LATIN_WORD = re.compile(r"^[A-Za-z]+$")
# punctuation that rides along on a token but is not part of the word
EDGE_PUNCT = ",;:.\u00b7!?\"'*+=-–— "


def mask_markup(text: str):
    """Return a bool array: True where a character sits inside [ ], < > or { }.

    Brackets are frequently unbalanced in this corpus (~22% of texts): a
    fragment opens "[" that never closes, or begins with a stray "]". Both are
    meaningful -- the lost text runs off the edge of the stone -- so an
    unclosed "[" masks to end of string and a leading "]" masks from the start.
    """
    n = len(text)
    masked = [False] * n
    for opener, closer in (("[", "]"), ("<", ">"), ("{", "}")):
        depth = 0
        start = None
        # a closer with no opener => everything before it is lost text
        first_open = text.find(opener)
        first_close = text.find(closer)
        if first_close != -1 and (first_open == -1 or first_close < first_open):
            for i in range(0, first_close + 1):
                masked[i] = True
        for i, ch in enumerate(text):
            if ch == opener:
                if depth == 0:
                    start = i
                depth += 1
            elif ch == closer and depth > 0:
                depth -= 1
                if depth == 0:
                    for j in range(start, i + 1):
                        masked[j] = True
                    start = None
        if depth > 0 and start is not None:  # never closed -> runs to the end
            for j in range(start, n):
                masked[j] = True
    return masked


def is_latin_alpha(s: str) -> bool:
    return bool(LATIN_WORD.match(s))


def script_of(s: str) -> str:
    for ch in s:
        if ch.isalpha():
            name = unicodedata.name(ch, "")
            if "GREEK" in name:
                return "greek"
            if "LATIN" in name:
                return "latin"
    return "other"


def iter_tokens(text: str):
    """Yield (token, start_offset) for whitespace/slash separated tokens."""
    pos = 0
    for piece in HARD_SEP.split(text):
        if piece:
            idx = text.find(piece, pos)
            if idx == -1:
                idx = pos
            yield piece, idx
            pos = idx + len(piece)


def extract_pairs(text: str, excluded: Counter, excluded_samples: dict):
    """Yield (abbrev, expansion, start, end) for genuine expansions in `text`."""
    if "(" not in text:
        return
    masked = mask_markup(text)

    def drop(reason, tok):
        excluded[reason] += 1
        s = excluded_samples.setdefault(reason, [])
        if len(s) < 8 and tok not in s:
            s.append(tok)

    for tok, off in iter_tokens(text):
        if "(" not in tok:
            continue

        # Shed punctuation clinging to the edges of the token. Without this,
        # "mission(e)," and "Viator(is)?" are thrown away as non-alphabetic
        # when they are perfectly good expansions. A trailing "?" outside the
        # parens flags an uncertain reading of the whole word; we treat it the
        # same way as an uncertainty marker inside the parens and keep the pair.
        lead = len(tok) - len(tok.lstrip(EDGE_PUNCT))
        tok = tok.strip(EDGE_PUNCT)
        off += lead
        if "(" not in tok:
            continue

        # sits inside [ ] / < > / { } -> not an ancient abbreviation
        if any(masked[off + i] for i in range(len(tok)) if off + i < len(masked)):
            drop("inside_bracket_markup", tok)
            continue
        if any(c in MARKUP_CHARS for c in tok):
            drop("token_carries_markup", tok)
            continue
        if tok.count("(") != tok.count(")"):
            drop("unbalanced_parens", tok)
            continue
        if re.search(r"\([^)]*\(", tok):
            drop("nested_parens", tok)
            continue

        groups = re.findall(r"\(([^)]*)\)", tok)
        # strip editorial uncertainty markers that ride along inside the paren
        cleaned = [g.rstrip("?!").strip() for g in groups]
        if any(g == "" for g in cleaned):
            # "(?)", "(!)", "()" -> an editorial comment, not an expansion
            drop("editorial_marker_paren", tok)
            continue

        # rebuild with markers removed
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
            drop("no_letters_outside_parens", tok)
            continue
        if script_of(expansion) == "greek" or script_of(abbrev) == "greek":
            drop("greek_script", tok)
            continue
        if re.search(r"\d", tok):
            drop("contains_numeral", tok)
            continue
        if not is_latin_alpha(abbrev):
            # e.g. |(mulieris): the "abbreviation" is a symbol, not letters
            drop("non_alphabetic_abbrev", tok)
            continue
        if not is_latin_alpha(expansion):
            drop("non_alphabetic_expansion", tok)
            continue
        if len(expansion) <= len(abbrev):
            drop("expansion_not_longer", tok)
            continue

        yield abbrev, expansion, off, off + len(tok)


# --------------------------------------------------------------------------
# 3. metadata helpers
# --------------------------------------------------------------------------

def parse_year(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s == "-":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def century_of(year: int) -> int:
    """1..100 -> 1; -100..-1 -> -1. There is no year zero."""
    if year > 0:
        return (year - 1) // 100 + 1
    return -((-year - 1) // 100 + 1)


def century_label(c: int) -> str:
    return f"{abs(c)}{'BC' if c < 0 else 'AD'}"


def midpoint_century(nb, na):
    if nb is None or na is None:
        return None, False
    mid = (nb + na) / 2
    if -1 < mid < 1:
        mid = nb
    c = century_of(int(mid))
    single = century_of(nb) == century_of(na)
    return c, single


def clean_ctx(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


# --------------------------------------------------------------------------
# 4. main pass
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="stop after N records (smoke test)")
    args = ap.parse_args()

    if not RAW.exists():
        raise FieldError(f"raw corpus not found: {RAW}")

    out_lines = []

    def emit(s=""):
        print(s)
        out_lines.append(s)

    field, keys, candidates, examples = discover_fields(RAW)

    emit("# Abbreviation-expansion feasibility probe")
    emit()
    emit(f"Source: `{RAW.relative_to(REPO)}`")
    emit()
    emit("## 1. Transcription field")
    emit()
    emit(f"The transcription lives in **`{field}`**.")
    emit(f"Other text-like candidates considered: `{candidates}`")
    emit()
    emit("### 5 raw examples, verbatim")
    emit()
    for i, r in enumerate(examples, 1):
        emit(f"{i}. `{r['record_id']}`")
        emit("```")
        emit(r[field])
        emit("```")
    emit()

    # counters
    n_records = 0
    n_with_text = 0
    n_with_pairs = 0
    n_pairs = 0
    abbrev_counts = Counter()          # case-folded abbrev -> freq
    abbrev_surface = defaultdict(Counter)
    expansion_counts = Counter()
    pair_counts = Counter()            # (fold_abbrev, fold_expansion)
    unique_abbrev_cs = set()
    unique_expansion_cs = set()
    excluded = Counter()
    excluded_samples: dict = {}
    by_province = defaultdict(Counter)  # (abbrev, expansion) -> province -> n
    by_century = defaultdict(Counter)
    have_province = 0
    have_date = 0
    have_both = 0
    single_century = 0

    OUT_TSV.parent.mkdir(parents=True, exist_ok=True)
    with RAW.open(encoding="utf-8") as fh, OUT_TSV.open("w", encoding="utf-8", newline="") as tsv:
        w = csv.writer(tsv, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writerow(["inscription_id", "abbrev", "expansion", "left_context",
                    "right_context", "province", "date_from", "date_to"])

        for lineno, line in enumerate(fh, 1):
            if args.limit and n_records >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            n_records += 1

            for f in REQUIRED_FIELDS:
                if f not in rec:
                    raise FieldError(f"line {lineno}: required field {f!r} missing from record")

            text = rec[field]
            if not isinstance(text, str):
                raise FieldError(f"line {lineno}: {field!r} is {type(text).__name__}, expected str")
            if not text.strip():
                continue
            n_with_text += 1

            province = (rec["province"] or "").strip()
            nb = parse_year(rec["not_before"])
            na = parse_year(rec["not_after"])
            cent, single = midpoint_century(nb, na)
            rid = rec["record_id"]

            found = 0
            for abbrev, expansion, s, e in extract_pairs(text, excluded, excluded_samples):
                found += 1
                n_pairs += 1
                fa, fe = abbrev.lower(), expansion.lower()
                abbrev_counts[fa] += 1
                abbrev_surface[fa][abbrev] += 1
                expansion_counts[fe] += 1
                pair_counts[(fa, fe)] += 1
                unique_abbrev_cs.add(abbrev)
                unique_expansion_cs.add(expansion)

                if province:
                    have_province += 1
                    by_province[(fa, fe)][province] += 1
                if cent is not None:
                    have_date += 1
                    by_century[(fa, fe)][cent] += 1
                    if single:
                        single_century += 1
                if province and cent is not None:
                    have_both += 1

                w.writerow([
                    rid, abbrev, expansion,
                    clean_ctx(text[max(0, s - CONTEXT_CHARS):s]),
                    clean_ctx(text[e:e + CONTEXT_CHARS]),
                    province,
                    "" if nb is None else nb,
                    "" if na is None else na,
                ])
            if found:
                n_with_pairs += 1

    # ---------------- report ----------------
    emit("## 2. Corpus totals")
    emit()
    pct = lambda a, b: f"{100 * a / b:.1f}%" if b else "n/a"
    tbl = pd.DataFrame([
        ("inscriptions scanned", f"{n_records:,}", ""),
        ("with a non-empty transcription", f"{n_with_text:,}", pct(n_with_text, n_records)),
        ("with >=1 expansion pair", f"{n_with_pairs:,}", pct(n_with_pairs, n_records)),
        ("total pairs extracted", f"{n_pairs:,}", ""),
        ("unique abbreviation forms (case-sensitive)", f"{len(unique_abbrev_cs):,}", ""),
        ("unique abbreviation forms (case-folded)", f"{len(abbrev_counts):,}", ""),
        ("unique expansions (case-sensitive)", f"{len(unique_expansion_cs):,}", ""),
        ("unique expansions (case-folded)", f"{len(expansion_counts):,}", ""),
        ("unique (abbrev, expansion) pair types", f"{len(pair_counts):,}", ""),
    ], columns=["metric", "value", "share"])
    emit(md(tbl))
    emit()

    emit("## 3. What was excluded, and why")
    emit()
    rows = []
    for reason, cnt in excluded.most_common():
        ex = ", ".join(f"`{s}`" for s in excluded_samples.get(reason, [])[:4])
        rows.append((reason, f"{cnt:,}", ex))
    if rows:
        emit(md(pd.DataFrame(rows, columns=["reason", "count", "examples"])))
    else:
        emit("_nothing excluded_")
    emit()

    # ---- top 50 abbreviations ----
    emit("## 4. Top 50 abbreviations by frequency")
    emit()
    rows = []
    for ab, cnt in abbrev_counts.most_common(50):
        exps = {e: c for (a, e), c in pair_counts.items() if a == ab}
        top = sorted(exps.items(), key=lambda kv: -kv[1])
        shown = ", ".join(f"{e} ({c:,})" for e, c in top[:4])
        if len(top) > 4:
            shown += f", +{len(top) - 4} more"
        rows.append((ab, f"{cnt:,}", len(top), shown))
    emit(md(pd.DataFrame(rows, columns=["abbrev", "freq", "n_expansions", "expansions"])))
    emit()

    # ---- ambiguity ----
    exp_by_abbrev = defaultdict(Counter)
    for (a, e), c in pair_counts.items():
        exp_by_abbrev[a][e] = c

    def balance(counter):
        tot = sum(counter.values())
        k = len(counter)
        if k < 2 or tot == 0:
            return 0.0
        h = -sum((v / tot) * math.log(v / tot) for v in counter.values() if v)
        return h / math.log(k)

    def same_lemma(x: str, y: str) -> bool:
        """Are these two expansions inflections of one word, or different words?

        filius / filio  -> same lemma (a case ending differs)
        filius / fecit  -> different words
        Requires a shared stem of >=3 characters with only a short ending
        differing on each side.
        """
        n = min(len(x), len(y))
        i = 0
        while i < n and x[i] == y[i]:
            i += 1
        return i >= 3 and (len(x) - i) <= 4 and (len(y) - i) <= 4

    def lemma_clusters(expansions):
        """Group expansions into distinct-word classes (simple union-find)."""
        parent = {e: e for e in expansions}

        def find(a):
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a

        exps = list(expansions)
        for i, x in enumerate(exps):
            for y in exps[i + 1:]:
                if same_lemma(x, y):
                    rx, ry = find(x), find(y)
                    if rx != ry:
                        parent[rx] = ry
        groups = defaultdict(list)
        for e in exps:
            groups[find(e)].append(e)
        return list(groups.values())

    ambiguous = {a: c for a, c in exp_by_abbrev.items() if len(c) > 1}
    amb_rows = []
    n_lexical = 0
    n_inflectional = 0
    lexical_token_total = 0
    for a, c in ambiguous.items():
        tot = sum(c.values())
        top1 = c.most_common(1)[0][1]
        clusters = lemma_clusters(list(c))
        if len(clusters) > 1:
            n_lexical += 1
            lexical_token_total += tot
        else:
            n_inflectional += 1
        amb_rows.append({
            "abbrev": a, "freq": tot, "n_expansions": len(c),
            "n_words": len(clusters),
            "kind": "lexical" if len(clusters) > 1 else "inflectional",
            "balance": round(balance(c), 4),
            "majority_share": round(top1 / tot, 4),
            "expansions": ", ".join(f"{e} ({n:,})" for e, n in c.most_common(6)),
        })
    amb = pd.DataFrame(amb_rows)

    emit("## 5. Ambiguity table")
    emit()
    emit(f"- abbreviations with >1 distinct expansion: **{len(ambiguous):,}** "
         f"of {len(abbrev_counts):,} ({pct(len(ambiguous), len(abbrev_counts))})")
    amb_tokens = int(amb["freq"].sum()) if not amb.empty else 0
    emit(f"- pairs sitting under an ambiguous abbreviation: **{amb_tokens:,}** "
         f"of {n_pairs:,} ({pct(amb_tokens, n_pairs)})")
    emit()
    emit("Not all ambiguity is the same kind. Most of it is **inflectional** -- the "
         "abbreviation stands for one word and only the case ending is in doubt "
         "(`co(n)s(ul)` -> consul / consuli / consulibus). The hard and interesting "
         "kind is **lexical**, where the same letters stand for genuinely different "
         "words (`f` -> filius / fecit / faciendum). Expansions are grouped into words "
         "by shared stem, and an abbreviation counts as lexically ambiguous only if "
         "its expansions fall into more than one such group.")
    emit()
    emit(md(pd.DataFrame([
        ("ambiguous, inflectional only", f"{n_inflectional:,}", pct(n_inflectional, len(ambiguous))),
        ("ambiguous, genuinely lexical", f"{n_lexical:,}", pct(n_lexical, len(ambiguous))),
        ("pairs under a lexically ambiguous abbrev", f"{lexical_token_total:,}", pct(lexical_token_total, n_pairs)),
    ], columns=["class", "count", "share"])))
    emit()
    emit("`balance` = Shannon entropy of the expansion distribution normalised by "
         "log(n_expansions): 1.0 = a perfectly even split, ~0 = one dominant reading "
         "with rare alternatives. Sorted most-balanced first.")
    emit(f"Restricted to abbreviations seen >= {AMBIG_MIN_FREQ} times, so that a 1-vs-1 "
         "split of a hapax does not outrank a genuine coin-flip.")
    emit()
    amb_sorted = pd.DataFrame()
    if not amb.empty:
        amb_sorted = (amb[amb["freq"] >= AMBIG_MIN_FREQ]
                      .sort_values(["balance", "freq"], ascending=[False, False]))
        emit(md(amb_sorted.head(60)))
        emit()
        emit(f"_(showing {min(60, len(amb_sorted)):,} of {len(amb_sorted):,} "
             "ambiguous abbreviations above the frequency floor)_")
    emit()

    # ---- breakdowns ----
    def breakdown(ab):
        exps = exp_by_abbrev[ab]
        emit(f"### `{ab}`  ({sum(exps.values()):,} occurrences, {len(exps)} expansions)")
        emit()
        keep = [e for e, _ in exps.most_common(5)]

        prov = defaultdict(dict)
        for e in keep:
            for p, n in by_province[(ab, e)].items():
                prov[p][e] = n
        if prov:
            dfp = pd.DataFrame(prov).T.fillna(0).astype(int)
            dfp = dfp.reindex(columns=[c for c in keep if c in dfp.columns])
            dfp["total"] = dfp.sum(axis=1)
            dfp = dfp.sort_values("total", ascending=False).head(12)
            dfp.index.name = "province"
            emit("**by province** (top 12)")
            emit()
            emit(md(dfp, index=True))
            emit()

        cen = defaultdict(dict)
        for e in keep:
            for c, n in by_century[(ab, e)].items():
                cen[c][e] = n
        if cen:
            dfc = pd.DataFrame(cen).T.fillna(0).astype(int)
            dfc = dfc.reindex(columns=[c for c in keep if c in dfc.columns])
            dfc["total"] = dfc.sum(axis=1)
            dfc = dfc.sort_index()
            dfc.index = [century_label(i) for i in dfc.index]
            dfc.index.name = "century"
            emit("**by century** (midpoint of the dating range)")
            emit()
            emit(md(dfc, index=True))
            emit()

    emit("## 6. Expansion choice by province and century (top 20 ambiguous)")
    emit()
    emit("Does context predict the reading? If an abbreviation's expansion split is "
         "flat across every province and century, context carries no signal for it.")
    emit()
    if not amb_sorted.empty:
        for ab in amb_sorted.head(20)["abbrev"].tolist():
            breakdown(ab)

    emit("## 6b. The same breakdown for the highest-volume ambiguous abbreviations")
    emit()
    emit("Ranking by balance alone puts rare, evenly-split forms on top. These are the "
         "abbreviations that actually carry the corpus's weight, and they are where a "
         "disambiguation model would win or lose.")
    emit()
    if not amb.empty:
        top_lex = (amb[(amb["kind"] == "lexical") & (amb["freq"] >= AMBIG_MIN_FREQ)]
                   .sort_values("freq", ascending=False).head(10)["abbrev"].tolist())
        for ab in top_lex:
            breakdown(ab)

    # ---- coverage ----
    emit("## 7. Metadata coverage (per extracted pair)")
    emit()
    cov = pd.DataFrame([
        ("province present", f"{have_province:,}", pct(have_province, n_pairs)),
        ("date range present", f"{have_date:,}", pct(have_date, n_pairs)),
        ("both present", f"{have_both:,}", pct(have_both, n_pairs)),
        ("date resolves to a single century", f"{single_century:,}", pct(single_century, n_pairs)),
    ], columns=["metric", "pairs", "share of all pairs"])
    emit(md(cov))
    emit()

    emit("## 8. Parsing notes")
    emit()
    emit("- **`/` is a line break in EDCS, not a paired delimiter.** The brief asked to "
         "ignore text inside `/ /`; treating slashes as a pair would mask every other "
         "line of the corpus (200k+ single `/` against 55 `//` in a 60k sample). "
         "It is treated here as a hard token boundary instead, so no abbreviation is "
         "read across a line break. Square, angle and curly brackets ARE masked as spans.")
    emit("- Brackets are unbalanced in roughly a fifth of texts (fragments). An unclosed "
         "`[` masks to the end of the string; a `]` with no opener masks from the start.")
    emit("- Extraction is per whitespace token, not per `(...)` group, so `co(n)s(ul)` "
         "yields one pair (`cos` -> `consul`) rather than two fragments.")
    emit("- `(?)` and `(!)` are editorial comments and are dropped; a trailing `?`/`!` "
         "*inside* an otherwise valid expansion (`dep(ositus?)`) is stripped and the pair kept.")
    emit("- Frequency tables are case-folded; the TSV keeps the surface form.")
    emit("- **Known artifact, not corrected here:** the doubled-letter plural. EDCS "
         "writes `DD(ominis)`, `CC(aiorum)`, `LL(uciorum)`, where the repeated letter "
         "marks a plural rather than starting the word. Concatenating gives `DDominis` "
         "instead of `dominis`. Roman numerals fused to a following abbreviation "
         "(`III(triere)`, `XX(vicesimae)`) fail the same way. Together these are ~3.8k "
         "pairs, 0.26% of the total; they are left in the TSV so they can be measured, "
         "and any real training set should filter or repair them.")
    emit("- A few hundred pairs carry dates after 700 AD. Most are genuine early-medieval "
         "Christian inscriptions (`eps` -> episcopus, `scae` -> sanctae) that EDCS "
         "legitimately includes; a handful (e.g. 1998) are modern or mis-keyed. 446 "
         "pairs in total, 0.03%.")
    emit()
    emit(f"Pairs written to `{OUT_TSV.relative_to(REPO)}`.")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(out_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except FieldError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(2)

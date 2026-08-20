#!/usr/bin/env python3
"""Phase 3b evidence base: what actually follows a Roman-numeral prefix.

Before any word list is written, enumerate every parenthesis content that
appears after an all-Roman-numeral prefix, with counts, split by whether the
prefix is one character (where 271,012 ordinary abbreviations D/M/C/L/I/V/X
live) or two or more (where the prefix cannot be a praenomen initial).

No classification here. Evidence only.
"""
from __future__ import annotations
import csv, re
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
ROMAN = re.compile(r"^[IVXLCDM]+$")

multi = Counter(); single = Counter()
multi_pairs = defaultdict(Counter); single_pairs = defaultdict(Counter)
exp_freq = Counter(); abbrev_freq = Counter()

with (REPO/"data"/"derived"/"abbrev_pairs.tsv").open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE); next(rd)
    for r in rd:
        ab, ex = r[1], r[2]
        exp_freq[ex.lower()] += 1
        abbrev_freq[ab.lower()] += 1
        if not ROMAN.match(ab): continue
        if not ex.startswith(ab): continue
        rest = ex[len(ab):]
        if not rest: continue
        if len(ab) == 1:
            single[rest.lower()] += 1; single_pairs[rest.lower()][f"{ab}({rest})"] += 1
        else:
            multi[rest.lower()] += 1; multi_pairs[rest.lower()][f"{ab}({rest})"] += 1

print("=== MULTI-CHARACTER numeral prefix (II, III, XX, XL ...) ===")
print(f"{len(multi)} distinct paren contents, {sum(multi.values()):,} pairs\n")
print(f"{'paren content':<22}{'pairs':>8}{'indep. attest':>14}  top surface forms")
for w, n in multi.most_common(45):
    ind = exp_freq.get(w, 0)
    forms = ", ".join(f"{k}×{v}" for k, v in multi_pairs[w].most_common(3))
    print(f"{w:<22}{n:>8,}{ind:>14,}  {forms}")

print(f"\n\n=== SINGLE-CHARACTER numeral prefix (I V X L C D M) ===")
print(f"{len(single)} distinct paren contents, {sum(single.values()):,} pairs")
print("(this pool contains every ordinary praenomen/formula abbreviation)\n")
print(f"{'paren content':<22}{'pairs':>8}{'indep. attest':>14}  top surface forms")
for w, n in single.most_common(60):
    ind = exp_freq.get(w, 0)
    forms = ", ".join(f"{k}×{v}" for k, v in single_pairs[w].most_common(3))
    print(f"{w:<22}{n:>8,}{ind:>14,}  {forms}")

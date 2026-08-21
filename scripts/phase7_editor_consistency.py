#!/usr/bin/env python3
"""Editorial inconsistency: how much of the gold label is the editor?

The expansions are editorial interpretations, not attested text. That is the
reviewer's sharpest attack on this dataset. It is also measurable, three ways,
without a model:

  A. IDENTICAL CONTEXT, DIFFERENT ANSWER. The same abbreviation, with the same
     40 characters either side, expanded differently somewhere else in the
     corpus. Same evidence, different call.

  B. SAME MONUMENT, DIFFERENT ANSWER. D-0004 established that 45,655 records
     are segments of 30,503 monuments. If two faces of one stone expand the
     same abbreviation differently, no property of the stone explains it.

  C. THE ABBREVIATION BOUNDARY ITSELF IS EDITORIAL. In EDCS-00000245 one
     segment prints "Parthici nepos" and another prints "Parthic(i) nep(os)" --
     the same titulature, with the editor choosing in one case that the stone
     abbreviates and in the other that it does not. Measured corpus-wide: how
     often does a word appear BOTH as plain text and as an expansion?

Read-only. Uses the frozen v1 table.
"""
from __future__ import annotations
import csv, json, re
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
V1 = REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv"
RAW = REPO/"data"/"edcs_inscriptions.jsonl"
OUT = REPO/"results"/"editor_consistency.json"
csv.field_size_limit(10**9)

ctx_exp = defaultdict(Counter)     # (abbrev, left, right) -> expansions
mon_exp = defaultdict(Counter)     # (base_id, abbrev)     -> expansions
exp_forms = Counter()
with V1.open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
    h = next(rd); I = {c: i for i, c in enumerate(h)}
    for r in rd:
        if r[I["excluded_reason"]]: continue
        ab = r[I["abbrev"]]; ex = r[I["corrected_expansion"]].lower()
        ctx_exp[(ab, r[I["left_context"]], r[I["right_context"]])][ex] += 1
        mon_exp[(r[0].rsplit("-", 1)[0], ab)][ex] += 1
        exp_forms[ex] += 1

# ---- A: identical context, different answer -------------------------------
A_groups = {k: v for k, v in ctx_exp.items() if len(v) > 1}
A_rows = sum(sum(v.values()) for v in A_groups.values())
A_ex = sorted(A_groups.items(), key=lambda x: -sum(x[1].values()))[:15]

# ---- B: same monument, different answer -----------------------------------
B_groups = {k: v for k, v in mon_exp.items() if len(v) > 1}
B_rows = sum(sum(v.values()) for v in B_groups.values())
B_ex = sorted(B_groups.items(), key=lambda x: -sum(x[1].values()))[:15]

# ---- C: the abbreviation boundary is itself an editorial choice -----------
# A word that EDCS sometimes prints plain and sometimes prints as an expansion.
WORD = re.compile(r"[A-Za-z]{3,}")
plain = Counter()
for line in RAW.open(encoding="utf-8"):
    t = json.loads(line).get("inscription_text") or ""
    if not t: continue
    stripped = re.sub(r"\([^)]*\)", "", t)         # remove expansions entirely
    for w in WORD.findall(stripped):
        plain[w.lower()] += 1
both = {}
for e, n in exp_forms.items():
    if len(e) >= 3 and plain.get(e, 0) > 0:
        both[e] = {"as_expansion": n, "as_plain_text": plain[e]}
both_top = dict(sorted(both.items(), key=lambda x: -(x[1]["as_expansion"]))[:25])

res = {
  "A_identical_context_different_expansion": {
    "context_groups_with_disagreement": len(A_groups),
    "rows_involved": A_rows,
    "share_of_task_rows": round(A_rows/sum(sum(v.values()) for v in ctx_exp.values()), 5),
    "examples": [{"abbrev": k[0], "left": k[1][-45:], "right": k[2][:45],
                  "expansions": dict(v)} for k, v in A_ex],
  },
  "B_same_monument_different_expansion": {
    "monument_abbrev_groups_with_disagreement": len(B_groups),
    "rows_involved": B_rows,
    "examples": [{"monument": k[0], "abbrev": k[1], "expansions": dict(v)} for k, v in B_ex],
  },
  "C_boundary_is_editorial": {
    "expansion_forms_also_printed_plain": len(both),
    "distinct_expansion_forms": len(exp_forms),
    "share": round(len(both)/len(exp_forms), 4),
    "examples": both_top,
  },
}
OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")

print("A. IDENTICAL CONTEXT, DIFFERENT ANSWER")
print(f"   context groups where editors disagree : {len(A_groups):,}")
print(f"   rows involved                         : {A_rows:,} ({100*res['A_identical_context_different_expansion']['share_of_task_rows']:.2f}%)")
for k, v in A_ex[:6]:
    print(f"     {k[0]:<10} -> {dict(v)}")
print()
print("B. SAME MONUMENT, DIFFERENT ANSWER")
print(f"   (monument, abbrev) pairs that disagree : {len(B_groups):,}")
print(f"   rows involved                          : {B_rows:,}")
for k, v in B_ex[:6]:
    print(f"     {k[0]} {k[1]:<8} -> {dict(v)}")
print()
print("C. THE ABBREVIATION BOUNDARY IS ITSELF EDITORIAL")
print(f"   expansion forms EDCS also prints as plain text: {len(both):,} of {len(exp_forms):,}"
      f" ({100*res['C_boundary_is_editorial']['share']:.1f}%)")
for e, d in list(both_top.items())[:10]:
    print(f"     {e:<16} as expansion {d['as_expansion']:>7,}   as plain text {d['as_plain_text']:>7,}")
print(f"\nwritten {OUT}")

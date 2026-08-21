#!/usr/bin/env python3
"""Phase 2c: how the vinculum's replacement device currently sits in the
kept dataset, and separating the two |-devices that Phase 2b conflated.

Bounded to the vinculum question. The full three-class numeral taxonomy is
Phase 3b's; this establishes only that the device exists in the kept set and
how large it is, because that is what makes the Phase 2 negative consequential.
"""
from __future__ import annotations
import csv, json, re
from collections import Counter
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
TSV = REPO/"data"/"derived"/"abbrev_pairs.tsv"
ROMAN = re.compile(r"^[IVXLCDM]+$")

exp_freq = Counter()
rows = []
with TSV.open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE); next(rd)
    for r in rd:
        exp_freq[r[2].lower()] += 1
        rows.append((r[1], r[2]))

# A fused numeral pair: abbreviation is entirely Roman-numeral characters and
# the expansion is that same string with a COMPLETE attested word appended.
# "attested" = the remainder occurs >=20 times in its own right as an
# expansion elsewhere in the kept set. Stated rule, no Latin judgement.
fused = Counter(); fused_word = Counter(); nonfused_numeric = Counter()
for ab, ex in rows:
    if not ROMAN.match(ab):
        continue
    if not ex.startswith(ab) or len(ex) == len(ab):
        continue
    rest = ex[len(ab):]
    if exp_freq.get(rest.lower(), 0) >= 20:
        fused[(ab, ex)] += 1
        fused_word[rest.lower()] += 1
    else:
        nonfused_numeric[(ab, ex)] += 1

thous = {w for w in fused_word if w.startswith(("mili", "mille", "milli", "milib"))}
n_thous = sum(n for w, n in fused_word.items() if w in thous)

# separate the two |-devices in the raw text
bare_ctx = Counter(); pipe_ctx = Counter()
BARE = re.compile(r"(?<![A-Za-z\)])([IVXLCDM]+)\((mili[a-z]*|mille|milli[a-z]*)\)")
PIPE = re.compile(r"(\|+)\((mili[a-z]*|mille|milli[a-z]*)\)")
def rv(s):
    v={"I":1,"V":5,"X":10,"L":50,"C":100,"D":500,"M":1000}; t=0; p=0
    for c in reversed(s.upper()):
        x=v.get(c,0); t += -x if x<p else x; p=max(p,x)
    return t
bare_num = Counter(); pipe_pre = Counter(); hs_prefixed = 0; bare_total = 0
for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    t = json.loads(line).get("inscription_text") or ""
    if "mil" not in t and "Mil" not in t: continue
    for m in BARE.finditer(t):
        bare_total += 1
        bare_num[m.group(1)] += 1
        pre = t[:m.start()].rstrip().split()
        if pre and pre[-1].strip(",.;:") in ("HS", "hs"): hs_prefixed += 1
    for m in PIPE.finditer(t):
        pre = t[:m.start()].rstrip().split()
        pipe_pre[pre[-1].strip(",.;:") if pre else "<start>"] += 1

out = {
  "fused_numeral_pairs_in_kept_set": sum(fused.values()),
  "fused_numeral_pair_types": len(fused),
  "fused_appended_words": dict(fused_word.most_common(40)),
  "thousands_family_fused_pairs": n_thous,
  "top_fused_pairs": {f"{a} -> {e}": n for (a, e), n in fused.most_common(30)},
  "numeric_abbrev_not_matching_rule": sum(nonfused_numeric.values()),
  "bare_numeral_milia_occurrences": bare_total,
  "bare_numeral_milia_preceded_by_HS": hs_prefixed,
  "bare_numeral_values": {k: {"n": v, "value": rv(k)} for k, v in bare_num.most_common(25)},
  "pipe_milia_preceding_token": dict(pipe_pre.most_common(25)),
}
p = REPO/"data"/"derived"/"phase2_numeral_fusion.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

print(f"FUSED numeral pairs currently in the kept 1,424,314 : {sum(fused.values()):,}  ({len(fused)} types)")
print(f"  of which the thousands family (mil-)               : {n_thous:,}")
print(f"  numeric-abbrev pairs not matching the rule         : {sum(nonfused_numeric.values()):,}")
print("\ntop appended words:")
for w, n in fused_word.most_common(18): print(f"   {w:<16}{n:>6,}")
print("\ntop fused pairs:")
for (a, e), n in fused.most_common(18): print(f"   {a:<8} -> {e:<20}{n:>6,}")
print(f"\n--- device A: bare  N(milia)   {bare_total:,} occurrences ---")
print(f"    preceded by HS (sesterces): {hs_prefixed:,}  ({100*hs_prefixed/bare_total:.1f}%)")
print("    numeral, count, value:")
for k, v in bare_num.most_common(14): print(f"      {k:<8}{v:>5,}   = {rv(k):,}")
print(f"\n--- device B: pipe |(miliaria)  {sum(pipe_pre.values()):,} occurrences ---")
print("    token immediately before:")
for k, v in pipe_pre.most_common(14): print(f"      {k:<24}{v:>5,}")
print("\nwritten", p)

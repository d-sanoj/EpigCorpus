#!/usr/bin/env python3
"""3e. Before/after diff of every headline figure.

Three views of the same data, so the effect of each correction is visible in
both directions (3a requires BOTH keys be kept):

  V0  original abbrev  -> original expansion          (the current release)
  V1  original abbrev  -> corrected expansion         (corrections only)
  V1c collapsed abbrev -> corrected expansion         (keys merged too)

Collapsing dd->d enlarges the expansion set of the highest-frequency keys.
That is quantified here rather than asserted.
"""
from __future__ import annotations
import csv, json
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
V1 = REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv"

def lcp(a, b):
    n = 0
    for x, y in zip(a, b):
        if x != y: break
        n += 1
    return n

def inflectional(a, b):
    """Two expansions are inflectional variants of one lexeme if they share a
    long common prefix. Stated rule: LCP >= 3 chars AND >= 60% of the shorter
    form. Otherwise they are treated as different words (lexical ambiguity).
    This is a morphological proxy, not a lemmatiser. [VERIFY -- LATINIST]"""
    if a == b: return True
    n = lcp(a, b)
    return n >= 3 and n >= 0.6*min(len(a), len(b))

def lexical_ambiguity(exps):
    """True if the expansion set contains at least two forms that are NOT
    inflectional variants of each other."""
    e = sorted(exps)
    for i in range(len(e)):
        for j in range(i+1, len(e)):
            if not inflectional(e[i], e[j]):
                return True
    return False

views = {"V0": (1, 2), "V1": (1, 9), "V1c": (8, 9)}
maps = {k: defaultdict(Counter) for k in views}
n_rows = 0
excluded = 0
with V1.open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
    h = next(rd); IDX = {c: i for i, c in enumerate(h)}
    for r in rd:
        n_rows += 1
        if r[IDX["excluded_reason"]]:
            excluded += 1
        for name, (ai, ei) in views.items():
            maps[name][r[ai].lower()][r[ei].lower()] += 1

res = {"rows": n_rows, "rows_flagged_excluded": excluded}
for name, m in maps.items():
    pairs = sum(sum(c.values()) for c in m.values())
    uniq_keys = len(m)
    uniq_types = sum(len(c) for c in m.values())
    ambig = {k: c for k, c in m.items() if len(c) > 1}
    # ambiguity restricted to keys with enough evidence to matter
    ambig20 = {k: c for k, c in ambig.items() if sum(c.values()) >= 20}
    lex = {k for k, c in ambig20.items() if lexical_ambiguity(list(c))}
    res[name] = {
        "pairs": pairs,
        "unique_abbrev_keys": uniq_keys,
        "unique_pair_types": uniq_types,
        "ambiguous_keys": len(ambig),
        "ambiguous_keys_n>=20": len(ambig20),
        "lexically_ambiguous_keys_n>=20": len(lex),
        "lexical_share_of_ambiguous": round(len(lex)/len(ambig20), 4) if ambig20 else None,
        "mean_expansions_per_key": round(uniq_types/uniq_keys, 3),
        "pairs_on_ambiguous_keys": sum(sum(c.values()) for c in ambig.values()),
    }

# top-50 table, all three views side by side
top = maps["V0"]
top50 = sorted(top, key=lambda k: -sum(top[k].values()))[:50]
tbl = []
for k in top50:
    v0 = maps["V0"][k]; v1 = maps["V1"][k]; v1c = maps["V1c"].get(k, Counter())
    tbl.append({
        "abbrev": k,
        "pairs": sum(v0.values()),
        "V0_expansions": len(v0),
        "V1_expansions": len(v1),
        "V1c_pairs": sum(v1c.values()),
        "V1c_expansions": len(v1c),
        "V0_top": v0.most_common(1)[0][0],
        "V1c_top": v1c.most_common(1)[0][0] if v1c else "",
    })
res["top50"] = tbl

# which keys gained the most expansions when keys were merged
gain = []
for k, c in maps["V1c"].items():
    before = len(maps["V1"].get(k, {}))
    if len(c) > before:
        gain.append((k, before, len(c), sum(c.values())))
gain.sort(key=lambda x: -(x[2]-x[1]))
res["keys_gaining_expansions_from_collapse"] = [
    {"key": k, "before": b, "after": a, "pairs": n} for k, b, a, n in gain[:30]]
res["n_keys_gaining"] = len(gain)

(REPO/"data"/"derived"/"v1"/"phase3_diff.json").write_text(
    json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")

print(f"rows {n_rows:,}   flagged excluded {excluded:,}\n")
print(f"{'measure':<36}{'V0':>14}{'V1':>14}{'V1c':>14}")
for f in ["pairs","unique_abbrev_keys","unique_pair_types","ambiguous_keys",
          "ambiguous_keys_n>=20","lexically_ambiguous_keys_n>=20",
          "lexical_share_of_ambiguous","mean_expansions_per_key",
          "pairs_on_ambiguous_keys"]:
    a,b,c = res["V0"][f], res["V1"][f], res["V1c"][f]
    fmt = lambda x: f"{x:,}" if isinstance(x,int) else f"{x}"
    print(f"{f:<36}{fmt(a):>14}{fmt(b):>14}{fmt(c):>14}")
print(f"\nkeys gaining expansions from collapse: {len(gain):,}")
for k,b,a,n in gain[:12]: print(f"   {k:<10} {b} -> {a} expansions   ({n:,} pairs)")
print("\ntop-10 of the top-50 table:")
print(f"{'abbrev':<8}{'pairs':>10}{'V0 exp':>8}{'V1 exp':>8}{'V1c pairs':>11}{'V1c exp':>9}  V0 top -> V1c top")
for r in tbl[:10]:
    print(f"{r['abbrev']:<8}{r['pairs']:>10,}{r['V0_expansions']:>8}{r['V1_expansions']:>8}"
          f"{r['V1c_pairs']:>11,}{r['V1c_expansions']:>9}  {r['V0_top']} -> {r['V1c_top']}")

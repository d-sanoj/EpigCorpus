#!/usr/bin/env python3
"""4b evidence: choose the three held-out provinces by stated criteria.

Criteria the brief names -- size, geographic spread, genre mix -- made
measurable before any province is picked:
  size          share of task pairs, and of monuments
  genre regime  the dominant reading of the top ambiguous form V, which
                Phase 3f showed separates funerary / votive / military
  spread        Roman geographic zone
"""
from __future__ import annotations
import csv
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
V1 = REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv"

pairs = Counter(); stones = defaultdict(set); vdist = defaultdict(Counter)
keys = defaultdict(set); cent = defaultdict(Counter)
with V1.open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE); h = next(rd)
    I = {c: i for i, c in enumerate(h)}
    for r in rd:
        if r[I["excluded_reason"]]: continue
        p = r[I["province"]] or "UNKNOWN"
        pairs[p] += 1
        stones[p].add(r[0].rsplit("-", 1)[0])
        keys[p].add(r[I["abbrev_collapsed"]].lower())
        if r[I["abbrev_collapsed"]].lower() == "v":
            vdist[p][r[I["corrected_expansion"]].lower()] += 1

tot = sum(pairs.values())
print(f"{'province':<36}{'pairs':>10}{'share':>8}{'stones':>9}{'keys':>8}   V-regime (top 2)")
for p, n in pairs.most_common(30):
    v = vdist[p]; vt = sum(v.values())
    reg = ", ".join(f"{w} {100*c/vt:.0f}%" for w, c in v.most_common(2)) if vt >= 100 else f"(V n={vt})"
    print(f"{p:<36}{n:>10,}{100*n/tot:>7.2f}%{len(stones[p]):>9,}{len(keys[p]):>8,}   {reg}")

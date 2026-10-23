#!/usr/bin/env python3
"""3f self-adversarial: the permutation null in phase3f_signal.py shuffles at
the PAIR level. Two v(ixit) from the same stone are not independent draws, so
that null is too easy and overstates the association.

Re-run the null as a BLOCK permutation: shuffle the conditioning label among
INSCRIPTIONS, keeping all pairs from one stone together. Also print the actual
expansion distributions, because NMI says an association exists, not what it is.
"""
from __future__ import annotations
import csv, importlib.util, json, math, random
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
SEED = 20260820; NPERM = 200
def _load(n,p):
    s=importlib.util.spec_from_file_location(n,p); m=importlib.util.module_from_spec(s)
    s.loader.exec_module(m); return m
ap=_load("abbrev_probe", REPO/"scripts"/"abbrev_probe.py")

def nmi(xs, ys):
    n=len(xs)
    cx=Counter(xs); cy=Counter(ys); cxy=Counter(zip(xs,ys))
    hx=-sum(v/n*math.log(v/n) for v in cx.values())
    hy=-sum(v/n*math.log(v/n) for v in cy.values())
    if hx==0 or hy==0: return 0.0
    mi=sum((v/n)*math.log((v/n)/((cx[a]/n)*(cy[b]/n))) for (a,b),v in cxy.items())
    return mi/min(hx,hy)

def century_of(a,b):
    ya,yb=ap.parse_year(a),ap.parse_year(b)
    if ya is None or yb is None: return None
    c,s=ap.midpoint_century(ya,yb)
    return ap.century_label(c) if (c is not None and s) else None

rows=defaultdict(list)   # abbrev -> [(base_id, expansion, province, century)]
with (REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv").open(encoding="utf-8",newline="") as fh:
    rd=csv.reader(fh,delimiter="\t",quoting=csv.QUOTE_NONE); h=next(rd)
    I={c:i for i,c in enumerate(h)}
    for r in rd:
        if r[I["excluded_reason"]]: continue
        ab=r[I["abbrev_collapsed"]].lower()
        if ab not in ("v","c","a","l","aug"): continue
        base=r[0].rsplit("-",1)[0]
        rows[ab].append((base, r[I["corrected_expansion"]].lower(),
                         r[I["province"]] or None, century_of(r[I["date_from"]],r[I["date_to"]])))

res={"seed":SEED,"permutations":NPERM}
print(f"{'abbrev|cond':<14}{'n pairs':>9}{'n stones':>9}{'nmi':>9}"
      f"{'pair-null p95':>15}{'BLOCK-null p95':>16}{'ratio':>8}")
for ab in ("v","c","a","l","aug"):
    for ci,cond in ((2,"province"),(3,"century")):
        data=[(x[0],x[1],x[ci]) for x in rows[ab] if x[ci]]
        if len(data)<200: continue
        ex=[d[1] for d in data]; cd=[d[2] for d in data]
        obs=nmi(ex,cd)
        rng=random.Random(SEED)
        pair_null=sorted(nmi(ex,random.Random(SEED+i).sample(cd,len(cd))) for i in range(NPERM))
        # block permutation: one label per inscription, reassigned wholesale
        by_stone=defaultdict(list)
        for i,(b,_,_) in enumerate(data): by_stone[b].append(i)
        stones=list(by_stone); stone_label={b:data[by_stone[b][0]][2] for b in stones}
        block_null=[]
        for k in range(NPERM):
            r2=random.Random(SEED+k)
            labs=[stone_label[b] for b in stones]; r2.shuffle(labs)
            sh=[None]*len(data)
            for b,l in zip(stones,labs):
                for i in by_stone[b]: sh[i]=l
            block_null.append(nmi(ex,sh))
        block_null.sort()
        bp95=block_null[int(0.95*NPERM)]
        print(f"{ab+'|'+cond:<14}{len(data):>9,}{len(stones):>9,}{obs:>9.4f}"
              f"{pair_null[int(0.95*NPERM)]:>15.4f}{bp95:>16.4f}{obs/bp95 if bp95 else 0:>8.1f}")
        res[f"{ab}|{cond}"]={"n_pairs":len(data),"n_stones":len(stones),"nmi":round(obs,5),
            "pair_null_p95":round(pair_null[int(0.95*NPERM)],5),
            "block_null_p95":round(bp95,5),"ratio_block":round(obs/bp95,2) if bp95 else None}

print("\n=== WHAT the association actually is ===")
for ab,cond,ci in (("v","province",2),("c","century",3)):
    print(f"\n--- {ab.upper()} by {cond} ---")
    g=defaultdict(Counter)
    for b,e,p,c in rows[ab]:
        k=(p if ci==2 else c)
        if k: g[k][e]+=1
    order=sorted(g,key=lambda k:-sum(g[k].values()))
    for k in order[:10]:
        tot=sum(g[k].values())
        top=", ".join(f"{w} {100*n/tot:.0f}%" for w,n in g[k].most_common(3))
        print(f"  {k:<34}n={tot:>7,}   {top}")
    res[f"dist_{ab}_{cond}"]={k:dict(g[k].most_common(5)) for k in order[:12]}

(REPO/"data"/"derived"/"v1"/"phase3f_blockperm.json").write_text(
    json.dumps(res,ensure_ascii=False,indent=1),encoding="utf-8")

#!/usr/bin/env python3
"""Phase 1 supplement, part 3: aggregate bias across ALL exclusions pooled.

The prior audit tests each category against the kept set separately. The
dataset a user receives is shaped by the union of all ten filters, and the
union is what a reviewer will ask about. Province, century and inscription
length, pooled, with a bootstrap null for scale.
"""
from __future__ import annotations
import importlib.util, json, random, re, statistics
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEED = 20260820; BOOT = 200
def load_probe():
    p = Path(__file__).resolve().parent/"abbrev_probe.py"
    s = importlib.util.spec_from_file_location("abbrev_probe", p)
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
ap = load_probe()

def is_dropped(tok, o, masked):
    """True if abbrev_probe would drop this token, for any reason."""
    if any(masked[o+i] for i in range(len(tok)) if o+i < len(masked)): return True
    if any(c in ap.MARKUP_CHARS for c in tok): return True
    if tok.count("(") != tok.count(")"): return True
    if re.search(r"\([^)]*\(", tok): return True
    g = re.findall(r"\(([^)]*)\)", tok)
    cl = [x.rstrip("?!").strip() for x in g]
    if any(x == "" for x in cl): return True
    i = 0
    def _s(_m):
        nonlocal i
        v = cl[i]; i += 1; return "("+v+")"
    nm = re.sub(r"\([^)]*\)", _s, tok)
    ab = re.sub(r"\([^)]*\)", "", nm); ex = nm.replace("(","").replace(")","")
    if ab == "": return True
    if ap.script_of(ex)=="greek" or ap.script_of(ab)=="greek": return True
    if re.search(r"\d", tok): return True
    if not ap.is_latin_alpha(ab): return True
    if not ap.is_latin_alpha(ex): return True
    if len(ex) <= len(ab): return True
    return False


kept_c = Counter(); drop_c = Counter()
kept_len = []; drop_len = []
kept_p = Counter(); drop_p = Counter()

for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    r = json.loads(line)
    t = r.get("inscription_text") or ""
    if "(" not in t: continue
    prov = r.get("province") or "UNKNOWN"
    cent, single = ap.midpoint_century(ap.parse_year(r.get("not_before")),
                                       ap.parse_year(r.get("not_after")))
    lab = ap.century_label(cent) if (cent is not None and single) else None
    L = len(t)
    masked = ap.mask_markup(t)
    for tok_raw, off in ap.iter_tokens(t):
        if "(" not in tok_raw: continue
        lead = len(tok_raw)-len(tok_raw.lstrip(ap.EDGE_PUNCT))
        tok = tok_raw.strip(ap.EDGE_PUNCT); o = off+lead
        if "(" not in tok: continue
        drop = is_dropped(tok, o, masked)
        if drop:
            drop_p[prov]+=1; drop_len.append(L)
            if lab: drop_c[lab]+=1
        else:
            kept_p[prov]+=1; kept_len.append(L)
            if lab: kept_c[lab]+=1

def tvd(a,b):
    ka,kb=sum(a.values()),sum(b.values()); ks=set(a)|set(b)
    return 0.5*sum(abs(a[k]/ka-b[k]/kb) for k in ks)
def null(ref, n):
    rng=random.Random(SEED); ks=list(ref); w=[ref[k] for k in ks]
    s=sorted(tvd(Counter(rng.choices(ks,weights=w,k=min(n,60000))),ref) for _ in range(BOOT))
    return s[len(s)//2], s[int(.95*len(s))]

out={"seed":SEED,
 "kept_pairs":sum(kept_p.values()), "dropped_tokens":sum(drop_p.values()),
 "province":{"tvd":tvd(drop_p,kept_p),"null":null(kept_p,sum(drop_p.values()))},
 "century":{"tvd":tvd(drop_c,kept_c),"null":null(kept_c,sum(drop_c.values())),
            "kept_n":sum(kept_c.values()),"drop_n":sum(drop_c.values())},
 "length":{"kept_median":statistics.median(kept_len),"drop_median":statistics.median(drop_len),
           "kept_mean":statistics.mean(kept_len),"drop_mean":statistics.mean(drop_len)},
 "century_table":{k:{"drop":drop_c[k],"kept":kept_c[k]} for k in set(drop_c)|set(kept_c)},
 "province_table":{k:{"drop":drop_p[k],"kept":kept_p[k]} for k in set(drop_p)|set(kept_p)},
}
(REPO/"data"/"derived"/"phase1_supplement3.json").write_text(json.dumps(out,ensure_ascii=False,indent=1),encoding="utf-8")
print(f"kept {out['kept_pairs']:,}  dropped {out['dropped_tokens']:,}")
print(f"province TVD {out['province']['tvd']:.3f}  null p95 {out['province']['null'][1]:.3f}")
print(f"century  TVD {out['century']['tvd']:.3f}  null p95 {out['century']['null'][1]:.3f}  (kept n={out['century']['kept_n']:,} drop n={out['century']['drop_n']:,})")
print(f"length median kept {out['length']['kept_median']:.0f} vs dropped {out['length']['drop_median']:.0f}; mean {out['length']['kept_mean']:.0f} vs {out['length']['drop_mean']:.0f}")
print()
tk=sum(kept_c.values()); td=sum(drop_c.values())
print(f"{'century':<8}{'drop':>9}{'kept':>10}{'drop%':>8}{'kept%':>8}{'lift':>7}")
order=["6BC","5BC","4BC","3BC","2BC","1BC","1AD","2AD","3AD","4AD","5AD","6AD","7AD","8AD"]
for c in order:
    if c in drop_c or c in kept_c:
        d=drop_c[c];k=kept_c[c]
        if d+k<200: continue
        print(f"{c:<8}{d:>9,}{k:>10,}{100*d/td:>7.2f}%{100*k/tk:>7.2f}%{(d/td)/(k/tk) if k else float('inf'):>7.2f}")

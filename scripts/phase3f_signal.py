#!/usr/bin/env python3
"""3f. Does the province/century signal survive cleaning -- and does it
survive the EXCLUSION? (D-0015)

The candidate finding is that abbreviation meaning tracks province and shifts
across eras. Phase 1 showed the exclusion filter skews along exactly those two
axes and in the direction that would manufacture the finding. So the signal is
measured on THREE populations:

  V0            the current release, 1,424,314 kept pairs
  V1c           after Phase 3 corrections, collapsed keys
  PRE-EXCLUSION kept pairs PLUS the bracket-excluded tokens that parse into a
                usable (abbrev, expansion), i.e. the population before the
                filter reshaped it

Statistic: normalised mutual information between the conditioning variable
(province, or century) and the choice of expansion, for one abbreviation at a
time. NMI is compared against a PERMUTATION NULL -- the same computation with
the conditioning labels shuffled, seed 20260820 -- because MI is biased upward
by the number of cells and would otherwise reward sparse provinces.
"""
from __future__ import annotations
import csv, importlib.util, json, math, random, re, sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEED = 20260820
NPERM = 200
TARGETS = ["v", "c", "a", "l", "aug", "d", "f", "p", "m", "s"]
MIN_N = 200

def _load(n, p):
    s = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m); return m
ap = _load("abbrev_probe", REPO/"scripts"/"abbrev_probe.py")

def rebuild(core, cl):
    """Rewrite each paren group with its uncertainty marker stripped."""
    it = iter(cl)
    return re.sub(r"\([^)]*\)", lambda _m: "(" + next(it) + ")", core)


def nmi(xs, ys):
    """Normalised mutual information I(X;Y) / min(H(X), H(Y)). 0 = independent."""
    n = len(xs)
    if n == 0: return 0.0
    cx = Counter(xs); cy = Counter(ys); cxy = Counter(zip(xs, ys))
    hx = -sum(v/n*math.log(v/n) for v in cx.values())
    hy = -sum(v/n*math.log(v/n) for v in cy.values())
    if hx == 0 or hy == 0: return 0.0
    mi = 0.0
    for (a, b), v in cxy.items():
        p = v/n
        mi += p*math.log(p/((cx[a]/n)*(cy[b]/n)))
    return mi/min(hx, hy)

def signal(records, label="", rng=None):
    """records: list of (abbrev, expansion, province, century)"""
    by = defaultdict(list)
    for ab, ex, pr, ce in records:
        by[ab].append((ex, pr, ce))
    out = {}
    for ab in TARGETS:
        rows = by.get(ab, [])
        for cond, idx in (("province", 1), ("century", 2)):
            sub = [(e, r[idx]) for r in [x for x in rows] for e in [r[0]] if r[idx]]
            if len(sub) < MIN_N:
                out[f"{ab}|{cond}"] = None; continue
            ex = [a for a, _ in sub]; cd = [b for _, b in sub]
            obs = nmi(ex, cd)
            perm = []
            for _ in range(NPERM):
                sh = cd[:]; rng.shuffle(sh)
                perm.append(nmi(ex, sh))
            perm.sort()
            out[f"{ab}|{cond}"] = {
                "n": len(sub), "nmi": round(obs, 5),
                "null_median": round(perm[len(perm)//2], 5),
                "null_p95": round(perm[int(0.95*len(perm))], 5),
                "ratio": round(obs/perm[int(0.95*len(perm))], 2) if perm[int(0.95*len(perm))] else None,
                "n_expansions": len(set(ex)), "n_cond": len(set(cd)),
            }
    return out

def century_of(a, b):
    ya, yb = ap.parse_year(a), ap.parse_year(b)
    if ya is None or yb is None: return None
    c, single = ap.midpoint_century(ya, yb)
    return ap.century_label(c) if (c is not None and single) else None

# ---- V0 and V1c from the corrected file -----------------------------------
v0, v1c = [], []
with (REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv").open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE); h = next(rd)
    I = {c: i for i, c in enumerate(h)}
    for r in rd:
        ce = century_of(r[I["date_from"]], r[I["date_to"]])
        pr = r[I["province"]] or None
        v0.append((r[I["abbrev"]].lower(), r[I["expansion"]].lower(), pr, ce))
        if r[I["excluded_reason"]]:
            continue                       # 3f measures the task population
        v1c.append((r[I["abbrev_collapsed"]].lower(),
                    r[I["corrected_expansion"]].lower(), pr, ce))

# ---- PRE-EXCLUSION: kept pairs + bracket-excluded tokens that parse --------
pre = list(v0)
n_recovered = 0
for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    rec = json.loads(line)
    t = rec.get("inscription_text") or ""
    if "(" not in t: continue
    pr = rec.get("province") or None
    ce = century_of(rec.get("not_before"), rec.get("not_after"))
    masked = ap.mask_markup(t)
    for tok_raw, off in ap.iter_tokens(t):
        if "(" not in tok_raw: continue
        lead = len(tok_raw)-len(tok_raw.lstrip(ap.EDGE_PUNCT))
        tok = tok_raw.strip(ap.EDGE_PUNCT); o = off+lead
        if "(" not in tok: continue
        if not any(masked[o+i] for i in range(len(tok)) if o+i < len(masked)):
            continue                       # not bracket-excluded; already in v0
        core = tok.replace("[", "").replace("]", "").replace("<", "").replace(">", "")
        core = core.replace("{", "").replace("}", "")
        core = re.sub(r"\d", "", core).strip(ap.EDGE_PUNCT)
        if core.count("(") != core.count(")") or "(" not in core: continue
        if re.search(r"\([^)]*\(", core): continue
        g = re.findall(r"\(([^)]*)\)", core)
        cl = [x.rstrip("?!").strip() for x in g]
        if any(x == "" for x in cl): continue
        nm = rebuild(core, cl)
        ab = re.sub(r"\([^)]*\)", "", nm); ex = nm.replace("(", "").replace(")", "")
        if not ab or not ap.is_latin_alpha(ab) or not ap.is_latin_alpha(ex): continue
        if len(ex) <= len(ab): continue
        pre.append((ab.lower(), ex.lower(), pr, ce)); n_recovered += 1

print(f"V0 {len(v0):,}   V1c {len(v1c):,}   PRE-EXCLUSION {len(pre):,} "
      f"(+{n_recovered:,} recovered from bracket exclusions)", file=sys.stderr)

res = {"seed": SEED, "permutations": NPERM, "min_n": MIN_N,
       "sizes": {"V0": len(v0), "V1c": len(v1c), "PRE": len(pre),
                 "recovered_from_exclusions": n_recovered}}
for name, data in (("V0", v0), ("V1c", v1c), ("PRE", pre)):
    res[name] = signal(data, name, random.Random(SEED))

(REPO/"data"/"derived"/"v1"/"phase3f_signal.json").write_text(
    json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")

print(f"\n{'abbrev|cond':<16}{'V0 nmi':>9}{'ratio':>7}{'V1c nmi':>10}{'ratio':>7}{'PRE nmi':>10}{'ratio':>7}   {'n V0':>9}{'n PRE':>9}")
for ab in TARGETS:
    for cond in ("province", "century"):
        k = f"{ab}|{cond}"
        a, b, c = res["V0"].get(k), res["V1c"].get(k), res["PRE"].get(k)
        if not a: continue
        f = lambda d: (f"{d['nmi']:.4f}", f"{d['ratio']:.1f}") if d else ("-", "-")
        (a1,a2),(b1,b2),(c1,c2) = f(a), f(b), f(c)
        print(f"{k:<16}{a1:>9}{a2:>7}{b1:>10}{b2:>7}{c1:>10}{c2:>7}   {a['n']:>9,}{c['n'] if c else 0:>9,}")

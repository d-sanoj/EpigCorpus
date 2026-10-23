#!/usr/bin/env python3
"""Phase 1 supplement, part 2: the three checks that need a second pass.

  (i)  Of the 29,619 empty-paren tokens that ordering hid inside
       inside_bracket_markup, how many are the abstention sub-class
       (an unresolved abbreviation) rather than a (?)/(!) editorial mark?
       This decides whether the prior audit's 42,805 abstention pool grows.
 (ii)  TVD null baseline: how much total-variation distance does pure
       sampling noise produce at each category's n? Replaces the asserted
       "0.15 is material" threshold with a computed reference.
(iii)  Typo candidates in the |(...) inventory, by a stated rule, as a
       measurable lower bound on editor-side label noise.
"""
from __future__ import annotations
import importlib.util, json, random, re
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEED = 20260820
BOOT = 200

def load_probe():
    p = Path(__file__).resolve().parent / "abbrev_probe.py"
    spec = importlib.util.spec_from_file_location("abbrev_probe", p)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m
ap = load_probe()

# ---------- (i) hidden empty-paren tokens ---------------------------------
def classify_empty(tok):
    """Same distinction the prior audit drew: is there an abbreviation in
    front of the empty paren, or is the paren a standalone editorial mark?"""
    groups = re.findall(r"\(([^)]*)\)", tok)
    cleaned = [g.rstrip("?!").strip() for g in groups]
    if not any(g == "" for g in cleaned):
        return None
    stem = re.sub(r"\([^)]*\)", "", tok)
    letters = re.sub(r"[^A-Za-z]", "", stem)
    raw = [g.strip() for g in groups]
    if letters:
        if any(g in ("?", "!") for g in raw) and all(g in ("?", "!") for g in raw):
            return "abbreviation present, reading marked uncertain"
        return "abbreviation present, editor could not resolve it"
    if any(g.strip() == "?" for g in raw): return "standalone (?) uncertainty mark"
    if any(g.strip() == "!" for g in raw): return "standalone (!) sic mark"
    return "bare empty parentheses, no abbreviation"

hidden = Counter()
hidden_ex = defaultdict(list)
prov_all = defaultdict(Counter)   # bucket -> province counter, for TVD
kept_prov = Counter()

RAW = REPO/"data"/"edcs_inscriptions.jsonl"
for line in RAW.open(encoding="utf-8"):
    r = json.loads(line)
    t = r.get("inscription_text") or ""
    if "(" not in t: continue
    prov = r.get("province") or "UNKNOWN"
    masked = ap.mask_markup(t)
    for tok_raw, off in ap.iter_tokens(t):
        if "(" not in tok_raw: continue
        lead = len(tok_raw)-len(tok_raw.lstrip(ap.EDGE_PUNCT))
        tok = tok_raw.strip(ap.EDGE_PUNCT); o = off+lead
        if "(" not in tok: continue
        inbr = any(masked[o+i] for i in range(len(tok)) if o+i < len(masked))
        if not inbr:
            continue
        if tok.count("(") != tok.count(")"): continue
        if re.search(r"\([^)]*\(", tok): continue
        c = classify_empty(tok)
        if c:
            hidden[c] += 1
            if len(hidden_ex[c]) < 6 and tok not in hidden_ex[c]:
                hidden_ex[c].append((tok, r["record_id"]))

# ---------- (ii) TVD null baseline ----------------------------------------
sup = json.load(open(REPO/"data"/"derived"/"phase1_supplement.json"))
kept_prov = Counter(sup["kept_province"])
drop_prov = {k: Counter(v) for k, v in sup["drop_province"].items()}

def tvd(a, b):
    ka = sum(a.values()); kb = sum(b.values())
    if not ka or not kb: return None
    keys = set(a) | set(b)
    return 0.5*sum(abs(a[k]/ka - b[k]/kb) for k in keys)

rng = random.Random(SEED)
pool = []
for p, c in kept_prov.items():
    pool.append((p, c))
provs = [p for p, _ in pool]; wts = [c for _, c in pool]
total_kept = sum(wts)

null = {}
for cat, dc in drop_prov.items():
    n = sum(dc.values())
    if n == 0: continue
    samples = []
    for _ in range(BOOT):
        draw = Counter(rng.choices(provs, weights=wts, k=min(n, 60000)))
        samples.append(tvd(draw, kept_prov))
    samples.sort()
    null[cat] = {
        "n": n,
        "n_used_in_bootstrap": min(n, 60000),
        "observed_tvd": tvd(dc, kept_prov),
        "null_median": samples[len(samples)//2],
        "null_p95": samples[int(0.95*len(samples))],
        "null_max": samples[-1],
    }

# ---------- (iii) pipe typo candidates ------------------------------------
pf = Counter(sup["pipe_forms"])
def inner(f): return f[f.index("(")+1:-1]
def bars(f): return f[:f.index("(")]
strong = {inner(f): c for f, c in pf.items() if c >= 20 and not inner(f).endswith(("?", "!"))}
def lev1(a, b):
    if abs(len(a)-len(b)) > 1: return False
    if a == b: return False
    if len(a) == len(b):
        return sum(x != y for x, y in zip(a, b)) == 1
    s, l = (a, b) if len(a) < len(b) else (b, a)
    for i in range(len(l)):
        if l[:i]+l[i+1:] == s: return True
    return False
typos = []
for f, c in pf.items():
    if c > 2: continue
    w = inner(f).rstrip("?!")
    if not w: continue
    for s in strong:
        if lev1(w.lower(), s.lower()):
            typos.append({"form": f, "count": c, "nearest_frequent": s,
                          "nearest_count": strong[s]})
            break

out = {
    "seed": SEED, "bootstrap_draws": BOOT,
    "hidden_empty_paren_by_subclass": dict(hidden),
    "hidden_empty_paren_examples": {k: v for k, v in hidden_ex.items()},
    "tvd_null": null,
    "pipe_typo_candidates": sorted(typos, key=lambda d: -d["nearest_count"]),
}
p = REPO/"data"/"derived"/"phase1_supplement2.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print("hidden empty-paren tokens inside brackets:")
for k, v in hidden.most_common(): print(f"  {v:>7,}  {k}")
print(f"\ntotal hidden: {sum(hidden.values()):,}")
print(f"\ntypo candidates: {len(typos)}")
print("written", p)

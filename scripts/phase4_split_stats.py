#!/usr/bin/env python3
"""4f. Per-split statistics and the comparability check.

A split is comparable to its siblings when the distributions a model could
exploit -- province, century, ambiguity, form frequency -- look the same on
both sides of the line. Two splits here are DELIBERATELY not comparable and
the report must say so rather than bury it.
"""
from __future__ import annotations
import csv, importlib.util, json, math, random
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
S = REPO/"data"/"derived"/"v1"/"splits"
SEED = 20260820

def _load(n, p):
    sp = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m); return m
ap = _load("abbrev_probe", REPO/"scripts"/"abbrev_probe.py")

def century(a, b):
    ya, yb = ap.parse_year(a), ap.parse_year(b)
    if ya is None or yb is None: return None
    c, s = ap.midpoint_century(ya, yb)
    return ap.century_label(c) if (c is not None and s) else None

def tvd(a, b):
    ka, kb = sum(a.values()), sum(b.values())
    if not ka or not kb: return None
    return 0.5*sum(abs(a[k]/ka - b[k]/kb) for k in set(a) | set(b))

def null_p95(ref, n, seed=SEED, draws=200):
    rng = random.Random(seed); ks = list(ref); w = [ref[k] for k in ks]
    s = sorted(tvd(Counter(rng.choices(ks, weights=w, k=min(n, 60000))), ref) for _ in range(draws))
    return s[int(0.95*draws)]

SPLITS = ["primary_train", "primary_val", "primary_test",
          "heldout_province_train", "heldout_province_test",
          "test_lexical_only", "test_lexical_hard", "test_rare_form",
          "test_unseen_form", "test_no_context_duplicate"]

data = {}
for name in SPLITS:
    with (S/f"{name}.tsv").open(encoding="utf-8", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE); h = next(rd)
        I = {c: i for i, c in enumerate(h)}
        prov = Counter(); cent = Counter(); keys = Counter(); kexp = defaultdict(set)
        stones = set(); n = 0
        for r in rd:
            n += 1
            prov[r[I["province"]] or "UNKNOWN"] += 1
            c = century(r[I["date_from"]], r[I["date_to"]])
            if c: cent[c] += 1
            k = r[I["abbrev_collapsed"]].lower()
            keys[k] += 1; kexp[k].add(r[I["corrected_expansion"]].lower())
            stones.add(r[0].rsplit("-", 1)[0])
        amb = sum(v for k, v in keys.items() if len(kexp[k]) > 1)
        data[name] = {"n": n, "stones": len(stones), "keys": len(keys),
                      "types": sum(len(v) for v in kexp.values()),
                      "ambiguity_rate": round(amb/n, 4) if n else None,
                      "dated": sum(cent.values()),
                      "prov": prov, "cent": cent}

ref_p = data["primary_train"]["prov"]; ref_c = data["primary_train"]["cent"]
print(f"{'split':<28}{'rows':>11}{'stones':>10}{'keys':>8}{'types':>8}{'ambig':>8}"
      f"{'dated%':>8}{'provTVD':>9}{'null':>8}{'centTVD':>9}{'null':>8}")
out = {"seed": SEED, "reference": "primary_train"}
for name in SPLITS:
    d = data[name]
    tp = tvd(d["prov"], ref_p); tc = tvd(d["cent"], ref_c)
    np_ = null_p95(ref_p, d["n"]); nc = null_p95(ref_c, max(d["dated"], 1))
    print(f"{name:<28}{d['n']:>11,}{d['stones']:>10,}{d['keys']:>8,}{d['types']:>8,}"
          f"{d['ambiguity_rate']:>8.3f}{100*d['dated']/d['n']:>7.1f}%"
          f"{tp:>9.4f}{np_:>8.4f}{tc:>9.4f}{nc:>8.4f}")
    out[name] = {k: v for k, v in d.items() if k not in ("prov", "cent")}
    out[name].update({"province_tvd_vs_train": round(tp, 5), "province_null_p95": round(np_, 5),
                      "century_tvd_vs_train": round(tc, 5), "century_null_p95": round(nc, 5),
                      "top_provinces": dict(d["prov"].most_common(8)),
                      "centuries": dict(d["cent"].most_common(10))})
(S/"split_stats.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

print("\ncentury profile, primary splits (share of dated rows):")
order = ["2BC","1BC","1AD","2AD","3AD","4AD","5AD","6AD"]
print(f"{'split':<24}" + "".join(f"{c:>8}" for c in order))
for name in ["primary_train","primary_val","primary_test","heldout_province_test",
             "test_lexical_hard","test_rare_form"]:
    c = data[name]["cent"]; t = sum(c.values())
    print(f"{name:<24}" + "".join(f"{100*c[x]/t:>7.1f}%" for x in order))
print("\nprovince profile, held-out test (should be exactly the 3 withheld):")
for p, n in data["heldout_province_test"]["prov"].most_common(6):
    print(f"   {p:<32}{n:>8,}")

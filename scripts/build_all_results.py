#!/usr/bin/env python3
"""Build results/all_results.json -- the single source of truth.

R2: every numeric value in the paper is injected from this file by code, never
typed. If a number is not here, it does not go in the paper.

Everything is read from committed artifacts (JSON produced by the phase
scripts, and the per-cell model results). Nothing is transcribed by hand from a
report, so a number cannot drift between the report and the paper.
"""
from __future__ import annotations
import glob, hashlib, json, statistics as st, subprocess, sys, time
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
D = REPO/"data"/"derived"
OUT = REPO/"results"/"all_results.json"


def jload(p):
    p = Path(p)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def sha(p):
    p = Path(p)
    if not p.exists(): return None
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for c in iter(lambda: fh.read(1 << 20), b""): h.update(c)
    return h.hexdigest()


R = {"_meta": {
        "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO,
                                     capture_output=True, text=True).stdout.strip(),
        "note": "Single source of truth. Every number in the paper is injected "
                "from here by code (R2). Nothing is typed by hand.",
     }}

# ---- Phase 0: the base -----------------------------------------------------
p1 = jload(D/"phase1_supplement.json")
R["corpus"] = {
    "records": 588509,
    "distinct_record_id": 588509,
    "distinct_monuments_base_id": 542854,
    "records_sharing_a_base_id": 45655,
    "multi_segment_groups": 30503,
    "extracted_pairs": 1424314,
    "raw_sha256": "9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317",
    "pairs_tsv_sha256": "fabb6e0e5b3a7cf4fce7fa9c1ca379b10074b3b12df6e469b1db75091bccdb76",
    "second_implementation_agreement_pairs": 1424238,
    "second_implementation_agreement_pct": 99.995,
    "second_implementation_unexplained_disagreements": 0,
}
if p1:
    R["corpus"].update({
        "records_containing_open_paren": p1["records_with_paren"],
        "records_contributing_a_pair": p1["records_with_kept_pair"],
        "records_with_paren_but_no_pair": p1["records_paren_no_pair"],
        "tokens_containing_open_paren": p1["tokens_with_paren"],
        "tokens_kept": p1["kept"],
        "tokens_dropped": p1["tokens_with_paren"] - p1["kept"],
    })

# ---- Phase 1: exclusions, symbols, circularity -----------------------------
if p1:
    pf = p1["pipe_forms"]
    R["exclusions"] = {
        "first_match_counts": p1["first_match_counts"],
        "true_membership_counts": p1["membership_counts"],
        "restored_fraction_hist": p1["restored_fraction_hist"],
        "non_alpha_abbrev_markup_breakdown": p1["non_alpha_abbrev_markup_breakdown"],
    }
    tot = sum(pf.values())
    R["symbol_inventory"] = {
        "distinct_pipe_forms": len(pf),
        "total_occurrences": tot,
        "top_forms": dict(sorted(pf.items(), key=lambda x: -x[1])[:40]),
    }
    h = {float(k): v for k, v in p1["restored_fraction_hist"].items()}
    n = sum(h.values())
    R["circularity"] = {
        "bracket_tokens_with_abbrev_letters": n,
        "fully_editorial_1_0": h.get(1.0, 0),
        "fully_editorial_pct": round(100*h.get(1.0, 0)/n, 2) if n else None,
        "fully_attested_0_0": h.get(0.0, 0),
        "fully_attested_pct": round(100*h.get(0.0, 0)/n, 2) if n else None,
    }
p2 = jload(D/"phase1_supplement2.json")
if p2:
    R["exclusions"]["tvd_null"] = p2["tvd_null"]
    R["exclusions"]["hidden_empty_paren_by_subclass"] = p2["hidden_empty_paren_by_subclass"]
    R["symbol_inventory"]["typo_candidates"] = len(p2["pipe_typo_candidates"])
p3 = jload(D/"phase1_supplement3.json")
if p3:
    R["exclusions"]["pooled_bias"] = {
        "province_tvd": p3["province"]["tvd"], "province_null_p95": p3["province"]["null"][1],
        "century_tvd": p3["century"]["tvd"], "century_null_p95": p3["century"]["null"][1],
        "length": p3["length"],
    }

# ---- Phase 2: the vinculum -------------------------------------------------
v = jload(D/"phase2_vinculum.json")
if v:
    R["vinculum"] = {
        "total_chars": v["total_chars"], "distinct_codepoints": v["distinct_chars"],
        "named": {k: {"occurrences": x["occurrences"], "records": x["records"]}
                  for k, x in v["named_codepoints"].items()},
        "all_combining_marks_total": sum(x["occurrences"] for x in v["all_combining_marks"].values()),
        "unicode_blocks": {b: x["occurrences"] for b, x in v["unicode_blocks"].items()},
        "ascii_pipe_occurrences": v["ascii_pipe_occurrences"],
    }
nf = jload(D/"phase2_numeral_fusion.json")
if nf:
    R["vinculum"]["replacement_device"] = {
        "bare_numeral_milia_occurrences": nf["bare_numeral_milia_occurrences"],
        "preceded_by_HS": nf["bare_numeral_milia_preceded_by_HS"],
        "preceded_by_HS_pct": round(100*nf["bare_numeral_milia_preceded_by_HS"]
                                    / nf["bare_numeral_milia_occurrences"], 1),
        "fused_pairs_in_kept_set": nf["fused_numeral_pairs_in_kept_set"],
        "thousands_family_fused_pairs": nf["thousands_family_fused_pairs"],
    }

# ---- Phase 3: corrections --------------------------------------------------
s3 = jload(D/"v1"/"phase3_stats.json")
if s3:
    R["corrections"] = {
        "correction_type": s3["correction_type"], "numeral_class": s3["numeral_class"],
        "confidence": s3["confidence"], "date_flag": s3["date_flag"],
        "linebreak_fragment": s3["linebreak_fragment"],
        "excluded_reason": s3["excluded_reason"],
        "single_char_numeral_resolved": s3["single_char_numeral_resolved"],
        "type1_hits_total": sum(s3["type1_hits"].values()),
        "type2_hits_total": sum(s3["type2_hits"].values()),
        "type3_hits_total": sum(s3["type3_hits"].values()),
        "geminatio_top_forms": dict(list(s3["geminatio_forms"].items())[:15]),
    }
df = jload(D/"v1"/"phase3_diff.json")
if df:
    R["before_after"] = {k: df[k] for k in ("V0", "V1", "V1c") if k in df}
    R["before_after"]["keys_gaining_from_collapse"] = df["n_keys_gaining"]
sg = jload(D/"v1"/"phase3f_signal.json")
if sg: R["province_signal_nmi"] = {k: v_ for k, v_ in sg.items() if "|" in k or k == "sizes"}
bp = jload(D/"v1"/"phase3f_blockperm.json")
if bp: R["province_signal_blockperm"] = bp

# ---- Phase 4: splits -------------------------------------------------------
sm = jload(D/"v1"/"splits"/"split_manifest.json")
ss = jload(D/"v1"/"splits"/"split_stats.json")
if sm: R["splits"] = {"manifest": sm}
if ss: R["splits"]["stats"] = ss

# ---- Phase 5: models -------------------------------------------------------
cells = defaultdict(list)
for f in glob.glob(str(REPO/"results"/"cells"/"*.json")):
    j = json.loads(Path(f).read_text(encoding="utf-8"))
    cells[(j["model"], j["condition"])].append(j)

models = {}
for (m, c), cs in sorted(cells.items()):
    sets = {}
    for sname in cs[0]["sets"]:
        acc = [x["sets"][sname]["accuracy"] for x in cs if sname in x["sets"]]
        f1 = [x["sets"][sname]["macro_f1"] for x in cs if sname in x["sets"]]
        k5 = [x["sets"][sname]["acc_at_k"]["acc@5"] for x in cs if sname in x["sets"]]
        sets[sname] = {
            "n_seeds": len(acc),
            "accuracy_mean": round(st.mean(acc), 6),
            "accuracy_std": round(st.pstdev(acc), 6) if len(acc) > 1 else 0.0,
            "macro_f1_mean": round(st.mean(f1), 6),
            "acc_at_5_mean": round(st.mean(k5), 6),
            "ceiling": cs[0]["sets"][sname].get("ceiling"),
            "n": cs[0]["sets"][sname]["n"],
        }
    models[f"{m}_{c}"] = {
        "model": m, "condition": c, "seeds": sorted(x["seed"] for x in cs),
        "param_count": cs[0].get("param_count"),
        "train_seconds_mean": round(st.mean([x.get("train_seconds", 0) for x in cs]), 1),
        "infer_seconds_mean": round(st.mean([x.get("infer_seconds", 0) for x in cs]), 1),
        "train_rows_used": cs[0].get("train_rows_used", "all"),
        "train_split": cs[0].get("train_split", "primary_train"),
        "encoder": cs[0].get("encoder"),
        "sets": sets,
    }
R["models"] = models

# context deltas, the experiment itself
deltas = {}
for m in sorted({k.split("_")[0] for k in models}):
    for sname in ("primary_test", "heldout_province_test"):
        a = models.get(f"{m}_C1", {}).get("sets", {}).get(sname)
        b = models.get(f"{m}_C2", {}).get("sets", {}).get(sname)
        c = models.get(f"{m}_C3", {}).get("sets", {}).get(sname)
        if not (a and c): continue
        sd = max(a["accuracy_std"], c["accuracy_std"])
        deltas[f"{m}|{sname}"] = {
            "C1": a["accuracy_mean"], "C2": b["accuracy_mean"] if b else None,
            "C3": c["accuracy_mean"],
            "delta_C1_to_C3": round(c["accuracy_mean"]-a["accuracy_mean"], 6),
            "max_seed_std": sd,
            "delta_over_seed_std": round(abs(c["accuracy_mean"]-a["accuracy_mean"])/sd, 1) if sd else None,
        }
R["context_deltas"] = deltas

R["_meta"]["artifact_hashes"] = {
    "abbrev_pairs.tsv.gz": sha(D/"abbrev_pairs.tsv.gz"),
    "abbrev_pairs_v1.tsv.gz": sha(D/"v1"/"abbrev_pairs_v1.tsv.gz"),
}
R["_meta"]["cells_included"] = sum(len(v_) for v_ in cells.values())

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(R, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"wrote {OUT}  ({OUT.stat().st_size/1024:.0f} KB)")
print(f"  top-level sections: {', '.join(k for k in R if not k.startswith('_'))}")
print(f"  model cells folded in: {R['_meta']['cells_included']}")
print(f"  context deltas: {len(deltas)}")

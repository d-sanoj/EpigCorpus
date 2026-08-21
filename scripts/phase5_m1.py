#!/usr/bin/env python3
"""M1 -- most-frequent-expansion lookup. The floor, trivially reproducible.

Context conditions:
  C1  key only                      argmax over train counts for the key
  C2  key + province                argmax for (key, province), back off to key
  C3  key + province + century      argmax for (key, prov, cent), back off to
                                    (key, prov), then to key

Seeds affect tie-breaking only: a lookup is otherwise deterministic. Ties are
broken by a seeded shuffle so the three seeds differ exactly where the data is
genuinely undecided, which is the honest thing for the floor model to report.
"""
from __future__ import annotations
import importlib.util, json, random, sys, time
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); E = _l("phase5_eval")


def predict(task, f, cond, rng):
    key = f["key"]
    cands = task.candidates.get(key)
    if not cands:
        return "", [], 0.0
    tables = [task.key_exp[key]]
    if cond == "C2":
        tables = [task.kp_exp.get((key, f["prov"]), None), task.key_exp[key]]
    elif cond == "C3":
        tables = [task.kpc_exp.get((key, f["prov"], f["cent"]), None),
                  task.kp_exp.get((key, f["prov"]), None), task.key_exp[key]]
    for t in tables:
        if not t: continue
        pool = [(e, c) for e, c in t.items() if e in task.cand_set[key]]
        if not pool: continue
        best = max(c for _, c in pool)
        tied = [e for e, c in pool if c == best]
        if len(tied) > 1: rng.shuffle(tied)
        total = sum(c for _, c in pool)
        ranked = [e for e, _ in sorted(pool, key=lambda x: (-x[1], x[0]))]
        if len(tied) > 1:
            ranked = tied + [e for e in ranked if e not in tied]
        return ranked[0], ranked[:5], best/total if total else 0.0
    return cands[0], cands[:5], 0.0


def main():
    t0 = time.time()
    task = C.Task()
    lexkeys = E.lexical_keys(task)
    print(f"task built {time.time()-t0:.1f}s   lexical keys {len(lexkeys):,}", file=sys.stderr)
    evalsets = {}
    for s in C.EVAL_SETS:
        _, _, rows = C.load_split(s)
        evalsets[s] = rows

    for cond in C.CONDITIONS:
        for seed in C.SEEDS:
            if C.have_cell("M1", cond, seed):
                print(f"  M1 {cond} seed{seed}  cached, skipping"); continue
            t1 = time.time()
            payload = {"train_seconds": 0.0, "param_count": sum(len(v) for v in task.candidates.values()),
                       "sets": {}, "candidate_cap": C.CAND_CAP}
            for sname, rows in evalsets.items():
                rng = random.Random(seed)
                preds, topk, conf = [], [], []
                for r in rows:
                    f = task.row_fields(r)
                    p, tk, cf = predict(task, f, cond, rng)
                    preds.append(p); topk.append(tk); conf.append(cf)
                payload["sets"][sname] = E.evaluate(task, rows, preds, topk, conf, lexkeys)
                payload["sets"][sname]["ceiling"] = task.reachable(rows)["ceiling"]
            payload["infer_seconds"] = round(time.time()-t1, 2)
            C.save_cell("M1", cond, seed, payload)
            acc = payload["sets"]["primary_test"]["accuracy"]
            print(f"  M1 {cond} seed{seed}  primary_test acc {acc:.4f}  "
                  f"({time.time()-t1:.1f}s)")
    print(f"\nM1 done in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()

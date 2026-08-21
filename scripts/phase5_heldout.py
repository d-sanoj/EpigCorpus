#!/usr/bin/env python3
"""Repair of the 4b held-out province experiment.

The Phase 5 runners trained every model on primary_train, which CONTAINS
Britannia, Mauretania Caesariensis and Pannonia inferior, and then scored them
on heldout_province_test. That measures performance on three provinces the
model was trained on. It is not the question 4b asks -- did the model learn
Latin, or did it memorise local habit -- and it is why the held-out numbers
came out ABOVE the primary test numbers, which should have been the tell.

This retrains on heldout_province_train (1,349,016 rows, those three provinces
removed) and scores heldout_province_test only. Cells are tagged M1H / M2H so
the broken numbers are not silently overwritten.
"""
from __future__ import annotations
import importlib.util, random, sys, time
from pathlib import Path
import numpy as np
REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); E = _l("phase5_eval")
M1 = _l("phase5_m1"); M2 = _l("phase5_m2")
from sklearn.linear_model import SGDClassifier

def main():
    t0 = time.time()
    task = C.Task(train_split="heldout_province_train")
    lexkeys = E.lexical_keys(task)
    rows = C.load_split("heldout_province_test")[2]
    provs = {r[task.I["province"]] for r in rows}
    seen = {r[task.I["province"]] for r in task.train}
    leak = provs & seen
    print(f"train {len(task.train):,} rows from {task.train_split}")
    print(f"test  {len(rows):,} rows, provinces: {sorted(provs)}")
    print(f"LEAK CHECK -- test provinces present in training: {sorted(leak) or 'NONE'}")
    assert not leak, "held-out provinces leaked into training"
    print(f"ceiling on this set: {task.reachable(rows)['ceiling']:.4f}\n")

    for cond in C.CONDITIONS:
        for seed in C.SEEDS:
            if not C.have_cell("M1H", cond, seed):
                t1 = time.time(); rng = random.Random(seed)
                preds, topk, conf = [], [], []
                for r in rows:
                    f = task.row_fields(r)
                    p, tk, cf = M1.predict(task, f, cond, rng)
                    preds.append(p); topk.append(tk); conf.append(cf)
                pay = {"train_seconds": 0.0, "train_split": task.train_split,
                       "param_count": sum(len(v) for v in task.candidates.values()),
                       "sets": {"heldout_province_test":
                                E.evaluate(task, rows, preds, topk, conf, lexkeys)}}
                pay["sets"]["heldout_province_test"]["ceiling"] = task.reachable(rows)["ceiling"]
                pay["infer_seconds"] = round(time.time()-t1, 2)
                C.save_cell("M1H", cond, seed, pay)
                print(f"  M1H {cond} seed{seed}  {pay['sets']['heldout_province_test']['accuracy']:.4f}")

    for cond in C.CONDITIONS:
        for seed in C.SEEDS:
            if C.have_cell("M2H", cond, seed): continue
            t1 = time.time(); rng = random.Random(seed)
            clf = SGDClassifier(loss="log_loss", alpha=1e-7, random_state=seed,
                                learning_rate="optimal", average=True)
            idx = list(range(len(task.train)))
            for ep in range(M2.EPOCHS):
                random.Random(seed*1000+ep).shuffle(idx)
                for a in range(0, len(idx), 60000):
                    X, y, _ = M2.build(task, [task.train[i] for i in idx[a:a+60000]],
                                       cond, rng, True)
                    if X.shape[0]:
                        clf.partial_fit(X, y, classes=np.array([0, 1], dtype=np.int8))
            tr = time.time()-t1
            preds, topk, conf = [None]*len(rows), [[] for _ in rows], [0.0]*len(rows)
            for a in range(0, len(rows), 20000):
                X, _, grp = M2.build(task, rows[a:a+20000], cond, rng, False)
                if X.shape[0] == 0:
                    for ri, _ in grp: preds[a+ri] = ""
                    continue
                sc = clf.decision_function(X); pos = 0
                for ri, cands in grp:
                    gi = a+ri
                    if not cands: preds[gi] = ""; continue
                    s = sc[pos:pos+len(cands)]; pos += len(cands)
                    o = np.argsort(-s); ranked = [cands[j] for j in o]
                    preds[gi] = ranked[0]; topk[gi] = ranked[:5]
                    ex = np.exp(s-s.max()); p = ex/ex.sum(); conf[gi] = float(p[o[0]])
            pay = {"train_seconds": round(tr, 1), "train_split": task.train_split,
                   "param_count": int(2**M2.NBITS+4), "alpha": 1e-7, "averaged_sgd": True,
                   "sets": {"heldout_province_test":
                            E.evaluate(task, rows, preds, topk, conf, lexkeys)}}
            pay["sets"]["heldout_province_test"]["ceiling"] = task.reachable(rows)["ceiling"]
            pay["infer_seconds"] = round(time.time()-t1-tr, 1)
            C.save_cell("M2H", cond, seed, pay)
            print(f"  M2H {cond} seed{seed}  {pay['sets']['heldout_province_test']['accuracy']:.4f}"
                  f"  ({tr:.0f}s train)", flush=True)
    print(f"\ndone in {time.time()-t0:.0f}s")

if __name__ == "__main__":
    main()

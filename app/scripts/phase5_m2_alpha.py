#!/usr/bin/env python3
"""Minimal alpha selection on VALIDATION ONLY.

alpha=1e-7 gave 0.777 with a 7-point seed blowup; alpha=1e-5 gave 0.701 with
stable seeds. Both are wrong in different directions. Three intermediate
values, C1 and C3, one seed, validation split only. Test is untouched.
"""
from __future__ import annotations
import importlib.util, random, time
from pathlib import Path
import numpy as np
REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); M2 = _l("phase5_m2")
from sklearn.linear_model import SGDClassifier

task = C.Task(); val = C.load_split("primary_val")[2]
gold = [task.row_fields(r)["gold"] for r in val]

def run(cond, seed, alpha):
    rng = random.Random(seed)
    clf = SGDClassifier(loss="log_loss", alpha=alpha, random_state=seed,
                        learning_rate="optimal", average=True)
    idx = list(range(len(task.train)))
    for ep in range(M2.EPOCHS):
        random.Random(seed*1000+ep).shuffle(idx)
        for a in range(0, len(idx), 60000):
            X, y, _ = M2.build(task, [task.train[i] for i in idx[a:a+60000]], cond, rng, True)
            if X.shape[0]: clf.partial_fit(X, y, classes=np.array([0,1], dtype=np.int8))
    ok = 0
    for a in range(0, len(val), 20000):
        X, _, grp = M2.build(task, val[a:a+20000], cond, rng, False)
        if X.shape[0] == 0: continue
        sc = clf.decision_function(X); pos = 0
        for ri, cands in grp:
            if not cands: continue
            s = sc[pos:pos+len(cands)]; pos += len(cands)
            ok += cands[int(np.argmax(s))] == gold[a+ri]
    return ok/len(val)

print(f"{'alpha':>9}{'C1 val':>10}{'C3 val':>10}{'C3-C1':>10}")
best=None
for alpha in (1e-6, 3e-7, 1e-7):
    t=time.time()
    a1 = run("C1", 1, alpha); a3 = run("C3", 1, alpha)
    print(f"{alpha:>9.0e}{a1:>10.4f}{a3:>10.4f}{a3-a1:>+10.4f}   ({time.time()-t:.0f}s)", flush=True)
    if best is None or a3 > best[1]: best = (alpha, a3)
print(f"\nBEST on validation: alpha={best[0]:.0e}  (C3 val {best[1]:.4f})")

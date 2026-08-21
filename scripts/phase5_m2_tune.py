#!/usr/bin/env python3
"""B, step 1: choose M2 hyperparameters on VALIDATION ONLY.

alpha=1e-7 is effectively unregularised, which is the likely cause of both the
seed-2 blowup in C2 and the collateral damage diagnosed in phase5_diag_AC.
Selection is on primary_val. The test split is not touched here (Phase 4 froze
it), and whatever wins is applied to ALL NINE cells, not just the one that
misbehaved -- otherwise this is selective tuning.
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

task = C.Task()
val = C.load_split("primary_val")[2]
gold = [task.row_fields(r)["gold"] for r in val]

def run(cond, seed, alpha, average, epochs):
    rng = random.Random(seed)
    clf = SGDClassifier(loss="log_loss", alpha=alpha, random_state=seed,
                        learning_rate="optimal", average=average)
    idx = list(range(len(task.train)))
    for ep in range(epochs):
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

print(f"{'alpha':>8}{'avg':>7}{'ep':>4}   {'C1 val':>9}{'C3 val':>9}{'C3-C1':>9}   seeds")
best = None
for alpha, average, epochs in [(1e-7, False, 2), (1e-6, True, 2),
                               (1e-5, True, 2), (1e-5, True, 3)]:
    a1 = [run("C1", s, alpha, average, epochs) for s in (1, 2)]
    a3 = [run("C3", s, alpha, average, epochs) for s in (1, 2)]
    m1, m3 = sum(a1)/2, sum(a3)/2
    sd = max(abs(a1[0]-a1[1]), abs(a3[0]-a3[1]))
    print(f"{alpha:>8.0e}{str(average):>7}{epochs:>4}   {m1:>9.4f}{m3:>9.4f}{m3-m1:>+9.4f}   "
          f"spread {sd:.4f}")
    score = m3 - 0.5*sd
    if best is None or score > best[0]:
        best = (score, alpha, average, epochs)
print(f"\nSELECTED on validation: alpha={best[1]:.0e} average={best[2]} epochs={best[3]}")

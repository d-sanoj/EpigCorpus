#!/usr/bin/env python3
"""Diagnostics A and C: is the province signal REDUNDANT, or is M2 IGNORING it?

A. Of the rows a perfect province+century lookup rescues, how many does a
   text-reading model with NO province already get right? If nearly all, the
   signal is redundant with text and the candidate finding fails as an
   experimental result -- no neural model needed to establish that.

C. Permutation importance. Re-score the C3 model with the province and century
   labels SHUFFLED. If accuracy does not drop, the model is not using them at
   all, and A's answer is about the model rather than about Latin.
"""
from __future__ import annotations
import importlib.util, random, time
from collections import Counter
from pathlib import Path
import numpy as np
REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); M2 = _l("phase5_m2")
from sklearn.linear_model import SGDClassifier

task = C.Task()
test = C.load_split("primary_test")[2]
I = task.I

def train(cond, seed=1):
    rng = random.Random(seed)
    clf = SGDClassifier(loss="log_loss", alpha=1e-7, random_state=seed,
                        learning_rate="optimal")
    idx = list(range(len(task.train)))
    for ep in range(M2.EPOCHS):
        random.Random(seed*1000+ep).shuffle(idx)
        for a in range(0, len(idx), 60000):
            X, y, _ = M2.build(task, [task.train[i] for i in idx[a:a+60000]], cond, rng, True)
            if X.shape[0]: clf.partial_fit(X, y, classes=np.array([0,1], dtype=np.int8))
    return clf

def predict(clf, cond, rows, perturb=None):
    rng = random.Random(1)
    preds = [None]*len(rows)
    for a in range(0, len(rows), 20000):
        sub = rows[a:a+20000]
        if perturb is not None:
            sub = [perturb(r) for r in sub]
        X, _, grp = M2.build(task, sub, cond, rng, False)
        if X.shape[0] == 0:
            for ri, _ in grp: preds[a+ri] = ""
            continue
        sc = clf.decision_function(X); pos = 0
        for ri, cands in grp:
            if not cands: preds[a+ri] = ""; continue
            s = sc[pos:pos+len(cands)]; pos += len(cands)
            preds[a+ri] = cands[int(np.argmax(s))]
    return preds

gold = [task.row_fields(r)["gold"] for r in test]

# ---- the oracle rescue set ------------------------------------------------
kb = {k: c.most_common(1)[0][0] for k, c in task.key_exp.items()}
kpb = {k: c.most_common(1)[0][0] for k, c in task.kp_exp.items()}
kpcb = {k: c.most_common(1)[0][0] for k, c in task.kpc_exp.items()}
rescued = []
for i, r in enumerate(test):
    f = task.row_fields(r)
    a = kb.get(f["key"])
    b = kpb.get((f["key"], f["prov"]), a)
    c3 = kpcb.get((f["key"], f["prov"], f["cent"]), b)
    if a != f["gold"] and c3 == f["gold"]:
        rescued.append(i)
print(f"oracle province+century rescue set: {len(rescued):,} rows "
      f"({100*len(rescued)/len(test):.2f}% of primary_test)\n")

t0 = time.time()
res = {}
for cond in ("C1", "C3"):
    clf = train(cond)
    p = predict(clf, cond, test)
    res[cond] = p
    acc = sum(p[i] == gold[i] for i in range(len(test)))/len(test)
    hit = sum(p[i] == gold[i] for i in rescued)/len(rescued)
    print(f"M2 {cond}: overall {acc:.4f}   on the rescue set {hit:.4f} "
          f"({sum(p[i]==gold[i] for i in rescued):,}/{len(rescued):,})")

print(f"\n--- A: is the province signal redundant with text? ---")
c1_hit = sum(res["C1"][i] == gold[i] for i in rescued)
c3_hit = sum(res["C3"][i] == gold[i] for i in rescued)
print(f"  rows province rescues that TEXT ALONE (C1) already gets: "
      f"{c1_hit:,}/{len(rescued):,} = {100*c1_hit/len(rescued):.1f}%")
print(f"  adding province+century to the text model changes this to "
      f"{c3_hit:,} = {100*c3_hit/len(rescued):.1f}%  ({c3_hit-c1_hit:+,} rows)")

# ---- C: permutation importance -------------------------------------------
print(f"\n--- C: does the C3 model actually USE province/century? ---")
clf3 = train("C3")
base = predict(clf3, "C3", test)
base_acc = sum(base[i] == gold[i] for i in range(len(test)))/len(test)
rng = random.Random(7)
provs = [r[I["province"]] for r in test]; shuf = provs[:]; rng.shuffle(shuf)
def perturb(r):
    r2 = list(r); r2[I["province"]] = shuf[perturb.n]; perturb.n += 1; return r2
perturb.n = 0
perm = predict(clf3, "C3", test, perturb)
perm_acc = sum(perm[i] == gold[i] for i in range(len(test)))/len(test)
print(f"  C3 accuracy, real province labels     : {base_acc:.4f}")
print(f"  C3 accuracy, province labels SHUFFLED : {perm_acc:.4f}")
print(f"  drop from destroying province info    : {base_acc-perm_acc:+.4f}")
print(f"\ntotal {time.time()-t0:.0f}s")

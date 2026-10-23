#!/usr/bin/env python3
"""M2 -- feature-based candidate re-ranker (logistic regression, SGD).

Framed as BINARY re-ranking rather than 62k-way classification: for each row,
the gold candidate is a positive and up to NEG sampled rivals are negatives.
At inference every candidate is scored and the argmax is taken. This keeps the
label space out of the model's parameters and makes M1/M2/M3 directly
comparable (D-0038).

FEATURE SET, stated explicitly as the brief requires. All features are
conjunctions with the candidate -- a feature that does not mention the
candidate cannot discriminate between candidates of the same row.

  cand=<c>                        candidate identity
  key=<k>>c                       key x candidate
  ab<n>=<ngram>>c                 char 2/3/4-grams of the surface abbreviation
  L1..L3=<word>>c, R1..R3=<word>>c   context words either side
  prov=<p>>c                      C2 and C3 only
  cent=<t>>c, prov=<p>|cent=<t>>c C3 only
  [dense] log1p train count of the candidate for the key, for (key,prov),
          for (key,prov,cent), and the candidate's share of the key
"""
from __future__ import annotations
import importlib.util, math, random, re, sys, time
from pathlib import Path
import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier

REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); E = _l("phase5_eval")

NBITS = 20
NEG = 4
EPOCHS = 2
WORD = re.compile(r"[A-Za-z]+")
hv = HashingVectorizer(n_features=2**NBITS, analyzer=lambda x: x,
                       alternate_sign=False, norm=None, binary=True)


def ctx_words(s, n, right):
    w = WORD.findall(s)
    w = w[:n] if right else w[-n:][::-1]
    return (w + [""]*n)[:n]


def feats(task, f, cand, cond):
    k, ab = f["key"], f["abbrev"]
    out = [f"cand={cand}", f"key={k}>{cand}"]
    a = ab.lower()
    for n in (2, 3, 4):
        for i in range(max(0, len(a)-n+1)):
            out.append(f"ab{n}={a[i:i+n]}>{cand}")
    for i, w in enumerate(ctx_words(f["left"], 3, False)):
        out.append(f"L{i+1}={w.lower()}>{cand}")
    for i, w in enumerate(ctx_words(f["right"], 3, True)):
        out.append(f"R{i+1}={w.lower()}>{cand}")
    if cond in ("C2", "C3"):
        out.append(f"prov={f['prov']}>{cand}")
    if cond == "C3":
        out.append(f"cent={f['cent']}>{cand}")
        out.append(f"prov={f['prov']}|cent={f['cent']}>{cand}")
    return out


def dense(task, f, cand, cond):
    k = f["key"]
    tot = sum(task.key_exp[k].values()) or 1
    c0 = task.key_exp[k].get(cand, 0)
    c1 = task.kp_exp.get((k, f["prov"]), {}).get(cand, 0) if cond in ("C2", "C3") else 0
    c2 = task.kpc_exp.get((k, f["prov"], f["cent"]), {}).get(cand, 0) if cond == "C3" else 0
    return [math.log1p(c0), math.log1p(c1), math.log1p(c2), c0/tot]


def build(task, rows, cond, rng, training):
    """Returns (X, y, groups) where groups marks row boundaries for inference."""
    F, D, y, grp = [], [], [], []
    for ri, r in enumerate(rows):
        f = task.row_fields(r)
        cands = task.candidates.get(f["key"])
        if not cands:
            if not training: grp.append((ri, [])); 
            continue
        if training:
            if len(cands) < 2: continue
            if f["gold"] not in task.cand_set[f["key"]]: continue
            negs = [c for c in cands if c != f["gold"]]
            rng.shuffle(negs)
            use = [(f["gold"], 1)] + [(c, 0) for c in negs[:NEG]]
        else:
            use = [(c, 0) for c in cands]
            grp.append((ri, cands))
        for cand, lab in use:
            F.append(feats(task, f, cand, cond))
            D.append(dense(task, f, cand, cond))
            y.append(lab)
    if not F:
        # Every row in this batch has a key unseen in training, so there is no
        # candidate to score. test_unseen_form is entirely such rows: candidate
        # ranking cannot answer them at all (D-0038), and that is reported, not
        # patched around.
        return sparse.csr_matrix((0, 2**NBITS + 4), dtype=np.float32), \
               np.zeros(0, dtype=np.int8), grp
    X = sparse.hstack([hv.transform(F).astype(np.float32),
                       sparse.csr_matrix(np.asarray(D, dtype=np.float32))],
                      format="csr")
    return X, np.asarray(y, dtype=np.int8), grp


def main():
    t0 = time.time()
    task = C.Task()
    lexkeys = E.lexical_keys(task)
    evalsets = {s: C.load_split(s)[2] for s in C.EVAL_SETS}
    print(f"task built {time.time()-t0:.1f}s", file=sys.stderr)

    for cond in C.CONDITIONS:
        if all(C.have_cell("M2", cond, s) for s in C.SEEDS):
            print(f"  M2 {cond}  all seeds cached, skipping"); continue
        for seed in C.SEEDS:
            if C.have_cell("M2", cond, seed):
                print(f"  M2 {cond} seed{seed}  cached, skipping"); continue
            t1 = time.time()
            rng = random.Random(seed)
            # alpha selected on primary_val by scripts/phase5_m2_alpha.py;
            # the test split was not consulted. Validation accuracy rises
            # monotonically as alpha falls (1e-6 .7242, 3e-7 .7320, 1e-7 .7398),
            # so 1e-7 wins. average=True fixes the seed instability that
            # unaveraged SGD showed (a 7-point seed-2 blowup in C2) at a cost
            # of ~4 accuracy points. A published baseline needs reproducible
            # numbers, so stability is bought deliberately. Applied identically
            # to all nine cells.
            clf = SGDClassifier(loss="log_loss", alpha=1e-7, random_state=seed,
                                learning_rate="optimal", average=True)
            CH = 60000
            classes = np.array([0, 1], dtype=np.int8)
            nseen = 0
            for ep in range(EPOCHS):
                idx = list(range(len(task.train)))
                random.Random(seed*1000+ep).shuffle(idx)
                for a in range(0, len(idx), CH):
                    chunk = [task.train[i] for i in idx[a:a+CH]]
                    X, y, _ = build(task, chunk, cond, rng, True)
                    if X.shape[0] == 0: continue
                    clf.partial_fit(X, y, classes=classes)
                    nseen += X.shape[0]
            train_s = time.time()-t1
            print(f"  M2 {cond} seed{seed}  trained on {nseen:,} pairs in {train_s:.0f}s",
                  file=sys.stderr)

            payload = {"train_seconds": round(train_s, 1),
                       "param_count": int(2**NBITS + 4),
                       "epochs": EPOCHS, "negatives_per_row": NEG,
                       "alpha": 1e-7, "averaged_sgd": True,
                       "hparam_selection": "alpha chosen on primary_val only; test untouched",
                       "feature_bits": NBITS, "candidate_cap": C.CAND_CAP,
                       "training_pairs_seen": nseen, "sets": {}}
            t2 = time.time()
            for sname, rows in evalsets.items():
                preds, topk, conf = [None]*len(rows), [[] for _ in rows], [0.0]*len(rows)
                B = 20000
                for a in range(0, len(rows), B):
                    sub = rows[a:a+B]
                    X, _, grp = build(task, sub, cond, rng, False)
                    if X.shape[0] == 0:
                        continue
                    sc = clf.decision_function(X)
                    pos = 0
                    for ri, cands in grp:
                        gi = a+ri
                        if not cands:
                            preds[gi] = ""; continue
                        s = sc[pos:pos+len(cands)]; pos += len(cands)
                        order = np.argsort(-s)
                        ranked = [cands[j] for j in order]
                        preds[gi] = ranked[0]; topk[gi] = ranked[:5]
                        ex = np.exp(s - s.max()); p = ex/ex.sum()
                        conf[gi] = float(p[order[0]])
                payload["sets"][sname] = E.evaluate(task, rows, preds, topk, conf, lexkeys)
                payload["sets"][sname]["ceiling"] = task.reachable(rows)["ceiling"]
            payload["infer_seconds"] = round(time.time()-t2, 1)
            C.save_cell("M2", cond, seed, payload)
            print(f"  M2 {cond} seed{seed}  primary_test acc "
                  f"{payload['sets']['primary_test']['accuracy']:.4f}  "
                  f"(train {train_s:.0f}s, infer {time.time()-t2:.0f}s)")
    print(f"\nM2 done in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()

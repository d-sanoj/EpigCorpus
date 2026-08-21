#!/usr/bin/env python3
"""Shared evaluation. Every cell stores the full Phase 6 breakdown at write
time, so Phase 6 never needs a model re-run to slice the results differently.
"""
from __future__ import annotations
import math
from collections import Counter, defaultdict

TOP_FORMS = ["v", "c", "a", "l", "aug", "d", "f", "p", "m", "s"]


def _lcp(a, b):
    n = 0
    for x, y in zip(a, b):
        if x != y: break
        n += 1
    return n


def _inflectional(a, b):
    if a == b: return True
    n = _lcp(a, b)
    return n >= 3 and n >= 0.6*min(len(a), len(b))


def lexical_keys(task):
    out = set()
    for k, c in task.key_exp.items():
        if len(c) < 2: continue
        e = sorted(c)
        for i in range(len(e)):
            for j in range(i+1, len(e)):
                if not _inflectional(e[i], e[j]):
                    out.add(k); break
            if k in out: break
    return out


def band(freq):
    if freq == 0: return "unseen"
    if freq < 10: return "rare"
    if freq < 1000: return "mid"
    return "common"


def macro_f1(gold, pred):
    labs = set(gold) | set(pred)
    tp = Counter(); fp = Counter(); fn = Counter()
    for g, p in zip(gold, pred):
        if g == p: tp[g] += 1
        else: fp[p] += 1; fn[g] += 1
    tot = 0.0
    for l in labs:
        pr = tp[l]/(tp[l]+fp[l]) if tp[l]+fp[l] else 0.0
        rc = tp[l]/(tp[l]+fn[l]) if tp[l]+fn[l] else 0.0
        tot += 2*pr*rc/(pr+rc) if pr+rc else 0.0
    return tot/len(labs) if labs else 0.0


def abstention_curve(correct, conf, points=21):
    """Accuracy as a function of coverage, when the model declines its least
    confident predictions. Stored now so Phase 6's abstention analysis needs no
    re-run."""
    order = sorted(range(len(conf)), key=lambda i: -conf[i])
    out = []
    for j in range(1, points):
        cov = j/(points-1)
        n = max(1, int(round(cov*len(order))))
        sel = order[:n]
        acc = sum(correct[i] for i in sel)/n
        out.append({"coverage": round(cov, 3), "n": n, "accuracy": round(acc, 6)})
    return out


def evaluate(task, rows, preds, topk, conf, lexkeys):
    """preds: list of predicted expansion (str). topk: list of ranked lists.
    conf: list of float confidence in [0,1]."""
    I = task.I
    gold, key, prov, cent = [], [], [], []
    for r in rows:
        f = task.row_fields(r)
        gold.append(f["gold"]); key.append(f["key"])
        prov.append(f["prov"]); cent.append(f["cent"])
    correct = [1 if p == g else 0 for p, g in zip(preds, gold)]
    n = len(gold)

    def accof(idx):
        return round(sum(correct[i] for i in idx)/len(idx), 6) if idx else None

    freq = [len(task.key_exp.get(k, ())) and sum(task.key_exp[k].values()) or 0 for k in key]
    res = {
        "n": n,
        "accuracy": round(sum(correct)/n, 6) if n else None,
        "macro_f1": round(macro_f1(gold, preds), 6),
        "acc_at_k": {},
        "by_ambiguity": {}, "by_band": {}, "by_province": {}, "by_century": {},
        "per_form_province": {}, "per_form_century": {},
    }
    for k in (1, 3, 5):
        hit = sum(1 for g, tk in zip(gold, topk) if g in tk[:k])
        res["acc_at_k"][f"acc@{k}"] = round(hit/n, 6) if n else None

    lex_idx = [i for i in range(n) if key[i] in lexkeys]
    inf_idx = [i for i in range(n) if key[i] not in lexkeys and len(task.key_exp.get(key[i], ())) > 1]
    una_idx = [i for i in range(n) if len(task.key_exp.get(key[i], ())) <= 1]
    res["by_ambiguity"] = {
        "lexical": {"n": len(lex_idx), "accuracy": accof(lex_idx)},
        "inflectional_only": {"n": len(inf_idx), "accuracy": accof(inf_idx)},
        "unambiguous": {"n": len(una_idx), "accuracy": accof(una_idx)},
    }
    bands = defaultdict(list)
    for i in range(n): bands[band(freq[i])].append(i)
    res["by_band"] = {b: {"n": len(ix), "accuracy": accof(ix)} for b, ix in sorted(bands.items())}

    pidx = defaultdict(list); cidx = defaultdict(list)
    for i in range(n): pidx[prov[i]].append(i); cidx[cent[i]].append(i)
    res["by_province"] = {p: {"n": len(ix), "accuracy": accof(ix)}
                          for p, ix in sorted(pidx.items(), key=lambda x: -len(x[1]))[:25]}
    res["by_century"] = {c: {"n": len(ix), "accuracy": accof(ix)}
                         for c, ix in sorted(cidx.items(), key=lambda x: -len(x[1]))[:14]}

    for form in TOP_FORMS:
        fp = defaultdict(list); fc = defaultdict(list)
        for i in range(n):
            if key[i] == form:
                fp[prov[i]].append(i); fc[cent[i]].append(i)
        if fp:
            res["per_form_province"][form] = {
                p: {"n": len(ix), "accuracy": accof(ix)}
                for p, ix in sorted(fp.items(), key=lambda x: -len(x[1]))[:12]}
            res["per_form_century"][form] = {
                c: {"n": len(ix), "accuracy": accof(ix)}
                for c, ix in sorted(fc.items(), key=lambda x: -len(x[1]))[:10]}
    res["abstention"] = abstention_curve(correct, conf)
    res["confidence_hist"] = dict(Counter(round(min(0.999, c), 1) for c in conf))
    return res

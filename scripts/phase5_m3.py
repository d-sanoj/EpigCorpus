#!/usr/bin/env python3
"""M3 -- fine-tuned Latin encoder (bowphs/LaBerta), candidate ranking.

Architecture. The abbreviation is marked in its context with explicit
delimiters, the sequence is encoded, and the pooled representation is scored
against a learned embedding for each candidate expansion. Scoring is
restricted at inference to the key's train-derived candidates (D-0038), so
M1/M2/M3 all choose from the same set.

Context conditions are injected as text prefixes, not as extra parameters, so
the ONLY thing that changes between C1/C2/C3 is what the model is told:
  C1  <text>
  C2  province: <p> | <text>
  C3  province: <p> | century: <t> | <text>

Every cell is written to results/cells/ the moment it finishes, and a cell
that already exists is skipped, so an interrupted sweep resumes.
"""
from __future__ import annotations
import argparse, importlib.util, json, math, os, random, sys, time
from pathlib import Path
import numpy as np
import torch
import torch.nn as nn


REPO = Path(__file__).resolve().parent.parent
def _l(n):
    s = importlib.util.spec_from_file_location(n, REPO/"scripts"/f"{n}.py")
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
C = _l("phase5_common"); E = _l("phase5_eval")

MAXLEN = 64
BATCH = 128
LR = 3e-5
EPOCHS = 1
MIN_EXP_FREQ = 2          # expansions rarer than this share an <unk> label row


def device():
    if torch.backends.mps.is_available(): return torch.device("mps")
    if torch.cuda.is_available(): return torch.device("cuda")
    return torch.device("cpu")


def make_text(f, cond):
    core = f"{f['left']} [ {f['abbrev']} ] {f['right']}".strip()
    if cond == "C1": return core
    if cond == "C2": return f"province: {f['prov']} | {core}"
    return f"province: {f['prov']} | century: {f['cent']} | {core}"


def encode_all(task, rows, cond, tok, lab2id):
    """Tokenise the whole split once, batched. Per-item tokenisation inside the
    training loop was serialising the tokeniser against the GPU."""
    texts = []
    ys = []
    for r in rows:
        f = task.row_fields(r)
        texts.append(make_text(f, cond))
        ys.append(lab2id.get(f["gold"], -100))
    enc = tok(texts, truncation=True, max_length=MAXLEN)["input_ids"]
    return enc, ys


def batches(enc, ys, bs, shuffle, seed=0):
    """Length-bucketed batches, padded to the batch maximum rather than to
    MAXLEN. Mean token length is 45.6 against a 64 cap, so fixed padding was
    wasting 29% of every forward pass."""
    idx = sorted(range(len(enc)), key=lambda i: len(enc[i]))
    groups = [idx[a:a+bs] for a in range(0, len(idx), bs)]
    if shuffle:
        random.Random(seed).shuffle(groups)
    for g in groups:
        mx = max(len(enc[i]) for i in g)
        ids = torch.zeros(len(g), mx, dtype=torch.long)
        msk = torch.zeros(len(g), mx, dtype=torch.long)
        for j, i in enumerate(g):
            L = len(enc[i])
            ids[j, :L] = torch.tensor(enc[i], dtype=torch.long)
            msk[j, :L] = 1
        yield ids, msk, torch.tensor([ys[i] for i in g]), g


class Ranker(nn.Module):
    def __init__(self, enc, hidden, nlab):
        super().__init__()
        self.enc = enc
        self.drop = nn.Dropout(0.1)
        self.out = nn.Linear(hidden, nlab)
    def forward(self, ids, mask):
        h = self.enc(input_ids=ids, attention_mask=mask).last_hidden_state
        m = mask.unsqueeze(-1).float()
        pooled = (h*m).sum(1) / m.sum(1).clamp(min=1e-6)
        return self.out(self.drop(pooled))


def run_cell(task, lexkeys, evalsets, model_id, tag, cond, seed, args):
    from transformers import AutoTokenizer, AutoModel
    if C.have_cell(tag, cond, seed):
        print(f"  {tag} {cond} seed{seed}  cached, skipping", flush=True); return
    torch.manual_seed(seed); np.random.seed(seed); random.seed(seed)
    dev = device()
    tok = AutoTokenizer.from_pretrained(model_id)
    enc = AutoModel.from_pretrained(model_id)
    hidden = enc.config.hidden_size

    exp_freq = {}
    for k, c in task.key_exp.items():
        for e, n in c.items(): exp_freq[e] = exp_freq.get(e, 0) + n
    labels = sorted(e for e, n in exp_freq.items() if n >= MIN_EXP_FREQ)
    lab2id = {e: i for i, e in enumerate(labels)}
    model = Ranker(enc, hidden, len(labels)).to(dev)
    nparam = sum(p.numel() for p in model.parameters())

    train_rows = task.train
    if args.train_subsample and args.train_subsample < len(train_rows):
        r = random.Random(seed); train_rows = r.sample(train_rows, args.train_subsample)
    tr_enc, tr_y = encode_all(task, train_rows, cond, tok, lab2id)
    opt = torch.optim.AdamW(model.parameters(), lr=LR)
    lossf = nn.CrossEntropyLoss(ignore_index=-100)
    total = EPOCHS*max(1, math.ceil(len(tr_enc)/BATCH))
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=LR, total_steps=max(total,1),
                                                pct_start=0.1)
    t1 = time.time(); model.train(); step = 0
    for ep in range(EPOCHS):
        for ids, mask, y, _ in batches(tr_enc, tr_y, BATCH, True, seed*100+ep):
            ids, mask, y = ids.to(dev), mask.to(dev), y.to(dev)
            opt.zero_grad()
            loss = lossf(model(ids, mask), y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step(); sched.step(); step += 1
            if step % 500 == 0:
                el = time.time()-t1
                print(f"    step {step}/{total}  loss {loss.item():.3f}  "
                      f"{step/el:.1f} it/s  eta {(total-step)/(step/el)/60:.0f}m", flush=True)
    train_s = time.time()-t1

    payload = {"train_seconds": round(train_s, 1), "param_count": int(nparam),
               "encoder": model_id, "label_space": len(labels), "epochs": EPOCHS,
               "batch": BATCH, "lr": LR, "max_len": MAXLEN, "device": str(dev),
               "train_rows_used": len(train_rows), "candidate_cap": C.CAND_CAP,
               "sets": {}}
    model.eval(); t2 = time.time()
    SUBSETS = {"test_lexical_only", "test_lexical_hard", "test_no_context_duplicate"}
    logit_cache = {}
    for sname, rows in evalsets.items():
        if sname in SUBSETS:
            continue                      # derived below, no second forward pass
        ev_enc, ev_y = encode_all(task, rows, cond, tok, lab2id)
        L = np.zeros((len(rows), len(labels)), dtype=np.float32)
        with torch.no_grad():
            for ids, mask, _, g in batches(ev_enc, ev_y, 256, False):
                out = model(ids.to(dev), mask.to(dev)).float().cpu().numpy()
                for j, i in enumerate(g):
                    L[i] = out[j]
        logit_cache[sname] = L
        preds, topk, conf = [], [], []
        for i, r in enumerate(rows):
            f = task.row_fields(r)
            cands = task.candidates.get(f["key"]) or []
            ids_ = [lab2id[c] for c in cands if c in lab2id]
            keep = [c for c in cands if c in lab2id]
            if not keep:
                preds.append(cands[0] if cands else ""); topk.append(cands[:5]); conf.append(0.0); continue
            s = L[i, ids_]
            o = np.argsort(-s)
            ranked = [keep[j] for j in o]
            ex = np.exp(s - s.max()); p = ex/ex.sum()
            preds.append(ranked[0]); topk.append(ranked[:5]); conf.append(float(p[o[0]]))
        payload["sets"][sname] = E.evaluate(task, rows, preds, topk, conf, lexkeys)
        payload["sets"][sname]["ceiling"] = task.reachable(rows)["ceiling"]

    # derive the three primary_test subsets from the cached logits
    ptest = evalsets["primary_test"]
    pos = {}
    for i, r in enumerate(ptest):
        pos[(r[0], r[task.I["abbrev"]], r[task.I["left_context"]],
             r[task.I["right_context"]])] = i
    Lp = logit_cache["primary_test"]
    for sname in SUBSETS:
        rows = evalsets[sname]
        preds, topk, conf = [], [], []
        for r in rows:
            k = (r[0], r[task.I["abbrev"]], r[task.I["left_context"]],
                 r[task.I["right_context"]])
            i = pos.get(k)
            f = task.row_fields(r)
            cands = task.candidates.get(f["key"]) or []
            keep = [c for c in cands if c in lab2id]
            if i is None or not keep:
                preds.append(cands[0] if cands else ""); topk.append(cands[:5]); conf.append(0.0)
                continue
            s_ = Lp[i, [lab2id[c] for c in keep]]
            o = np.argsort(-s_)
            ranked = [keep[j] for j in o]
            ex = np.exp(s_ - s_.max()); p = ex/ex.sum()
            preds.append(ranked[0]); topk.append(ranked[:5]); conf.append(float(p[o[0]]))
        payload["sets"][sname] = E.evaluate(task, rows, preds, topk, conf, lexkeys)
        payload["sets"][sname]["ceiling"] = task.reachable(rows)["ceiling"]
    payload["infer_seconds"] = round(time.time()-t2, 1)
    C.save_cell(tag, cond, seed, payload)
    print(f"  {tag} {cond} seed{seed}  primary_test acc "
          f"{payload['sets']['primary_test']['accuracy']:.4f}  "
          f"(train {train_s/60:.1f}m, infer {(time.time()-t2)/60:.1f}m)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="bowphs/LaBerta")
    ap.add_argument("--tag", default="M3")
    ap.add_argument("--conditions", default="C1,C2,C3")
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--train-subsample", type=int, default=0)
    ap.add_argument("--smoke", type=int, default=0,
                    help="cap every eval set to N rows; for testing the code path only")
    args = ap.parse_args()
    task = C.Task(); lexkeys = E.lexical_keys(task)
    evalsets = {s: C.load_split(s)[2] for s in C.EVAL_SETS}
    if args.smoke:
        evalsets = {k: v[:args.smoke] for k, v in evalsets.items()}
    for cond in args.conditions.split(","):
        for seed in [int(s) for s in args.seeds.split(",")]:
            run_cell(task, lexkeys, evalsets, args.model, args.tag, cond, seed, args)

if __name__ == "__main__":
    main()

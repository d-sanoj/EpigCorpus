#!/usr/bin/env python3
"""Phase 5 shared infrastructure: splits, candidate sets, features, caching.

Every (model, context, seed) cell writes results/cells/<id>.json the moment it
finishes and is skipped on a later run. An interrupted sweep resumes; nothing
is ever recomputed.

D-0038: this is a CANDIDATE RANKING task. For a key, the candidates are the
expansions observed for that key in primary_train. All three models score the
same set, so the C1->C2->C3 delta is measured identically across them.
"""
from __future__ import annotations
import csv, json, os, sys, time
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SPLITS = REPO/"data"/"derived"/"v1"/"splits"
CELLS = REPO/"results"/"cells"
CELLS.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 2, 3]
CONDITIONS = ["C1", "C2", "C3"]
# Candidates per key are capped by training frequency. Uncapped, the head keys
# carry 500+ expansions and inference cost explodes. The cost of the cap is
# measured and reported as an accuracy ceiling, not hidden.
CAND_CAP = 50

csv.field_size_limit(10**9)


def load_split(name):
    p = SPLITS/f"{name}.tsv"
    if not p.exists():
        p = SPLITS/f"{name}.tsv.gz"
        import gzip
        fh = gzip.open(p, "rt", encoding="utf-8", newline="")
    else:
        fh = p.open(encoding="utf-8", newline="")
    with fh:
        rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(rd)
        I = {c: i for i, c in enumerate(header)}
        rows = [r for r in rd]
    return header, I, rows


def century_of(a, b):
    """Single-century label, else None. Mirrors abbrev_probe exactly."""
    try:
        ya, yb = int(a), int(b)
    except (ValueError, TypeError):
        return None
    ca = (ya // 100) + (1 if ya >= 0 else 0)
    cb = (yb // 100) + (1 if yb >= 0 else 0)
    if ca != cb:
        return None
    return f"{abs(ca)}{'AD' if ca > 0 else 'BC'}"


class Task:
    """Everything derived from the TRAINING split, and nothing from any test set.

    train_split is a parameter because the held-out province experiment needs a
    model that has never seen Britannia, Mauretania Caesariensis or Pannonia
    inferior. Training on primary_train and evaluating on heldout_province_test
    measures performance on three provinces the model was trained on, which is
    not the question 4b asks.
    """

    def __init__(self, train_split="primary_train"):
        self.train_split = train_split
        self.header, self.I, self.train = load_split(train_split)
        I = self.I
        self.key_exp = defaultdict(Counter)          # key -> expansion counts
        self.kp_exp = defaultdict(Counter)           # (key, province)
        self.kpc_exp = defaultdict(Counter)          # (key, province, century)
        self.kc_exp = defaultdict(Counter)           # (key, century)
        for r in self.train:
            k = r[I["abbrev_collapsed"]].lower()
            e = r[I["corrected_expansion"]].lower()
            p = r[I["province"]] or "UNK"
            c = century_of(r[I["date_from"]], r[I["date_to"]]) or "UNK"
            self.key_exp[k][e] += 1
            self.kp_exp[(k, p)][e] += 1
            self.kpc_exp[(k, p, c)][e] += 1
            self.kc_exp[(k, c)][e] += 1
        self.candidates = {k: [e for e, _ in c.most_common(CAND_CAP)]
                           for k, c in self.key_exp.items()}
        self.cand_set = {k: set(v) for k, v in self.candidates.items()}

    def row_fields(self, r):
        I = self.I
        return {
            "key": r[I["abbrev_collapsed"]].lower(),
            "gold": r[I["corrected_expansion"]].lower(),
            "abbrev": r[I["abbrev"]],
            "left": r[I["left_context"]],
            "right": r[I["right_context"]],
            "prov": r[I["province"]] or "UNK",
            "cent": century_of(r[I["date_from"]], r[I["date_to"]]) or "UNK",
            "id": r[0],
        }

    def reachable(self, rows):
        """Rows whose gold is inside the capped candidate set. Everything else
        is unreachable BY CONSTRUCTION and caps accuracy below 100%."""
        n = ok = nocand = 0
        for r in rows:
            f = self.row_fields(r)
            n += 1
            cs = self.cand_set.get(f["key"])
            if cs is None:
                nocand += 1
            elif f["gold"] in cs:
                ok += 1
        return {"rows": n, "gold_in_candidates": ok,
                "key_unseen_in_train": nocand,
                "ceiling": round(ok/n, 6) if n else None}


def cell_path(model, cond, seed, suffix=""):
    return CELLS/f"{model}_{cond}_seed{seed}{suffix}.json"


def have_cell(model, cond, seed):
    return cell_path(model, cond, seed).exists()


def save_cell(model, cond, seed, payload):
    p = cell_path(model, cond, seed)
    payload = dict(payload)
    payload.update({"model": model, "condition": cond, "seed": seed,
                    "written_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, p)          # atomic: a killed process never leaves a partial cell
    return p


def load_cell(model, cond, seed):
    return json.loads(cell_path(model, cond, seed).read_text(encoding="utf-8"))


EVAL_SETS = ["primary_test", "primary_val", "heldout_province_test",
             "test_lexical_only", "test_lexical_hard", "test_rare_form",
             "test_unseen_form", "test_no_context_duplicate"]

if __name__ == "__main__":
    t0 = time.time()
    T = Task()
    print(f"train rows            {len(T.train):,}")
    print(f"keys with candidates  {len(T.candidates):,}")
    print(f"candidate cap         {CAND_CAP}")
    tot = sum(len(v) for v in T.candidates.values())
    print(f"total candidates      {tot:,}   mean {tot/len(T.candidates):.2f} per key")
    print(f"build time            {time.time()-t0:.1f}s")
    print()
    for s in ["primary_test", "heldout_province_test", "test_rare_form"]:
        _, _, rows = load_split(s)
        r = T.reachable(rows)
        print(f"{s:<26} rows {r['rows']:>8,}  ceiling {r['ceiling']:.4f}  "
              f"key unseen {r['key_unseen_in_train']:>6,}")

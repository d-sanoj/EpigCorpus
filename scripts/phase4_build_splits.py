#!/usr/bin/env python3
"""Phase 4: seeded, documented, frozen dataset splits.

Writes data/derived/v1/splits/. Everything is deterministic from SEED and the
frozen v1 table -- no RNG state is carried between steps, so any split file
can be regenerated alone.

Grouping unit is the EDCS BASE ID, not record_id. D-0004 established that
588,509 records are only 542,854 monuments: the -0/-1/-2 suffix marks faces or
panels of one stone, 100% of multi-segment groups share a province, and 299
groups carry byte-identical text. Splitting on record_id would put two faces of
one monument on opposite sides of the train/test line.
"""
from __future__ import annotations
import csv, hashlib, json, sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
V1 = REPO/"data"/"derived"/"v1"/"abbrev_pairs_v1.tsv"
OUT = REPO/"data"/"derived"/"v1"/"splits"
SEED = 20260820

# 4b. Held-out provinces, chosen on the stated criteria in
# reports/phase4_splits.md BEFORE any split was drawn: one province per genre
# regime (funerary / votive / military, as separated by the V reading in
# Phase 3f), three widely separated geographic zones, comparable size, and for
# each a same-regime sister province left in training so the test measures
# transfer rather than absence.
HELDOUT_PROVINCES = {
    "Mauretania Caesariensis",   # funerary  (V -> vixit 81%)   North Africa
    "Pannonia inferior",         # votive    (V -> votum 79%)   Danube
    "Britannia",                 # military  (V -> victrix 36%, valeria 30%)
}

# 4d. An abbreviation is RARE below this many occurrences IN TRAINING.
# Justified against the measured frequency distribution in the report.
RARE_N = 10

# 4c. A key is eligible for the lexical set only with enough observations to
# score. D-0024: over keys at this threshold, lexical ambiguity is 54.5%.
LEX_MIN_N = 20


def bucket(base_id: str) -> float:
    """Stable uniform [0,1) from the group id. Not Python's hash(): that is
    salted per process. Not random.shuffle over a list: that depends on the
    order rows happen to be read. This depends only on SEED and the id."""
    h = hashlib.blake2b(f"{SEED}:{base_id}".encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") / 2**64


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    with V1.open(encoding="utf-8", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(rd); I = {c: i for i, c in enumerate(header)}
        for r in rd:
            rows.append(r)
    print(f"v1 rows: {len(rows):,}", file=sys.stderr)

    # ---- the task population -------------------------------------------
    task, held_out_of_task = [], Counter()
    for r in rows:
        if r[I["excluded_reason"]]:
            held_out_of_task[r[I["excluded_reason"]]] += 1
            continue
        task.append(r)
    print(f"task rows: {len(task):,}  (excluded: {dict(held_out_of_task)})", file=sys.stderr)

    # ---- 4e. rows barred from any TEST set ------------------------------
    # The brief names circularity_risk. D-0027 established it is 0.0 on every
    # v1 row -- kept pairs sit wholly outside brackets by construction -- so
    # that rule bars nothing here, and the count is reported rather than
    # quietly dropped. Two further bars are MY decision, stated so they are
    # reversible: a test set containing `ix -> ixit` measures the extractor,
    # not the model.
    def test_barred(r):
        reasons = []
        try:
            if float(r[I["circularity_risk"]]) > 0.0:
                reasons.append("circularity_risk>0")
        except ValueError:
            reasons.append("circularity_risk_unparseable")
        if r[I["linebreak_fragment"]] == "1":
            reasons.append("linebreak_fragment")
        if r[I["confidence"]] == "UNRESOLVED":
            reasons.append("unresolved_correction")
        if r[I["date_flag"]] == "MISKEYED":
            reasons.append("miskeyed_date")
        return reasons

    bar_ct = Counter(); barred = set()
    for i, r in enumerate(task):
        rs = test_barred(r)
        if rs:
            barred.add(i)
            for x in rs: bar_ct[x] += 1

    # ---- 4a. primary split, grouped by base id --------------------------
    assign = {}
    for r in task:
        b = r[0].rsplit("-", 1)[0]
        if b not in assign:
            u = bucket(b)
            assign[b] = "train" if u < 0.80 else ("val" if u < 0.90 else "test")
    prim = defaultdict(list)
    for i, r in enumerate(task):
        s = assign[r[0].rsplit("-", 1)[0]]
        if s in ("val", "test") and i in barred:
            prim["barred_from_test"].append(i); continue
        prim[s].append(i)

    # ---- 4b. held-out province split ------------------------------------
    hp = defaultdict(list)
    for i, r in enumerate(task):
        if (r[I["province"]] or "") in HELDOUT_PROVINCES:
            if i in barred: hp["barred_from_test"].append(i)
            else: hp["heldout_test"].append(i)
        else:
            hp["heldout_train"].append(i)

    # ---- statistics computed on the PRIMARY TRAIN ONLY ------------------
    # Deriving "rare" or "lexically ambiguous" from the whole corpus would let
    # the definition of the test set peek at the test set.
    train_key_freq = Counter()
    train_key_exp = defaultdict(Counter)
    for i in prim["train"]:
        r = task[i]
        k = r[I["abbrev_collapsed"]].lower()
        train_key_freq[k] += 1
        train_key_exp[k][r[I["corrected_expansion"]].lower()] += 1

    def lcp(a, b):
        n = 0
        for x, y in zip(a, b):
            if x != y: break
            n += 1
        return n
    def inflectional(a, b):
        if a == b: return True
        n = lcp(a, b)
        return n >= 3 and n >= 0.6*min(len(a), len(b))
    def is_lexical(exps):
        e = sorted(exps)
        for x in range(len(e)):
            for y in range(x+1, len(e)):
                if not inflectional(e[x], e[y]): return True
        return False

    lex_keys = {k for k, c in train_key_exp.items()
                if train_key_freq[k] >= LEX_MIN_N and len(c) > 1 and is_lexical(list(c))}

    # ---- majority reading, from TRAIN only ------------------------------
    train_majority = {k: c.most_common(1)[0][0] for k, c in train_key_exp.items()}

    # ---- exact-context duplication across the train/test line -----------
    # Grouping by monument stops two faces of one stone from straddling. It
    # does NOT stop two DIFFERENT stones carrying byte-identical text: the
    # corpus is formulaic and the context window is 40 characters, so
    # "Leg(io)" inside the same surrounding string recurs across the empire.
    # Measured, reported, and offered as a de-duplicated test variant rather
    # than silently left in or silently removed.
    def ctxkey(r):
        return (r[I["abbrev"]], r[I["expansion"]],
                r[I["left_context"]], r[I["right_context"]])
    train_ctx = {ctxkey(task[i]) for i in prim["train"]}

    # ---- 4c / 4d, carved from the PRIMARY TEST split --------------------
    lex_test = [i for i in prim["test"]
                if task[i][I["abbrev_collapsed"]].lower() in lex_keys]
    rare_test = [i for i in prim["test"]
                 if train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) < RARE_N]
    unseen_test = [i for i in prim["test"]
                   if train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) == 0]
    # primary test with every row whose exact context also occurs in train removed
    nodup_test = [i for i in prim["test"] if ctxkey(task[i]) not in train_ctx]
    # the discriminating difficulty set: a lexically ambiguous key AND a gold
    # label the most-frequent-expansion baseline gets wrong. test_lexical_only
    # is 88% of the test split because the head keys are all lexically
    # ambiguous, so it does not discriminate on its own.
    lex_hard = [i for i in prim["test"]
                if task[i][I["abbrev_collapsed"]].lower() in lex_keys
                and train_majority.get(task[i][I["abbrev_collapsed"]].lower())
                    != task[i][I["corrected_expansion"]].lower()]

    # ---- write ----------------------------------------------------------
    def write(name, idxs):
        p = OUT/f"{name}.tsv"
        with p.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, delimiter="\t", quoting=csv.QUOTE_NONE,
                           escapechar="\\", lineterminator="\n")
            w.writerow(header)
            for i in idxs: w.writerow(task[i])
        return len(idxs)

    written = {}
    for n, idxs in (("primary_train", prim["train"]), ("primary_val", prim["val"]),
                    ("primary_test", prim["test"]),
                    ("heldout_province_train", hp["heldout_train"]),
                    ("heldout_province_test", hp["heldout_test"]),
                    ("test_lexical_only", lex_test),
                    ("test_rare_form", rare_test),
                    ("test_unseen_form", unseen_test),
                    ("test_no_context_duplicate", nodup_test),
                    ("test_lexical_hard", lex_hard),
                    ("barred_from_test_primary", prim["barred_from_test"]),
                    ("barred_from_test_heldout", hp["barred_from_test"])):
        written[n] = write(n, idxs)
        print(f"  {n:<28}{written[n]:>10,}", file=sys.stderr)

    # ---- 4a leak verification -------------------------------------------
    grp = defaultdict(set)
    for split in ("train", "val", "test"):
        for i in prim[split]:
            grp[task[i][0].rsplit("-", 1)[0]].add(split)
    straddle = {g: sorted(s) for g, s in grp.items() if len(s) > 1}

    grp2 = defaultdict(set)
    for nm, key in (("heldout_train", "heldout_train"), ("heldout_test", "heldout_test")):
        for i in hp[key]:
            grp2[task[i][0].rsplit("-", 1)[0]].add(nm)
    straddle2 = {g: sorted(s) for g, s in grp2.items() if len(s) > 1}

    # exact-duplicate-text leakage: same (abbrev, expansion, context) across sides
    def ctxset(idxs):
        return {(task[i][I["abbrev"]], task[i][I["expansion"]],
                 task[i][I["left_context"]], task[i][I["right_context"]]) for i in idxs}
    tr, te = ctxset(prim["train"]), ctxset(prim["test"])
    dup_ctx = len(tr & te)
    dup_rows = sum(1 for i in prim["test"] if ctxkey(task[i]) in train_ctx)
    maj_hit = sum(1 for i in prim["test"]
                  if train_majority.get(task[i][I["abbrev_collapsed"]].lower())
                     == task[i][I["corrected_expansion"]].lower())

    stats = {
        "seed": SEED, "rare_n": RARE_N, "lex_min_n": LEX_MIN_N,
        "heldout_provinces": sorted(HELDOUT_PROVINCES),
        "v1_rows": len(rows), "task_rows": len(task),
        "excluded_from_task": dict(held_out_of_task),
        "barred_from_test_counts": dict(bar_ct),
        "barred_rows_total": len(barred),
        "written": written,
        "leak_check": {
            "primary_groups_total": len(grp),
            "primary_groups_straddling_splits": len(straddle),
            "heldout_groups_straddling": len(straddle2),
            "identical_context_types_shared_train_test": dup_ctx,
            "test_rows_with_exact_context_in_train": dup_rows,
            "test_rows_with_exact_context_in_train_pct": round(100*dup_rows/len(prim["test"]), 2),
            "majority_baseline_on_primary_test": maj_hit,
            "majority_baseline_on_primary_test_pct": round(100*maj_hit/len(prim["test"]), 2),
            "train_context_types": len(tr), "test_context_types": len(te),
        },
        "train_key_freq_bands": {
            "0 (unseen in train)": sum(1 for i in prim["test"]
                if train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) == 0),
            "1-9": sum(1 for i in prim["test"]
                if 0 < train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) < 10),
            "10-99": sum(1 for i in prim["test"]
                if 10 <= train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) < 100),
            "100-999": sum(1 for i in prim["test"]
                if 100 <= train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) < 1000),
            ">=1000": sum(1 for i in prim["test"]
                if train_key_freq.get(task[i][I["abbrev_collapsed"]].lower(), 0) >= 1000),
        },
        "lexical_keys_from_train": len(lex_keys),
        "train_keys_total": len(train_key_freq),
    }
    (OUT/"split_manifest.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1),
                                           encoding="utf-8")
    print(f"\nLEAK CHECK  groups={len(grp):,}  straddling={len(straddle)}  "
          f"heldout straddling={len(straddle2)}  identical-context shared={dup_ctx}")
    print("written", OUT/"split_manifest.json")

if __name__ == "__main__":
    main()

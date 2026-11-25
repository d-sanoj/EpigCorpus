#!/usr/bin/env python3
"""Reproduce the numbers behind the 'tree' crash story.

Searching 'tree' on EpigCorpus used to blank the whole page. The cause
was one inscription, one place, and a map zoom of Infinity: the old
substring match found 'Matree' (EDCS-29900632, Budapest / Aquincum),
the only match in Raw and Conservative mode, and fitting one place
asked Leaflet for an infinite zoom, which the MapLibre basemap could
not render. Fixed in PR #3 (whole-word search, single place centred
at zoom 10, maxZoom 18).

This script re-runs the counts with the real search code
(backend/src/epigcorpus/core/search.py) over the cleaned corpus and
writes research/results/tree_crash_verification.json. Read-only over
data/; writes only that one results file. Run with the backend
environment, which has the epigcorpus package installed:

    uv run --project backend python research/scripts/tree_crash_verification.py

Numbers (corpus 588,509 records, as of 2026-11-24):
  'tree' old substring: 1 row Raw, 1 row Conservative, 2 rows
      Interpretive (Matree, plus patreEm in EDCS-29200159)
  'tree'/'TREE' fixed whole-word: 0 rows in all three modes
  'tre' substring (Conservative): 3,599 rows, renders fine
  'matree': 1 row, 1 place; 'popidium': 154 rows, 1 place
  'viator': 379 rows, 213 places; 'vixit': 24,724 rows, 2,092 places
      (Conservative)
Also published as the blog post "Searching 'tree' crashed the whole
page" (sanoj.dev, 2026-11-24), which ships this script as
verify-tree.py with its results.json.
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import pandas as pd

def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for cand in [here.parents[2]] + list(here.parents):
        if (cand / "backend" / "src" / "epigcorpus" / "core" / "search.py").exists():
            return cand
    raise SystemExit("could not find the EpigCorpus repo root from " + str(here))


sys.path.insert(0, str(_repo_root() / "backend" / "src"))

from epigcorpus.config.paths import CLEANED_JSONL_GZ, RESULTS_DIR  # noqa: E402
from epigcorpus.core.search import (  # noqa: E402
    SEARCH_COLUMNS, aggregate_places, filter_inscriptions)


def main() -> int:
    rows = []
    with gzip.open(CLEANED_JSONL_GZ, "rt", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    out: dict = {"corpus_rows": len(df), "modes": {}, "contrasts": {}}

    for label, col in SEARCH_COLUMNS.items():
        text = df[col].fillna("").astype(str)
        old = df[text.str.contains("tree", case=False, na=False, regex=False)]
        fixed = filter_inscriptions(df, col, "tree")
        fixed_upper = filter_inscriptions(df, col, "TREE")
        mode = {
            "old_substring_tree_rows": int(len(old)),
            "old_substring_tree_places": sorted(old["place"].dropna().unique().tolist()),
            "fixed_whole_word_tree_rows": int(len(fixed)),
            "fixed_whole_word_TREE_rows": int(len(fixed_upper)),
        }
        if len(old):
            mode["old_substring_tree_aggregate"] = aggregate_places(old).to_dict("records")
            mode["old_substring_tree_match"] = [
                {"edcs_id": r["edcs_id"], "place": r["place"],
                 "province": r["province"], "text": r[col]}
                for _, r in old.iterrows()
            ]
        out["modes"][label] = mode

    cons = "inscription_text_conservative"
    text = df[cons].fillna("").astype(str)
    tre = df[text.str.contains("tre", case=False, na=False, regex=False)]
    out["contrasts"]["tre_substring_rows_conservative"] = int(len(tre))
    for term in ("matree", "popidium", "viator", "vixit"):
        m = filter_inscriptions(df, cons, term)
        out["contrasts"][term] = {
            "rows": int(len(m)),
            "places": int(len(aggregate_places(m))),
        }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    target = RESULTS_DIR / "tree_crash_verification.json"
    target.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

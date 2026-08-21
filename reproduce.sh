#!/usr/bin/env bash
# Reproduce every number and figure in the paper from the raw corpus.
#
#   ./reproduce.sh          full pipeline (models included, hours)
#   ./reproduce.sh --fast   everything except model training (minutes)
#
# Determinism: every stochastic step is seeded (SEED=20260820). The extraction
# is a pure function of the corpus and reproduces byte-identically under any
# PYTHONHASHSEED. Model cells are checkpointed to results/cells/ and skipped if
# present, so an interrupted run resumes.
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
FAST=${1:-}

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "0. verify the input corpus is the one every number was computed on"
EXPECT=9ebea1a7a5742d055af3b7059703cd8fd1ea708578c3ea43b9882f5873242317
if [ ! -f data/edcs_inscriptions.jsonl ]; then
  echo "   decompressing committed corpus…"
  gunzip -kf data/edcs_inscriptions.jsonl.gz
fi
GOT=$(shasum -a 256 data/edcs_inscriptions.jsonl | cut -d' ' -f1)
[ "$GOT" = "$EXPECT" ] || { echo "CORPUS HASH MISMATCH"; echo " expected $EXPECT"; echo " got      $GOT"; exit 1; }
echo "   corpus sha256 OK"

step "1. extraction (Phase 0) — expect 1,424,314 pairs"
$PY scripts/abbrev_probe.py > /dev/null
$PY scripts/phase0_verify.py | head -20

step "2. exclusion audit (Phase 1)"
$PY scripts/exclusion_audit.py > /dev/null
$PY scripts/phase1_supplement.py
$PY scripts/phase1_supplement2.py > /dev/null
$PY scripts/phase1_supplement3.py

step "3. vinculum census (Phase 2)"
$PY scripts/phase2_vinculum.py | head -14
$PY scripts/phase2_crossref.py > /dev/null
$PY scripts/phase2_numeral_fusion.py | head -6

step "4. corrections (Phase 3) — writes data/derived/v1/"
$PY scripts/phase3_build_v1.py
$PY scripts/phase3_diff.py | head -16

step "5. splits (Phase 4) — FROZEN"
$PY scripts/phase4_build_splits.py
$PY scripts/phase4_split_stats.py | head -14

step "6. editorial consistency (Phase 7)"
$PY scripts/phase7_editor_consistency.py | head -12

if [ "$FAST" != "--fast" ]; then
  step "7. models (Phase 5) — cached cells are skipped"
  $PY scripts/phase5_m1.py
  $PY scripts/phase5_m2.py
  $PY scripts/phase5_heldout.py
  HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
    $PY scripts/phase5_m3.py --seeds 1 --conditions C1,C2,C3 --train-subsample 100000
else
  step "7. models — SKIPPED (--fast); using the committed cells in results/cells/"
fi

step "8. aggregate every number into one file"
$PY scripts/build_all_results.py

step "9. figures"
$PY scripts/make_figures.py

step "10. tests"
$PY -m pytest tests/ -q

printf '\n\033[1mDONE.\033[0m Numbers: results/all_results.json   Figures: figures/\n'

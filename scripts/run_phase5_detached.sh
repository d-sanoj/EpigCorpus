#!/bin/bash
# Detached Phase 5 driver -- PRIORITY STAGES ONLY.
# Stops after M3 seed 1 so the core experiment can be reviewed before
# committing another 3-4 hours to seeds 2 and 3.
#
# HF_HUB_OFFLINE stops transformers phoning home; that network hang was what
# stalled the first M3 attempt. Every cell is checkpointed, so killing this at
# any point loses at most the cell in flight.
cd /Users/sanoj/Documents/Projects/EpigCorpus
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 TOKENIZERS_PARALLELISM=false
PY=.venv/bin/python
LOG=results/phase5_run.log
echo "=== priority driver started $(date -u +%FT%TZ) pid $$ ===" >> $LOG

echo "--- [1/2] finishing M2H (held-out province repair) ---" >> $LOG
$PY -u scripts/phase5_heldout.py >> $LOG 2>&1
echo "--- M2H done at $(date -u +%FT%TZ) ---" >> $LOG

echo "--- [2/2] M3 seed 1, conditions C1 C2 C3 ---" >> $LOG
$PY -u scripts/phase5_m3.py --seeds 1 --conditions C1,C2,C3 --train-subsample 100000 >> $LOG 2>&1

echo "=== PRIORITY STAGES COMPLETE $(date -u +%FT%TZ) ===" >> $LOG
echo "=== seeds 2 and 3 NOT run; relaunch with --seeds 2,3 when approved ===" >> $LOG

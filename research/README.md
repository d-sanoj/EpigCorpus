# Research

The abbreviation study built on the EpigCorpus corpus. This is a paper's
evidence, not application code.

| Folder | What it holds |
|---|---|
| `scripts/` | The phase scripts (`phase0_…` to `phase5_…`) and probes |
| `reports/` | Write-ups of each phase and its decisions |
| `results/` | Their outputs: analysis tables and model cells |

The datasets the scripts build live under `data/processed/derived/`.

The scripts import the `epigcorpus` package; `scripts/_paths.py` re-exports
its paths. Run them from the repository root with the backend's environment:

```bash
uv run --project backend python research/scripts/phase4_split_stats.py
```

Paths cited in older reports predate two moves. Follow them with
`docs/refactor/path-map.tsv` and `docs/refactor/path-map-backend.tsv`.

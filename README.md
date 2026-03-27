## EDCS Analytics

Incremental scraper for the [Epigraphy Database Clauss/Slaby (EDCS)](https://edcs.hist.uzh.ch/).

The scraper writes output to the `data/` folder and supports:
- first-time full scraping,
- checkpoint-based resume for interrupted runs,
- incremental updates (append only when new rows appear on EDCS).

## Project Structure

```text
EDCS-Analytics/
├── data/
│   ├── edcs_inscriptions.json
│   ├── edcs_inscriptions.tsv
│   └── edcs_checkpoint.json (created during runs)
├── src/
│   └── edcs_scraper.py
├── pyproject.toml
└── README.md
```

## Requirements

- Python 3.13+
- `requests`

## Install

Using `uv`:

```bash
uv sync
```

Or with `pip`:

```bash
pip install requests
```

## Run

From the project root:

```bash
python src/edcs_scraper.py
```

## Output Files

- `data/edcs_inscriptions.json`
- `data/edcs_inscriptions.tsv`
- `data/edcs_checkpoint.json` (temporary resume state)

On completion of a full successful run, checkpoint is removed automatically.

## License

MIT License. See `LICENSE`.

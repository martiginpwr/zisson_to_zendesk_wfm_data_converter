# Zisson Queue Volume Automation

This project fetches 15-minute queue volume data from Zisson and generates per-queue CSV files in forecasting format.

## Output format
Each queue gets one CSV in `data/processed/`:

```csv
datetime,value
2026-03-02T09:00:00Z,14
2026-03-02T09:15:00Z,15
```

Rules enforced by script:
- UTC timestamps only.
- Strict 15-minute increments.
- Missing intervals are filled with zero.
- Chronological order.
- Comma separator and ISO datetime format.

## Required repository secrets
- `ZISSON_API_TOKEN` (required)
- `ZISSON_AUTH_HEADER` (optional, defaults to `Authorization`)
- `ZISSON_AUTH_PREFIX` (optional, defaults to `Bearer`)

## Workflows
- `Backfill Queue CSVs` (`.github/workflows/backfill.yml`): manual, history load.
- `Incremental Queue Sync` (`.github/workflows/sync.yml`): scheduled daily + manual.
- `Test Queue Fetch` (`.github/workflows/test.yml`): manual test run, uploads CSV artifact, no commit.

## Queue selection input
For manual workflows, `queue_selection` accepts comma-separated values using exact queue name or queue id from `config/queues.json`.

Examples:
- `SE Aftersales,SE Delivery`
- `cd9eaf7f-db35-426a-8b3f-1a706c358209,d2e83872-0881-49ba-87f8-afb95938983e`

## Local usage
```bash
pip install -r requirements.txt

# Manual backfill example
python scripts/zisson_sync.py \
  --mode backfill \
  --start-date 2024-03-01T00:00:00Z \
  --end-date 2026-03-01T00:00:00Z \
  --queues "SE Aftersales,SE Delivery"

# Incremental sync (uses state/sync_state.json)
python scripts/zisson_sync.py --mode incremental
```

## Notes
- If API `interval` values are not UTC-based, set `source_timezone` in workflow inputs (or `--source-timezone` locally).
- CSV filenames are based on queue names with safe character normalization.

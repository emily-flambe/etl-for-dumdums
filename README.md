# ETL for Dumdums

Personal ETL pipeline: sync data from APIs to BigQuery, transform with dbt, visualize with Streamlit.

## Quick Start

```bash
uv sync                    # Install dependencies
cp .env.example .env       # Copy and fill in credentials
make run                   # Run full pipeline (sync + dbt)
make app                   # Start Streamlit dashboard
```

## Architecture

```
APIs + Public Datasets
    ↓ Python ETL (scripts/sync_*.py)
BigQuery raw tables (*.raw_*)
    ↓ dbt (dbt/models/)
BigQuery analytics tables (*.stg_*, *.fct_*, *.dim_*)
    ↓
Streamlit (Summary.py, pages/)
```

Each source has its own BigQuery dataset. Raw tables are prefixed `raw_`, dbt models live in the same dataset.

## Commands

```bash
make help                  # Show all available commands
make run                   # Sync all + run dbt
make sync                  # Sync all sources (incremental)
make sync-<source>         # Sync one source (e.g., sync-linear, sync-github)
make sync-<source> FULL=1  # Full historical sync
make dbt                   # Run + test all dbt models
make dbt-<source>          # Run + test models for one source
make app                   # Start Streamlit on localhost:8501
```

All syncs use BigQuery MERGE, so full syncs are safe to run anytime.

## Project Layout

```
scripts/sync_*.py          # ETL entry points
sources/*.py               # Source implementations (extend lib/source.py)
lib/                       # Shared utilities (bigquery.py, source.py)
dbt/models/staging/        # 1:1 views on raw tables
dbt/models/marts/          # Analytics tables (fct_*, dim_*)
pages/*.py                 # Streamlit sub-pages
.github/workflows/         # Scheduled syncs
```

## Adding a New Source

1. Create `sources/<name>.py` with classes extending `Source`
2. Create `scripts/sync_<name>.py` entry point
3. Add Makefile targets (`sync-<name>`, `dbt-<name>`)
4. Add dbt models in `dbt/models/staging/<name>/` and `dbt/models/marts/<name>/`
5. Add workflow in `.github/workflows/sync-<name>.yml`
6. Add Streamlit page in `pages/`

See `CLAUDE.md` for detailed conventions and patterns.

## Environment Variables

Copy `.env.example` to `.env` and fill in:
- `GCP_PROJECT_ID`, `GCP_SA_KEY` - BigQuery access
- Source-specific keys (LINEAR_API_KEY, GITHUB_TOKEN, OURA_API_TOKEN, etc.)

See `.env.example` for the full list.

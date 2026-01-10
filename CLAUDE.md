# CLAUDE.md - Project Instructions

## What This Is

Personal ETL pipeline: sync data from APIs to BigQuery, transform with dbt, visualize with Streamlit.

```
APIs (Linear, GitHub, Oura, etc.) + Public Datasets (Hacker News, FDA, Iowa Liquor)
    ↓ Python ETL (scripts/sync_*.py)
BigQuery raw tables (*.raw_*)
    ↓ dbt (dbt/models/)
BigQuery analytics tables (*.stg_*, *.fct_*, *.dim_*)
    ↓
Streamlit Dashboard (app.py, pages/)
```

## Tech Stack

- **Language**: Python 3.11+
- **Package Manager**: uv
- **ETL**: Custom Python scripts using `lib/source.py` base class
- **Data Warehouse**: BigQuery (one dataset per source)
- **Transformations**: dbt (BigQuery adapter)
- **Dashboard**: Streamlit with Altair charts
- **Notebooks**: Marimo
- **CI/CD**: GitHub Actions, deploys to Cloud Run
- **Linting**: ruff
- **Type Checking**: pyright

## Key Directories

```
scripts/           # ETL entry points (sync_linear.py, sync_github.py, etc.)
sources/           # Source implementations extending lib/source.py
lib/               # Shared utilities (bigquery.py, source.py)
dbt/models/        # dbt models (staging/ views, marts/ tables)
pages/             # Streamlit sub-pages
tests/             # pytest tests for Streamlit pages
.github/workflows/ # CI/CD and scheduled syncs
```

## Essential Commands

```bash
# Install dependencies
uv sync

# Run full pipeline (incremental sync + dbt)
make run

# Individual syncs (add FULL=1 for historical backfill)
make sync-linear        # Last 7 days
make sync-github        # Last 30 days
make sync-oura          # Last 7 days
make sync-hacker-news   # Last 30 days
make sync-<source> FULL=1  # Full historical sync

# dbt commands (append -<source> to filter)
make dbt               # Build + test all models
make dbt-linear        # Build + test Linear models only
make dbt-run           # Run without tests
make dbt-test          # Test only

# Dashboard
make app               # Run locally (all pages)
make app-public        # Run in public mode (hides PII pages)

# Tests
make test              # Run pytest

# Notebooks
make notebook-oura     # Edit Oura investigation notebook

# All commands
make help
```

## Environment Variables

Copy `.env.example` to `.env` and fill in:
- `GCP_PROJECT_ID`, `GCP_SA_KEY` - BigQuery access
- `LINEAR_API_KEY`, `GITHUB_TOKEN`, `OURA_API_TOKEN` - API credentials
- `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_WORKERS_AI_TOKEN` - Sentiment analysis
- See `.env.example` for full list

## Adding a New Source

1. Create `sources/<name>.py` with classes extending `Source`:
   - Define `dataset_id`, `table_id` (prefix with `raw_`), `primary_key`, `schema`
   - Implement `fetch()` and `transform()` methods

2. Create `scripts/sync_<name>.py` entry point

3. Add Makefile targets: `sync-<name>`, `dbt-<name>`, `run-<name>`

4. Add dbt models:
   - `dbt/models/staging/<name>/` - Views renaming columns
   - `dbt/models/marts/<name>/` - Tables with joins and derived fields
   - Tag models with `tags: ["<name>"]`

5. Add GitHub workflow in `.github/workflows/sync-<name>.yml`

6. Add Streamlit page in `pages/` and register in `app.py`

## Key Patterns

### ETL
- **Incremental by default**: Syncs fetch only recent data
- **Full sync**: Use `--full` flag for historical backfill
- **BigQuery MERGE**: Upsert pattern, safe to re-run
- **Raw table prefix**: All ETL tables named `raw_*`

### dbt
- **Staging**: Views that rename columns (e.g., `id` → `issue_id`)
- **Marts**: Tables with joins and derived fields
- **Naming**: `stg_<source>__<entity>`, `fct_*`, `dim_*`

### Streamlit
- **Page registration**: Pages must be registered in `app.py` (not auto-discovered)
- **Data loaders**: Use `@st.cache_data(ttl=300)` in `data.py`
- **Public vs Private**: `PUBLIC_PAGES` for public data, `PRIVATE_PAGES` for work data

## Testing Streamlit Changes

When modifying Streamlit pages:
1. Run `make app`
2. Navigate to modified page
3. **Take screenshots with Playwright** - verify charts show actual data
4. Check for overlapping elements, proper spacing, readable labels
5. Run `make test` for Altair chart validation

Common Altair issues causing empty charts:
- Complex tooltip format strings
- Incompatible scale domains
- Wrong column types (Int64, dbdate)

Fix: Start with simple encoding, add complexity incrementally.

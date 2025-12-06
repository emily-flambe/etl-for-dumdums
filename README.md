# GitHub Actions Workflows

ETL jobs for syncing data from various APIs to BigQuery, with dbt for transformations.

## Quick Start

```bash
# Install dependencies
uv sync

# Copy and fill in .env
cp .env.example .env

# Run full pipeline
make run
```

## Project Structure

```
github-actions-workflows/
├── Makefile                  # make run, make dbt, make dbt-compile, etc.
├── .github/workflows/
│   ├── linear-sync.yml       # ETL: Linear API -> BigQuery
│   └── dbt-run.yml           # Transform: runs after ETL completes
├── lib/                      # Shared Python utilities
│   ├── bigquery.py
│   └── source.py
├── sources/                  # ETL source implementations
│   └── linear.py
├── scripts/                  # ETL entry points
│   └── sync_linear.py
├── dbt/                      # dbt transformation layer
│   ├── models/
│   │   ├── staging/linear/   # 1:1 with raw tables
│   │   └── marts/core/       # Analytics-ready tables
│   ├── dbt_project.yml
│   └── profiles.yml
└── pyproject.toml
```

## Data Flow

```
APIs (Linear, GitHub, ...)
    ↓ ETL scripts (Python)
BigQuery raw tables (linear.raw_issues, linear.raw_users, ...)
    ↓ dbt models
BigQuery analytics tables (linear.fct_issues, linear.dim_users, ...)
```

One dataset per source. All tables (raw + dbt) live in the same dataset.

## Commands

```bash
make help           # Show all commands

# Full pipeline
make run            # Sync all sources + run dbt

# ETL
make sync           # Run all syncs (incremental, last 7 days)
make sync-linear    # Just Linear (incremental)
make sync-linear-full  # Full historical sync (first time or backfill)

# dbt
make dbt            # Build + test
make dbt-run        # Run models only
make dbt-test       # Test only
make dbt-compile    # Compile only
make dbt-docs       # Generate docs
make dbt-docs-serve # Serve docs locally
```

## GitHub Actions

Workflows run automatically:
- `linear-sync.yml`: Daily at 5 AM UTC
- `dbt-run.yml`: Triggers after linear-sync completes

Required secrets:
- `GCP_PROJECT_ID`
- `GCP_SA_KEY` (base64-encoded service account JSON)
- `LINEAR_API_KEY`

## BigQuery Tables

All tables in the `linear` dataset:

| Table | Type | Description |
|-------|------|-------------|
| `raw_issues` | ETL | Raw issues from Linear API |
| `raw_users` | ETL | Raw users |
| `raw_cycles` | ETL | Raw cycles/sprints |
| `stg_linear__*` | dbt view | Staged/cleaned data |
| `fct_issues` | dbt table | Issues with user/cycle details |
| `dim_users` | dbt table | User dimension |

**Example query:**
```sql
SELECT
  identifier,
  title,
  state,
  assignee_name,
  cycle_name,
  days_since_created
FROM linear.fct_issues
WHERE is_in_active_cycle = true
```

## Adding a New Source

1. **ETL**: Create `sources/new_source.py` with classes extending `Source`, and `scripts/sync_new_source.py`
2. **Makefile**: Add `sync-new_source` target, add to `sync:` dependencies
3. **dbt**: Create `dbt/models/staging/new_source/` with source definition and `stg_*` models
4. **Marts**: Update mart models to join new source data
5. **Workflow**: Add `.github/workflows/new_source-sync.yml`

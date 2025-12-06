# Project Context

This document provides context for continuing development on this project.

## What This Project Is

A personal ETL + analytics pipeline that:
1. **Extracts** data from APIs (Linear, eventually GitHub) via Python scripts
2. **Loads** raw data into BigQuery (one dataset per source)
3. **Transforms** raw data into analytics-ready tables using dbt

## Current State

### Working Components

- **ETL for Linear**: `scripts/sync_linear.py` syncs issues, users, and cycles to BigQuery
- **dbt models**: Staging views and mart tables defined but not yet tested against real data
- **GitHub Actions**: Workflows defined for both ETL and dbt

### Not Yet Working

- **Billing propagation**: GCP billing was just enabled; DML queries were failing. May need to re-test.
- **dbt not run yet**: The dbt models haven't been executed against real data
- **GitHub source**: Not implemented yet (planned for cross-platform user joins via email)

## Architecture

```
APIs (Linear, GitHub, ...)
    ↓ Python ETL (scripts/)
BigQuery raw datasets (linear.*, github.*)
    ↓ dbt (dbt/models/)
BigQuery analytics dataset (analytics.*)
```

### Directory Structure

```
github-actions-workflows/
├── .github/workflows/
│   ├── linear-sync.yml      # Daily ETL at midnight EST
│   └── dbt-run.yml          # Runs after ETL completes
├── lib/
│   ├── bigquery.py          # get_client(), merge_table(), load_table()
│   └── source.py            # Base Source class, run_sync()
├── sources/
│   └── linear.py            # LinearIssuesSource, LinearUsersSource, LinearCyclesSource
├── scripts/
│   └── sync_linear.py       # Entry point for Linear ETL
├── dbt/
│   ├── models/
│   │   ├── staging/linear/  # stg_linear__issues, stg_linear__users, stg_linear__cycles
│   │   └── marts/core/      # fct_issues, dim_users
│   ├── dbt_project.yml
│   └── profiles.yml
├── .env.example
├── pyproject.toml           # Uses uv, includes dbt-bigquery
└── README.md
```

## Key Design Decisions

### ETL Layer (Python)

- **Source abstraction**: Each source defines `dataset_id`, `table_id`, `primary_key`, `schema`, `fetch()`, `transform()`
- **Incremental merge**: Uses BigQuery MERGE for upserts (preserves historical data)
- **Temp table cleanup**: try/finally ensures temp tables are deleted even on failure
- **Source-specific datasets**: `linear.*`, `github.*` (not one shared `raw_data` dataset)

### dbt Layer

- **Staging**: Views that rename columns (e.g., `id` → `issue_id`), one folder per source
- **Marts**: Tables joining multiple sources, with derived fields
- **Naming**: `stg_<source>__<entity>`, `fct_<entity>`, `dim_<entity>`

### Authentication

- **ETL scripts**: `GCP_SA_KEY` (base64-encoded) decoded at runtime
- **dbt**: `GCP_SA_KEY_FILE` (path to JSON file)
- **Local**: `.env` file with credentials
- **CI**: GitHub secrets, decoded to temp file

## BigQuery Tables

### Raw (from ETL)

| Table | Primary Key | Notes |
|-------|-------------|-------|
| `linear.users` | id | email is cross-platform join key |
| `linear.cycles` | id | Sprint/cycle dimension |
| `linear.issues` | id | Has `assignee_id` FK, `cycle_id` FK, `labels` array |

### Analytics (from dbt)

| Table | Description |
|-------|-------------|
| `analytics.stg_linear__*` | Views cleaning raw tables |
| `analytics.fct_issues` | Issues enriched with user/cycle details |
| `analytics.dim_users` | User dimension (prepared for GitHub join) |

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `GCP_PROJECT_ID` | ETL, dbt | `emilys-personal-projects` |
| `GCP_SA_KEY` | ETL | Base64-encoded service account JSON |
| `GCP_SA_KEY_FILE` | dbt | Path to credentials.json |
| `LINEAR_API_KEY` | ETL | Linear API key |

## GitHub Secrets Needed

- `GCP_PROJECT_ID`
- `GCP_SA_KEY` (base64-encoded)
- `LINEAR_API_KEY`

## Next Steps

1. **Verify billing works**: Re-run `sync_linear.py` to confirm DML queries succeed
2. **Run dbt**: Execute `dbt build` to create analytics tables
3. **Add GitHub source**: Implement `sources/github.py` for repos/PRs/users
4. **Cross-platform joins**: Link Linear and GitHub users via email in `dim_users`

## Useful Commands

```bash
# Install dependencies
uv sync

# Run Linear ETL locally
uv run python scripts/sync_linear.py

# Run dbt locally
cd dbt && uv run dbt build --profiles-dir .

# Trigger GitHub Actions manually
gh workflow run linear-sync.yml
gh workflow run dbt-run.yml
```

## Repository

https://github.com/emily-flambe/github-actions-workflows

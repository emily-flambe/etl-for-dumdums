# CLAUDE.md - Project Instructions

## What This Project Is

A personal ETL + analytics pipeline that:
1. **Extracts** data from APIs (Linear, eventually GitHub) via Python scripts
2. **Loads** raw data into BigQuery (one dataset per source)
3. **Transforms** raw data into analytics-ready tables using dbt

## Directory Structure

```
etl-for-dumdums/
├── Makefile                 # make run, make dbt, make dbt-compile, etc.
├── .github/workflows/
│   ├── linear-sync.yml      # Daily ETL at 5 AM UTC
│   └── dbt-run.yml          # Runs after ETL completes
├── lib/
│   ├── bigquery.py          # get_client(), merge_table(), load_table()
│   └── source.py            # Base Source class, run_sync()
├── sources/
│   └── linear.py            # LinearIssuesSource, LinearUsersSource, LinearCyclesSource
├── scripts/
│   └── sync_linear.py       # Entry point for Linear ETL
└── dbt/
    ├── models/
    │   ├── staging/linear/  # stg_linear__* views
    │   └── marts/core/      # fct_issues, dim_users tables
    ├── dbt_project.yml
    └── profiles.yml
```

## Architecture

```
APIs (Linear, GitHub, ...)
    ↓ Python ETL (scripts/)
BigQuery source datasets (linear.raw_*, github.raw_*)
    ↓ dbt (dbt/models/)
BigQuery source datasets (linear.stg_*, linear.fct_*, linear.dim_*)
```

One dataset per source. Raw tables prefixed with `raw_`, dbt models in same dataset.

## Key Patterns

### Adding a New Source

1. Create `sources/<source_name>.py` with classes extending `Source`:
   - Define `dataset_id`, `table_id` (use `raw_` prefix), `primary_key`, `schema`
   - Implement `fetch()` to retrieve raw API data
   - Implement `transform()` to convert to BigQuery row format

2. Create `scripts/sync_<source_name>.py` entry point:
   - Instantiate source classes
   - Call `run_sync(source)` for each

3. Add Makefile target:
   - Add `sync-<source>` target
   - Add to `sync:` dependencies

4. Add dbt models:
   - `dbt/models/staging/<source>/`: One view per table, rename columns
   - `dbt/models/marts/core/`: Join sources, add derived fields
   - Naming: `stg_<source>__<entity>`, `fct_<entity>`, `dim_<entity>`

5. Add GitHub workflow in `.github/workflows/`

### ETL Conventions

- **Incremental merge**: Uses BigQuery MERGE for upserts (not TRUNCATE)
- **Source-specific datasets**: `linear.*`, `github.*` (one dataset per source)
- **Raw table prefix**: All ETL tables named `raw_*` (e.g., `raw_issues`)
- **Temp tables**: Written to `raw_data` dataset, cleaned up after merge
- **Primary keys**: Every table must have a `primary_key` for merge operations

### dbt Conventions

- **Staging models**: Views that rename columns (e.g., `id` -> `issue_id`)
- **Mart models**: Tables with joins and derived fields
- **Materialization**: staging = view, marts = table (set in dbt_project.yml)
- **Output dataset**: Same as source (e.g., `linear.*`)

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `GCP_PROJECT_ID` | ETL, dbt | Target GCP project |
| `GCP_SA_KEY` | ETL | Base64-encoded service account JSON |
| `GCP_SA_KEY_FILE` | dbt | Path to credentials.json file |
| `LINEAR_API_KEY` | ETL | Linear API key |
| `GITHUB_TOKEN` | ETL | GitHub PAT with repo and read:org scopes |

## Common Commands

```bash
# Install dependencies
uv sync

# Run full pipeline (sync + dbt)
make run

# Pipeline steps
make sync           # Run all data syncs
make sync-linear    # Just Linear

# dbt commands
make dbt            # Build models + run tests
make dbt-run        # Run models only
make dbt-test       # Run tests only
make dbt-compile    # Compile (no execution)
make dbt-docs       # Generate docs
make dbt-docs-serve # Serve docs locally
make dbt-debug      # Test connection
make dbt-clean      # Clean artifacts

# See all commands
make help
```

## BigQuery Tables

### Linear Dataset

All Linear tables live in the `linear` dataset:

| Table | Type | Description |
|-------|------|-------------|
| `linear.raw_issues` | ETL | Raw issues from Linear API |
| `linear.raw_users` | ETL | Raw users from Linear API |
| `linear.raw_cycles` | ETL | Raw cycles from Linear API |
| `linear.stg_linear__issues` | dbt view | Staged issues |
| `linear.stg_linear__users` | dbt view | Staged users |
| `linear.stg_linear__cycles` | dbt view | Staged cycles |
| `linear.fct_issues` | dbt table | Issues with user/cycle details |
| `linear.dim_users` | dbt table | User dimension (Linear + GitHub joined) |

### GitHub Dataset

| Table | Type | Description |
|-------|------|-------------|
| `github.raw_users` | ETL | Org members from GitHub API |
| `github.raw_pull_requests` | ETL | PRs from configured repos |
| `github.raw_pr_reviews` | ETL | PR reviews |
| `github.raw_pr_comments` | ETL | PR review comments |
| `github.stg_github__*` | dbt view | Staged GitHub tables |
| `github.fct_pull_requests` | dbt table | PRs with author/review stats |

Temp tables used during ETL merges go to `raw_data` dataset and are cleaned up automatically.

## Current State

- **Working**: Full pipeline (ETL + dbt) runs via `make run`
- **Working**: GitHub source syncs PRs, reviews, comments for demexchange org
- **Repos synced**: demexchange/ddx-data-pipeline, demexchange/snowflake-queries

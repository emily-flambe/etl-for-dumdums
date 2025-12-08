# CLAUDE.md - Project Instructions

## What This Project Is

A personal ETL + analytics pipeline that:
1. **Extracts** data from APIs (Linear, GitHub, Oura) via Python scripts
2. **Loads** raw data into BigQuery (one dataset per source)
3. **Transforms** raw data into analytics-ready tables using dbt

## Directory Structure

```
etl-for-dumdums/
├── Makefile                 # make run, make dbt, make dbt-compile, etc.
├── .github/workflows/
│   ├── sync-linear.yml      # Daily Linear ETL at 5 AM UTC
│   ├── sync-github.yml      # Daily GitHub ETL at 5 AM UTC
│   └── sync-oura.yml        # Daily Oura ETL at 5 AM UTC
├── lib/
│   ├── bigquery.py          # get_client(), merge_table(), load_table()
│   └── source.py            # Base Source class, run_sync()
├── sources/
│   ├── linear.py            # LinearIssuesSource, LinearUsersSource, LinearCyclesSource
│   ├── github.py            # GitHubUsersSource, GitHubPullRequestsSource, etc.
│   └── oura.py              # OuraSleepSource, OuraReadinessSource, OuraActivitySource
├── scripts/
│   ├── sync_linear.py       # Entry point for Linear ETL
│   ├── sync_github.py       # Entry point for GitHub ETL
│   └── sync_oura.py         # Entry point for Oura ETL
└── dbt/
    ├── models/
    │   ├── staging/linear/  # stg_linear__* views
    │   ├── staging/github/  # stg_github__* views
    │   ├── staging/oura/    # stg_oura__* views
    │   └── marts/core/      # fct_issues, fct_pull_requests, fct_oura_daily, dim_users
    ├── dbt_project.yml
    └── profiles.yml
```

## Architecture

```
APIs (Linear, GitHub, Oura)
    ↓ Python ETL (scripts/)
BigQuery source datasets (linear.raw_*, github.raw_*, oura.raw_*)
    ↓ dbt (dbt/models/)
BigQuery analytics tables (linear.stg_*, linear.fct_*, linear.dim_*)
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

- **Incremental by default**: All syncs fetch only recently updated records
- **Full sync option**: Use `--full` flag to fetch all historical records
- **BigQuery MERGE**: Uses upsert pattern (safe to run full sync anytime, no duplicates)
- **Source-specific datasets**: `linear.*`, `github.*` (one dataset per source)
- **Raw table prefix**: All ETL tables named `raw_*` (e.g., `raw_issues`)
- **Temp tables**: Written to `raw_data` dataset, cleaned up after merge
- **Primary keys**: Every table must have a `primary_key` for merge operations

### Sync Modes

| Source | Incremental (default) | Full (`--full`) |
|--------|----------------------|-----------------|
| Linear | Last 7 days | All issues |
| GitHub | Last 30 days | All PRs |
| Oura | Last 7 days | Last 365 days |

**When to use full sync:**
- First time setup (empty tables)
- Backfilling historical data
- Data recovery after issues

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
| `OURA_API_TOKEN` | ETL | Oura personal access token |

## Common Commands

```bash
# Install dependencies
uv sync

# Run full pipeline (incremental sync + dbt)
make run

# Syncs (add FULL=1 for full sync)
make sync               # All sources (incremental)
make sync-linear        # Linear only (7 day lookback)
make sync-github        # GitHub only (30 day lookback)
make sync-oura          # Oura only (7 day lookback)
make sync-linear FULL=1 # Linear full sync
make sync-oura FULL=1   # Oura full sync (365 days)

# dbt (append -linear, -github, or -oura to filter)
make dbt                # Build + test all models
make dbt-linear         # Build + test Linear models
make dbt-run            # Run all models (no tests)
make dbt-run-linear     # Run Linear models
make dbt-test           # Test all models
make dbt-test-github    # Test GitHub models
make dbt-compile        # Compile (no execution)
make dbt-docs           # Generate docs
make dbt-docs-serve     # Serve docs locally
make dbt-debug          # Test connection
make dbt-clean          # Clean artifacts

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

### Oura Dataset

| Table | Type | Description |
|-------|------|-------------|
| `oura.raw_sleep` | ETL | Daily sleep scores and contributors |
| `oura.raw_daily_readiness` | ETL | Daily readiness scores |
| `oura.raw_daily_activity` | ETL | Daily activity metrics (steps, calories) |
| `linear.stg_oura__sleep` | dbt view | Staged sleep data |
| `linear.stg_oura__daily_readiness` | dbt view | Staged readiness data |
| `linear.stg_oura__daily_activity` | dbt view | Staged activity data |
| `linear.fct_oura_daily` | dbt table | Joined daily wellness metrics |

Temp tables used during ETL merges go to `raw_data` dataset and are cleaned up automatically.

## Streamlit App

The Streamlit app lives in `app.py` (home page) and `pages/` (sub-pages).

### Testing Streamlit Changes

**IMPORTANT**: When modifying Streamlit pages, you MUST run the actual app to verify changes work:

```bash
make app  # Starts Streamlit on localhost:8501
```

Then navigate to the modified page in the browser. Do NOT rely only on Python syntax checks or isolated data tests - many errors (especially Altair chart errors) only appear at runtime when the page renders.

**Common issues that only appear at runtime**:
- Altair chart encoding errors (e.g., nested `alt.condition()` not supported in v6)
- BigQuery `dbdate` type not compatible with pandas operations
- Nullable `Int64` columns causing issues with Altair conditions
- Streamlit widget state errors

### Adding a New Streamlit Page

1. Create `pages/N_PageName.py` (N = order number)
2. Add data loader function to `app_data.py` with `@st.cache_data(ttl=300)`
3. Add tests to `tests/test_streamlit_pages.py` that exercise the chart code
4. Run `make test` to verify charts render without Altair errors
5. Run `make app` and navigate to the new page to verify it loads without errors
6. Test all interactive elements (filters, charts, tables)

## Current State

- **Working**: Full pipeline (ETL + dbt) runs via `make run`
- **Working**: GitHub source syncs PRs, reviews, comments for demexchange org
- **Working**: Oura source syncs sleep, readiness, and activity data
- **Repos synced**: demexchange/ddx-data-pipeline, demexchange/snowflake-queries

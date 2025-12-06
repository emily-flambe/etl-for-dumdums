# GitHub Actions Workflows

ETL jobs for syncing data from various APIs to BigQuery, with dbt for transformations.

## Project Structure

```
github-actions-workflows/
├── .github/workflows/
│   ├── linear-sync.yml       # ETL: Linear API -> BigQuery
│   └── dbt-run.yml           # Transform: raw -> analytics
├── lib/                      # Shared Python utilities
│   ├── bigquery.py
│   └── source.py
├── sources/                  # ETL source implementations
│   └── linear.py
├── scripts/                  # ETL entry points
│   └── sync_linear.py
├── dbt/                      # dbt transformation layer
│   ├── models/
│   │   ├── staging/          # 1:1 with raw tables
│   │   │   └── linear/
│   │   └── marts/            # Analytics-ready tables
│   │       └── core/
│   ├── dbt_project.yml
│   └── profiles.yml
└── pyproject.toml
```

## Data Flow

```
APIs (Linear, GitHub, ...)
    ↓ ETL scripts (Python)
BigQuery raw tables (linear.issues, linear.users, ...)
    ↓ dbt models
BigQuery analytics tables (analytics.fct_issues, analytics.dim_users, ...)
```

## Setup

```bash
# Install dependencies
uv sync

# Copy and fill in .env
cp .env.example .env
```

## Running Locally

### ETL (sync raw data)

```bash
uv run python scripts/sync_linear.py
```

### dbt (transform to analytics)

```bash
cd dbt
uv run dbt build --profiles-dir .
```

## GitHub Actions

Workflows run automatically:
- `linear-sync.yml`: Daily at midnight EST
- `dbt-run.yml`: Triggers after linear-sync completes

Required secrets:
- `GCP_PROJECT_ID`
- `GCP_SA_KEY` (base64-encoded)
- `LINEAR_API_KEY`

---

## ETL Sources

### Linear

Syncs to `linear.*` dataset:

| Table | Description |
|-------|-------------|
| `linear.users` | User dimension (id, email, display_name) |
| `linear.cycles` | Cycle/sprint dimension |
| `linear.issues` | Issues with `assignee_id`, `cycle_id` FKs |

---

## dbt Models

### Staging (`models/staging/`)

Views that clean and rename raw source columns. One folder per source.

```
staging/
└── linear/
    ├── stg_linear__issues.sql
    ├── stg_linear__users.sql
    └── stg_linear__cycles.sql
```

### Marts (`models/marts/`)

Analytics-ready tables joining multiple sources.

| Model | Description |
|-------|-------------|
| `fct_issues` | Issues enriched with user and cycle details |
| `dim_users` | User dimension (will include GitHub when added) |

**Query the marts:**
```sql
SELECT
  identifier,
  title,
  state,
  assignee_name,
  assignee_email,
  cycle_name,
  days_since_created
FROM analytics.fct_issues
WHERE is_in_active_cycle = true
```

---

## Adding a New Source

1. **ETL**: Create `sources/new_source.py` and `scripts/sync_new_source.py`
2. **Staging**: Create `dbt/models/staging/new_source/` with source definition and `stg_*` models
3. **Marts**: Update mart models to join new source data

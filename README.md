# GitHub Actions Workflows

ETL jobs for syncing data from various APIs to BigQuery. Can be run ad-hoc locally or on a schedule via GitHub Actions.

## Project Structure

```
github-actions-workflows/
├── .github/workflows/    # GitHub Actions workflow definitions
├── lib/
│   ├── bigquery.py       # BigQuery client, load, merge utilities
│   └── source.py         # Base Source class and run_sync()
├── sources/              # Source implementations (one per API)
│   └── linear.py
├── scripts/              # Thin wrappers that run syncs
│   └── sync_linear.py
└── pyproject.toml
```

## Setup

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure credentials

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Your Google Cloud project |
| `GCP_SA_KEY` | Base64-encoded service account JSON |
| `LINEAR_API_KEY` | Linear API key (for Linear sync) |

## Running Scripts

### Ad-hoc (local)

```bash
# First time: copy and fill in .env
cp .env.example .env

# Then just run
uv run python scripts/sync_linear.py
```

To get your `GCP_SA_KEY` value:
```bash
base64 -i /path/to/credentials.json | tr -d '\n'
```

### Scheduled (GitHub Actions)

Add secrets in GitHub repo settings (Settings > Secrets and variables > Actions), then workflows run on their defined schedules. Trigger manually via Actions tab > Select workflow > Run workflow.

## Available Sources

### Linear (`sync_linear.py`)

Syncs issues, cycles, and users from Linear to BigQuery.

- **Schedule**: Daily at midnight EST (5 AM UTC)
- **Mode**: Incremental merge on `id`

**Tables:**

| Table | Description |
|-------|-------------|
| `linear.users` | User dimension table (id, email, display_name, name, active) |
| `linear.cycles` | Cycle/sprint dimension table (all cycles) |
| `linear.issues` | Issue fact table with `assignee_id` and `cycle_id` FKs |

**Join for analysis:**
```sql
SELECT
  i.identifier,
  i.title,
  i.state,
  u.display_name AS assignee,
  u.email AS assignee_email,
  c.name AS cycle_name,
  c.starts_at,
  c.ends_at
FROM linear.issues i
LEFT JOIN linear.users u ON i.assignee_id = u.id
LEFT JOIN linear.cycles c ON i.cycle_id = c.id
```

**Cross-platform join (Linear + GitHub) via email:**
```sql
SELECT
  lu.email,
  lu.display_name AS linear_name,
  -- gu.login AS github_username  -- when github.users exists
FROM linear.users lu
-- JOIN github.users gu ON lu.email = gu.email
```

## Adding a New Source

1. Create `sources/your_source.py`:

```python
from lib.source import Source

class YourSource(Source):
    dataset_id = "your_source"  # Creates dataset if not exists
    table_id = "your_table"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        # ... other fields
    ]

    def __init__(self, ...):
        # Setup, get API keys from env vars

    def fetch(self) -> list[dict]:
        # Call API, handle pagination, return raw data

    def transform(self, raw_data) -> list[dict]:
        # Flatten/convert to match schema
```

2. Create `scripts/sync_your_source.py`:

```python
from lib.source import run_sync
from sources.your_source import YourSource

if __name__ == "__main__":
    source = YourSource()
    run_sync(source)
```

3. Create `.github/workflows/your-source-sync.yml` (copy from `linear-sync.yml`)

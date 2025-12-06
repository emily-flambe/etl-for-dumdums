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
└── requirements.txt
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
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
export LINEAR_API_KEY="lin_api_xxxxx"
export GCP_PROJECT_ID="your-project-id"
export GCP_SA_KEY="$(base64 -i /path/to/credentials.json | tr -d '\n')"

python scripts/sync_linear.py
```

### Scheduled (GitHub Actions)

Add secrets in GitHub repo settings (Settings > Secrets and variables > Actions), then workflows run on their defined schedules. Trigger manually via Actions tab > Select workflow > Run workflow.

## Available Sources

### Linear (`sync_linear.py`)

Syncs issues from Linear to BigQuery.

- **Schedule**: Daily at 6 AM UTC
- **Table**: `raw_data.linear_issues`
- **Lookback**: 7 days
- **Mode**: Incremental merge on `id`

## Adding a New Source

1. Create `sources/your_source.py`:

```python
from lib.source import Source

class YourSource(Source):
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

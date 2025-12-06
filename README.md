# GitHub Actions Workflows

ETL jobs for syncing data from various APIs to BigQuery. Can be run ad-hoc locally or on a schedule via GitHub Actions.

## Project Structure

```
github-actions-workflows/
├── .github/workflows/    # GitHub Actions workflow definitions
├── lib/                  # Shared utilities (BigQuery client, etc.)
├── scripts/              # Individual sync scripts
└── requirements.txt
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

You need three things:
- **Linear API key** - from Linear Settings > API > Personal API keys
- **GCP Project ID** - your Google Cloud project
- **GCP Service Account** - with BigQuery Data Editor + User roles

## Running Scripts

### Ad-hoc (local)

```bash
export LINEAR_API_KEY="lin_api_xxxxx"
export GCP_PROJECT_ID="your-project-id"
export GCP_SA_KEY="$(base64 -i /path/to/credentials.json | tr -d '\n')"

python scripts/sync_linear.py
```

### Scheduled (GitHub Actions)

Add these secrets in GitHub repo settings (Settings > Secrets and variables > Actions):

| Secret | Value |
|--------|-------|
| `LINEAR_API_KEY` | Your Linear API key |
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SA_KEY` | Base64-encoded service account JSON |

To base64 encode your credentials:
```bash
base64 -i credentials.json | tr -d '\n'
```

Workflows run on their defined schedules. To trigger manually: Actions tab > Select workflow > Run workflow.

## Available Scripts

### `sync_linear.py`

Syncs Linear issues to BigQuery.

- **Schedule**: Daily at 6 AM UTC
- **Table**: `raw_data.linear_issues`
- **Lookback**: Issues updated in last 7 days
- **Mode**: Incremental merge (upsert by `id`)

Uses MERGE to insert new issues and update existing ones. This preserves historical data that Linear's API may no longer return (e.g., old sprint details).

**Columns**: id, identifier, title, state, assignee, priority, created_at, updated_at, project_name

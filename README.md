# GitHub Actions Workflows

Scheduled ETL jobs that pull data from various APIs and write to BigQuery.

## Workflows

### Linear Sync (`linear-sync.yml`)

Syncs Linear issues to BigQuery daily at 6 AM UTC.

- Pulls all issues updated in the last 7 days
- Writes to `raw_data.linear_issues` table in BigQuery
- Uses WRITE_TRUNCATE mode (replaces all data each run)

## Required Secrets

Configure the following secrets in your GitHub repository settings (Settings > Secrets and variables > Actions):

### `LINEAR_API_KEY`

Your Linear API key for accessing the GraphQL API.

**How to get it:**
1. Go to Linear Settings (click your profile picture > Settings)
2. Navigate to "API" in the left sidebar
3. Click "Create key" under "Personal API keys"
4. Copy the generated key

### `GCP_PROJECT_ID`

Your Google Cloud project ID where BigQuery tables will be created.

**How to get it:**
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Your project ID is shown in the project selector dropdown
3. Or find it in Project Settings

### `GCP_SA_KEY`

Base64-encoded Google Cloud service account JSON key with BigQuery write permissions.

**How to get it:**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to IAM & Admin > Service Accounts
3. Create a new service account or use an existing one
4. Grant the following roles:
   - `BigQuery Data Editor` (to write data)
   - `BigQuery Job User` (to run load jobs)
5. Create a JSON key for the service account
6. Base64 encode the JSON file:
   ```bash
   base64 -i your-service-account-key.json | tr -d '\n'
   ```
7. Copy the output and use it as the secret value

## Manual Triggering

You can manually trigger the workflow:

1. Go to the Actions tab in your GitHub repository
2. Select "Sync Linear Issues to BigQuery"
3. Click "Run workflow"

## BigQuery Schema

The `linear_issues` table contains:

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Linear issue UUID |
| identifier | STRING | Human-readable ID (e.g., ENG-123) |
| title | STRING | Issue title |
| state | STRING | Current state (e.g., In Progress, Done) |
| assignee | STRING | Assigned user's name |
| priority | INTEGER | Priority level (0-4) |
| created_at | TIMESTAMP | When the issue was created |
| updated_at | TIMESTAMP | When the issue was last updated |
| project_name | STRING | Associated project name |

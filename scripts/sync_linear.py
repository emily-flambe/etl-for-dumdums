#!/usr/bin/env python3
"""
Sync Linear issues to BigQuery.

Pulls issues updated in the last 7 days from Linear's GraphQL API
and writes them to BigQuery.
"""

import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"
DATASET_ID = "raw_data"
TABLE_ID = "linear_issues"


def get_linear_client():
    """Get Linear API key from environment."""
    api_key = os.environ.get("LINEAR_API_KEY")
    if not api_key:
        raise ValueError("LINEAR_API_KEY environment variable is not set")
    return api_key


def get_bigquery_client():
    """Create BigQuery client from base64-encoded service account key."""
    gcp_sa_key_b64 = os.environ.get("GCP_SA_KEY")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not gcp_sa_key_b64:
        raise ValueError("GCP_SA_KEY environment variable is not set")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is not set")

    # Decode base64 service account key
    sa_key_json = base64.b64decode(gcp_sa_key_b64).decode("utf-8")
    sa_info = json.loads(sa_key_json)

    client = bigquery.Client.from_service_account_info(
        sa_info,
        project=project_id
    )
    return client


def fetch_linear_issues(api_key, since_date):
    """
    Fetch all issues updated since the given date from Linear.

    Uses cursor-based pagination to get all results.
    """
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    query = """
    query GetIssues($after: String, $filter: IssueFilter) {
        issues(first: 100, after: $after, filter: $filter) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                id
                identifier
                title
                state {
                    name
                }
                assignee {
                    name
                }
                priority
                createdAt
                updatedAt
                project {
                    name
                }
            }
        }
    }
    """

    all_issues = []
    cursor = None
    page = 1

    while True:
        logger.info(f"Fetching page {page} of Linear issues...")

        variables = {
            "after": cursor,
            "filter": {
                "updatedAt": {
                    "gte": since_date.isoformat()
                }
            }
        }

        response = requests.post(
            LINEAR_API_URL,
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        if "errors" in data:
            raise Exception(f"Linear API error: {data['errors']}")

        issues_data = data["data"]["issues"]
        nodes = issues_data["nodes"]
        all_issues.extend(nodes)

        logger.info(f"Fetched {len(nodes)} issues on page {page}")

        if not issues_data["pageInfo"]["hasNextPage"]:
            break

        cursor = issues_data["pageInfo"]["endCursor"]
        page += 1

    logger.info(f"Total issues fetched: {len(all_issues)}")
    return all_issues


def transform_issues(issues):
    """Transform Linear issues to BigQuery row format."""
    rows = []
    for issue in issues:
        row = {
            "id": issue["id"],
            "identifier": issue["identifier"],
            "title": issue["title"],
            "state": issue["state"]["name"] if issue["state"] else None,
            "assignee": issue["assignee"]["name"] if issue["assignee"] else None,
            "priority": issue["priority"],
            "created_at": issue["createdAt"],
            "updated_at": issue["updatedAt"],
            "project_name": issue["project"]["name"] if issue["project"] else None,
        }
        rows.append(row)
    return rows


def ensure_dataset_exists(client, dataset_id):
    """Create dataset if it doesn't exist."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id}")


def write_to_bigquery(client, rows):
    """Write rows to BigQuery table, creating if necessary."""
    project_id = os.environ.get("GCP_PROJECT_ID")
    table_ref = f"{project_id}.{DATASET_ID}.{TABLE_ID}"

    # Define schema
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("identifier", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("assignee", "STRING"),
        bigquery.SchemaField("priority", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("project_name", "STRING"),
    ]

    # Ensure dataset exists
    ensure_dataset_exists(client, DATASET_ID)

    # Configure job to truncate and replace
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(f"Writing {len(rows)} rows to {table_ref}...")

    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config
    )
    job.result()  # Wait for job to complete

    logger.info(f"Successfully wrote {len(rows)} rows to {table_ref}")


def main():
    """Main entry point."""
    logger.info("Starting Linear to BigQuery sync...")

    # Calculate date range (last 7 days)
    since_date = datetime.now(timezone.utc) - timedelta(days=7)
    logger.info(f"Fetching issues updated since {since_date.isoformat()}")

    # Initialize clients
    linear_api_key = get_linear_client()
    bq_client = get_bigquery_client()

    # Fetch and transform issues
    issues = fetch_linear_issues(linear_api_key, since_date)

    if not issues:
        logger.info("No issues found in the specified date range")
        return

    rows = transform_issues(issues)

    # Write to BigQuery
    write_to_bigquery(bq_client, rows)

    logger.info("Sync completed successfully")


if __name__ == "__main__":
    main()

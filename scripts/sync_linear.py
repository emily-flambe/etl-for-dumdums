#!/usr/bin/env python3
"""
Sync Linear issues to BigQuery.

Pulls issues updated in the last 7 days from Linear's GraphQL API
and writes them to BigQuery.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery

# Add parent directory to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import bigquery as bq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"
TABLE_ID = "linear_issues"

TABLE_SCHEMA = [
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


def get_linear_api_key() -> str:
    """Get Linear API key from environment."""
    api_key = os.environ.get("LINEAR_API_KEY")
    if not api_key:
        raise ValueError("LINEAR_API_KEY environment variable is not set")
    return api_key


def fetch_issues(api_key: str, since_date: datetime) -> list[dict]:
    """
    Fetch all issues updated since the given date from Linear.

    Uses cursor-based pagination to retrieve all matching results.

    Args:
        api_key: Linear API key
        since_date: Only fetch issues updated after this date

    Returns:
        List of issue dictionaries from Linear API
    """
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
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
                state { name }
                assignee { name }
                priority
                createdAt
                updatedAt
                project { name }
            }
        }
    }
    """

    all_issues = []
    cursor = None
    page = 1

    while True:
        logger.info(f"Fetching page {page} from Linear API...")

        variables = {
            "after": cursor,
            "filter": {"updatedAt": {"gte": since_date.isoformat()}},
        }

        response = requests.post(
            LINEAR_API_URL,
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise Exception(f"Linear API error: {data['errors']}")

        issues_data = data["data"]["issues"]
        nodes = issues_data["nodes"]
        all_issues.extend(nodes)

        logger.info(f"Retrieved {len(nodes)} issues on page {page}")

        if not issues_data["pageInfo"]["hasNextPage"]:
            break

        cursor = issues_data["pageInfo"]["endCursor"]
        page += 1

    logger.info(f"Total issues fetched: {len(all_issues)}")
    return all_issues


def transform_issues(issues: list[dict]) -> list[dict]:
    """
    Transform Linear API response to BigQuery row format.

    Args:
        issues: Raw issue data from Linear API

    Returns:
        List of flattened dictionaries matching TABLE_SCHEMA
    """
    return [
        {
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
        for issue in issues
    ]


def main():
    """Main entry point."""
    logger.info("Starting Linear to BigQuery sync...")

    # Calculate date range (last 7 days)
    since_date = datetime.now(timezone.utc) - timedelta(days=7)
    logger.info(f"Fetching issues updated since {since_date.isoformat()}")

    # Fetch from Linear
    api_key = get_linear_api_key()
    issues = fetch_issues(api_key, since_date)

    if not issues:
        logger.info("No issues found in the specified date range")
        return

    # Transform and merge to BigQuery (incremental upsert)
    rows = transform_issues(issues)
    client = bq.get_client()
    bq.merge_table(client, TABLE_ID, rows, TABLE_SCHEMA, primary_key="id")

    logger.info("Sync completed successfully")


if __name__ == "__main__":
    main()

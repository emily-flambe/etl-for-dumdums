"""
Linear data source.

Fetches issues from Linear's GraphQL API.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from google.cloud import bigquery

from lib.source import Source

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

ISSUES_QUERY = """
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
            labels { nodes { name } }
        }
    }
}
"""


class LinearIssuesSource(Source):
    """Fetches issues from Linear."""

    dataset_id = "linear"
    table_id = "issues"
    primary_key = "id"
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
        bigquery.SchemaField("labels", "STRING", mode="REPEATED"),
    ]

    def __init__(self, lookback_days: int = 7):
        """
        Args:
            lookback_days: Fetch issues updated within this many days
        """
        self.lookback_days = lookback_days
        self.api_key = os.environ.get("LINEAR_API_KEY")
        if not self.api_key:
            raise ValueError("LINEAR_API_KEY environment variable is not set")

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch issues updated within lookback period."""
        since_date = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        logger.info(f"Fetching issues updated since {since_date.isoformat()}")

        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

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
                json={"query": ISSUES_QUERY, "variables": variables},
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

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Flatten nested Linear API response to BigQuery rows."""
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
                "labels": [label["name"] for label in issue["labels"]["nodes"]],
            }
            for issue in raw_data
        ]

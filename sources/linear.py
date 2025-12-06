"""
Linear data source.

Fetches issues and cycles from Linear's GraphQL API.
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
            cycle { id }
        }
    }
}
"""

CYCLES_QUERY = """
query GetCycles($after: String) {
    cycles(first: 100, after: $after) {
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
            id
            number
            name
            startsAt
            endsAt
            team { name }
        }
    }
}
"""


def get_api_key() -> str:
    """Get Linear API key from environment."""
    api_key = os.environ.get("LINEAR_API_KEY")
    if not api_key:
        raise ValueError("LINEAR_API_KEY environment variable is not set")
    return api_key


def fetch_paginated(query: str, root_field: str, variables: dict = None) -> list[dict]:
    """Fetch all pages from a Linear GraphQL query."""
    api_key = get_api_key()
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    all_nodes = []
    cursor = None
    page = 1
    variables = variables or {}

    while True:
        logger.info(f"Fetching page {page} from Linear API ({root_field})...")

        request_vars = {**variables, "after": cursor}

        response = requests.post(
            LINEAR_API_URL,
            headers=headers,
            json={"query": query, "variables": request_vars},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise Exception(f"Linear API error: {data['errors']}")

        result = data["data"][root_field]
        nodes = result["nodes"]
        all_nodes.extend(nodes)

        logger.info(f"Retrieved {len(nodes)} {root_field} on page {page}")

        if not result["pageInfo"]["hasNextPage"]:
            break

        cursor = result["pageInfo"]["endCursor"]
        page += 1

    logger.info(f"Total {root_field} fetched: {len(all_nodes)}")
    return all_nodes


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
        bigquery.SchemaField("cycle_id", "STRING"),
    ]

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        since_date = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        logger.info(f"Fetching issues updated since {since_date.isoformat()}")
        return fetch_paginated(
            ISSUES_QUERY,
            "issues",
            {"filter": {"updatedAt": {"gte": since_date.isoformat()}}},
        )

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                "cycle_id": issue["cycle"]["id"] if issue["cycle"] else None,
            }
            for issue in raw_data
        ]


class LinearCyclesSource(Source):
    """Fetches cycles from Linear."""

    dataset_id = "linear"
    table_id = "cycles"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("number", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("starts_at", "TIMESTAMP"),
        bigquery.SchemaField("ends_at", "TIMESTAMP"),
        bigquery.SchemaField("team_name", "STRING"),
    ]

    def fetch(self) -> list[dict[str, Any]]:
        return fetch_paginated(CYCLES_QUERY, "cycles")

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": cycle["id"],
                "number": cycle["number"],
                "name": cycle["name"],
                "starts_at": cycle["startsAt"],
                "ends_at": cycle["endsAt"],
                "team_name": cycle["team"]["name"] if cycle["team"] else None,
            }
            for cycle in raw_data
        ]

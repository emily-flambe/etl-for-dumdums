"""
GitHub data source.

Fetches pull requests, reviews, comments, and users from GitHub REST API
for the demexchange organization.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from google.cloud import bigquery

from lib.source import Source

# Rate limiting delay between individual PR detail fetches
API_DELAY_SECONDS = 0.05  # 50ms between requests

logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com"

# Repositories to sync
REPOS = [
    "demexchange/ddx-data-pipeline",
    "demexchange/snowflake-queries",
]

ORG_NAME = "demexchange"


def get_token() -> str:
    """Get GitHub token from environment."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN environment variable is not set")
    return token


def get_headers() -> dict[str, str]:
    """Get headers for GitHub API requests."""
    return {
        "Authorization": f"Bearer {get_token()}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_paginated(url: str, params: dict = None) -> list[dict]:
    """Fetch all pages from a GitHub REST API endpoint."""
    headers = get_headers()
    all_items = []
    page = 1
    params = params or {}

    while True:
        logger.info(f"Fetching page {page} from {url}...")

        request_params = {**params, "page": page, "per_page": 100}
        response = requests.get(
            url,
            headers=headers,
            params=request_params,
            timeout=30,
        )
        response.raise_for_status()

        items = response.json()

        # Handle both array responses and object responses with items key
        if isinstance(items, dict):
            items = items.get("items", [])

        if not items:
            break

        all_items.extend(items)
        logger.info(f"Retrieved {len(items)} items on page {page}")

        # Check for next page via Link header
        if "next" not in response.links:
            break

        page += 1

    logger.info(f"Total items fetched from {url}: {len(all_items)}")
    return all_items


class GitHubUsersSource(Source):
    """Extracts unique users from PR, review, and comment data."""

    dataset_id = "github"
    table_id = "raw_users"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("login", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("avatar_url", "STRING"),
    ]

    def __init__(
        self,
        prs: list[dict[str, Any]] = None,
        reviews: list[dict[str, Any]] = None,
        comments: list[dict[str, Any]] = None,
    ):
        """
        Extract users from provided PR/review/comment data.

        Args:
            prs: List of raw PR data (from GitHubPullRequestsSource.fetch())
            reviews: List of raw review data (from GitHubPRReviewsSource.fetch())
            comments: List of raw comment data (from GitHubPRCommentsSource.fetch())
        """
        self.prs = prs or []
        self.reviews = reviews or []
        self.comments = comments or []

    def fetch(self) -> list[dict[str, Any]]:
        """Extract unique users from PR, review, and comment data."""
        users_by_id = {}

        # Extract from PRs
        for pr in self.prs:
            user = pr.get("user")
            if user and user.get("id"):
                users_by_id[user["id"]] = user

        # Extract from reviews
        for review in self.reviews:
            user = review.get("user")
            if user and user.get("id"):
                users_by_id[user["id"]] = user

        # Extract from comments
        for comment in self.comments:
            user = comment.get("user")
            if user and user.get("id"):
                users_by_id[user["id"]] = user

        logger.info(f"Extracted {len(users_by_id)} unique users from activity data")
        return list(users_by_id.values())

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": str(user["id"]),
                "login": user.get("login"),
                "email": user.get("email"),
                "name": user.get("name"),
                "avatar_url": user.get("avatar_url"),
            }
            for user in raw_data
        ]


class GitHubPullRequestsSource(Source):
    """Fetches pull requests from configured repositories."""

    dataset_id = "github"
    table_id = "raw_pull_requests"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("number", "INTEGER"),
        bigquery.SchemaField("repo", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("merged", "BOOLEAN"),
        bigquery.SchemaField("author_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("merged_at", "TIMESTAMP"),
        bigquery.SchemaField("closed_at", "TIMESTAMP"),
        bigquery.SchemaField("additions", "INTEGER"),
        bigquery.SchemaField("deletions", "INTEGER"),
        bigquery.SchemaField("changed_files", "INTEGER"),
    ]

    def __init__(self, lookback_days: int = 30, full_sync: bool = False):
        self.lookback_days = lookback_days
        self.full_sync = full_sync

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch PRs from all configured repos."""
        if self.full_sync:
            # Full sync goes back to Aug 1, 2025
            since_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
            logger.info(f"Fetching PRs since {since_date.date()} (full sync)")
        else:
            since_date = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
            logger.info(f"Fetching PRs updated since {since_date.isoformat()}")

        all_prs = []
        headers = get_headers()

        for repo in REPOS:
            repo_name = repo.split("/")[1]
            url = f"{GITHUB_API_URL}/repos/{repo}/pulls"
            params = {
                "state": "all",
                "sort": "updated",
                "direction": "desc",
                "per_page": 100,
            }

            # Paginate manually with early termination
            # Since results are sorted by updated desc, we can stop once we hit old PRs
            page = 1
            repo_pr_count = 0
            reached_cutoff = False

            while not reached_cutoff:
                logger.info(f"Fetching page {page} of PRs from {repo_name}...")
                response = requests.get(
                    url,
                    headers=headers,
                    params={**params, "page": page},
                    timeout=30,
                )
                response.raise_for_status()
                prs = response.json()

                if not prs:
                    break

                for pr in prs:
                    updated_at = datetime.fromisoformat(
                        pr["updated_at"].replace("Z", "+00:00")
                    )
                    if updated_at < since_date:
                        # All subsequent PRs will be older, stop pagination
                        reached_cutoff = True
                        logger.info(
                            f"Reached cutoff date at PR #{pr['number']} "
                            f"(updated {updated_at.date()})"
                        )
                        break

                    # Fetch individual PR to get additions/deletions/changed_files
                    # (these fields aren't included in the list endpoint)
                    pr_detail_url = f"{GITHUB_API_URL}/repos/{repo}/pulls/{pr['number']}"
                    try:
                        time.sleep(API_DELAY_SECONDS)  # Rate limiting
                        response = requests.get(
                            pr_detail_url, headers=headers, timeout=30
                        )
                        response.raise_for_status()
                        pr_detail = response.json()
                        pr["additions"] = pr_detail.get("additions")
                        pr["deletions"] = pr_detail.get("deletions")
                        pr["changed_files"] = pr_detail.get("changed_files")
                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch details for PR #{pr['number']}: {e}"
                        )

                    pr["_repo"] = repo_name
                    all_prs.append(pr)
                    repo_pr_count += 1

                page += 1

            logger.info(f"Fetched {repo_pr_count} PRs from {repo_name}")

        # Store for use by reviews/comments sources
        self._fetched_prs = all_prs
        return all_prs

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": str(pr["id"]),
                "number": pr["number"],
                "repo": pr["_repo"],
                "title": pr["title"],
                "state": pr["state"],
                "merged": pr.get("merged_at") is not None,
                "author_id": str(pr["user"]["id"]) if pr.get("user") else None,
                "created_at": pr["created_at"],
                "updated_at": pr["updated_at"],
                "merged_at": pr.get("merged_at"),
                "closed_at": pr.get("closed_at"),
                "additions": pr.get("additions"),
                "deletions": pr.get("deletions"),
                "changed_files": pr.get("changed_files"),
            }
            for pr in raw_data
        ]


class GitHubPRReviewsSource(Source):
    """Fetches PR reviews from configured repositories."""

    dataset_id = "github"
    table_id = "raw_pr_reviews"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pull_request_id", "STRING"),
        bigquery.SchemaField("repo", "STRING"),
        bigquery.SchemaField("author_id", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP"),
        bigquery.SchemaField("body", "STRING"),
    ]

    def __init__(self, prs: list[dict[str, Any]] = None, lookback_days: int = 30):
        """
        Args:
            prs: List of PRs to fetch reviews for. If None, fetches PRs first.
            lookback_days: Days to look back when fetching PRs (if prs not provided)
        """
        self.prs = prs
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch reviews for all PRs."""
        prs = self.prs
        if prs is None:
            # Fetch PRs if not provided
            pr_source = GitHubPullRequestsSource(lookback_days=self.lookback_days)
            prs = pr_source.fetch()

        all_reviews = []
        for pr in prs:
            repo = pr.get("_repo") or pr.get("repo")
            pr_number = pr["number"]
            pr_id = str(pr["id"])

            url = f"{GITHUB_API_URL}/repos/{ORG_NAME}/{repo}/pulls/{pr_number}/reviews"
            reviews = fetch_paginated(url)

            for review in reviews:
                review["_pull_request_id"] = pr_id
                review["_repo"] = repo
                all_reviews.append(review)

        return all_reviews

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": str(review["id"]),
                "pull_request_id": review["_pull_request_id"],
                "repo": review["_repo"],
                "author_id": str(review["user"]["id"]) if review.get("user") else None,
                "state": review["state"],
                "submitted_at": review.get("submitted_at"),
                "body": review.get("body"),
            }
            for review in raw_data
        ]


class GitHubPRCommentsSource(Source):
    """Fetches PR review comments from configured repositories."""

    dataset_id = "github"
    table_id = "raw_pr_comments"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pull_request_id", "STRING"),
        bigquery.SchemaField("repo", "STRING"),
        bigquery.SchemaField("author_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("path", "STRING"),
        bigquery.SchemaField("body", "STRING"),
    ]

    def __init__(self, prs: list[dict[str, Any]] = None, lookback_days: int = 30):
        """
        Args:
            prs: List of PRs to fetch comments for. If None, fetches PRs first.
            lookback_days: Days to look back when fetching PRs (if prs not provided)
        """
        self.prs = prs
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch review comments for all PRs."""
        prs = self.prs
        if prs is None:
            pr_source = GitHubPullRequestsSource(lookback_days=self.lookback_days)
            prs = pr_source.fetch()

        all_comments = []
        for pr in prs:
            repo = pr.get("_repo") or pr.get("repo")
            pr_number = pr["number"]
            pr_id = str(pr["id"])

            # Review comments (inline code comments)
            url = f"{GITHUB_API_URL}/repos/{ORG_NAME}/{repo}/pulls/{pr_number}/comments"
            comments = fetch_paginated(url)

            for comment in comments:
                comment["_pull_request_id"] = pr_id
                comment["_repo"] = repo
                all_comments.append(comment)

        return all_comments

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": str(comment["id"]),
                "pull_request_id": comment["_pull_request_id"],
                "repo": comment["_repo"],
                "author_id": str(comment["user"]["id"]) if comment.get("user") else None,
                "created_at": comment["created_at"],
                "updated_at": comment.get("updated_at"),
                "path": comment.get("path"),
                "body": comment.get("body"),
            }
            for comment in raw_data
        ]

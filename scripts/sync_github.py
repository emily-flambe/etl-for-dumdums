#!/usr/bin/env python3
"""Sync GitHub data to BigQuery."""

import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env for local development (no-op if not present)
from dotenv import load_dotenv
load_dotenv()

from lib.source import run_sync
from lib import bigquery as bq
from sources.github import (
    GitHubUsersSource,
    GitHubPullRequestsSource,
    GitHubPRReviewsSource,
    GitHubPRCommentsSource,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    client = bq.get_client()

    # Step 1: Fetch PRs
    logger.info("Fetching pull requests...")
    pr_source = GitHubPullRequestsSource(lookback_days=30)
    prs = pr_source.fetch()
    pr_rows = pr_source.transform(prs)

    # Sync PRs
    bq.merge_table(
        client,
        pr_source.table_id,
        pr_rows,
        pr_source.schema,
        pr_source.primary_key,
        dataset_id=pr_source.dataset_id,
    )
    logger.info(f"Sync complete for {pr_source.__class__.__name__}")

    # Step 2: Fetch reviews
    logger.info("Fetching PR reviews...")
    review_source = GitHubPRReviewsSource(prs=prs)
    reviews = review_source.fetch()
    review_rows = review_source.transform(reviews)

    bq.merge_table(
        client,
        review_source.table_id,
        review_rows,
        review_source.schema,
        review_source.primary_key,
        dataset_id=review_source.dataset_id,
    )
    logger.info(f"Sync complete for {review_source.__class__.__name__}")

    # Step 3: Fetch comments
    logger.info("Fetching PR comments...")
    comment_source = GitHubPRCommentsSource(prs=prs)
    comments = comment_source.fetch()
    comment_rows = comment_source.transform(comments)

    bq.merge_table(
        client,
        comment_source.table_id,
        comment_rows,
        comment_source.schema,
        comment_source.primary_key,
        dataset_id=comment_source.dataset_id,
    )
    logger.info(f"Sync complete for {comment_source.__class__.__name__}")

    # Step 4: Extract users from all activity data
    logger.info("Extracting users from activity data...")
    user_source = GitHubUsersSource(prs=prs, reviews=reviews, comments=comments)
    run_sync(user_source)

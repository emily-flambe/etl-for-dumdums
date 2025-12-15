#!/usr/bin/env python3
"""Backfill Hacker News comments one day at a time with parallel sentiment analysis."""

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env for local development
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
from lib import bigquery as bq
from sources.hacker_news import (
    HNCommentsSource,
    analyze_sentiment_batch,
    clean_html,
    DEFAULT_TOP_STORIES_PER_DAY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_comments_for_date(target_date: datetime.date, top_stories: int) -> list[dict]:
    """Fetch comments for a specific date from BigQuery public dataset."""
    client = bq.get_client()

    query = r"""
    WITH stories_ranked AS (
        SELECT
            id,
            DATE(`timestamp`) as story_day,
            descendants,
            ROW_NUMBER() OVER (
                PARTITION BY DATE(`timestamp`)
                ORDER BY descendants DESC
            ) as rank_in_day
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = 'story'
          AND DATE(`timestamp`) = @target_date
          AND deleted IS NOT TRUE
          AND dead IS NOT TRUE
          AND descendants > 0
    ),
    top_stories AS (
        SELECT id, story_day
        FROM stories_ranked
        WHERE rank_in_day <= @top_stories
    )
    SELECT
        c.id,
        c.parent as parent_id,
        c.parent as story_id,
        c.`by` as author,
        c.text,
        c.`timestamp` as posted_at,
        DATE_TRUNC(DATE(c.`timestamp`), MONTH) as posted_month,
        DATE(c.`timestamp`) as posted_day
    FROM `bigquery-public-data.hacker_news.full` c
    JOIN top_stories ts ON c.parent = ts.id
    WHERE c.type = 'comment'
      AND c.deleted IS NOT TRUE
      AND c.dead IS NOT TRUE
      AND c.text IS NOT NULL
      AND c.text != ''
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            bigquery.ScalarQueryParameter("top_stories", "INT64", top_stories),
        ]
    )

    results = client.query(query, job_config=job_config).result()
    return [dict(row) for row in results]


def process_day(target_date: datetime.date, top_stories: int) -> int:
    """Process comments for a single day: fetch, analyze sentiment, and load."""
    logger.info(f"Processing {target_date}...")

    # Fetch comments
    comments = fetch_comments_for_date(target_date, top_stories)
    if not comments:
        logger.info(f"  No comments found for {target_date}")
        return 0

    logger.info(f"  Found {len(comments)} comments for {target_date}")

    # Get Cloudflare credentials
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_WORKERS_AI_TOKEN")

    if not account_id or not api_token:
        logger.warning("  Cloudflare credentials not set, skipping sentiment analysis")
        for row in comments:
            row["sentiment_score"] = None
            row["sentiment_label"] = None
            row["sentiment_category"] = None
    else:
        # Analyze sentiment
        cleaned_texts = [clean_html(row.get("text", "")) for row in comments]
        sentiments = analyze_sentiment_batch(cleaned_texts, account_id, api_token)

        for row, sentiment in zip(comments, sentiments):
            row["sentiment_score"] = sentiment["sentiment_score"]
            row["sentiment_label"] = sentiment["sentiment_label"]
            row["sentiment_category"] = sentiment["sentiment_category"]

    # Convert datetime objects to ISO strings for JSON serialization
    for row in comments:
        if row.get("posted_at"):
            row["posted_at"] = row["posted_at"].isoformat()
        if row.get("posted_month"):
            row["posted_month"] = str(row["posted_month"])
        if row.get("posted_day"):
            row["posted_day"] = str(row["posted_day"])

    # Load to BigQuery using the source's schema
    source = HNCommentsSource()
    client = bq.get_client()
    project_id = os.environ["GCP_PROJECT_ID"]
    table_ref = f"{project_id}.{source.dataset_id}.{source.table_id}"

    # Ensure table exists
    dataset_ref = client.dataset(source.dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)

    table = bigquery.Table(table_ref, schema=source.schema)
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(table)

    # Insert rows
    errors = client.insert_rows_json(table_ref, comments)
    if errors:
        logger.error(f"  Errors inserting rows: {errors[:3]}")
    else:
        logger.info(f"  Loaded {len(comments)} comments for {target_date}")

    return len(comments)


def main():
    parser = argparse.ArgumentParser(description="Backfill HN comments day by day")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (default: yesterday). Backfill goes backwards from this date.",
    )
    parser.add_argument(
        "--top-stories",
        type=int,
        default=DEFAULT_TOP_STORIES_PER_DAY,
        help=f"Top N stories per day (default: {DEFAULT_TOP_STORIES_PER_DAY})",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel days to process (default: 1, be careful with API limits)",
    )
    args = parser.parse_args()

    # Generate list of dates to process
    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start = datetime.now().date() - timedelta(days=1)

    dates = [start - timedelta(days=i) for i in range(args.days)]

    logger.info(f"Backfilling {len(dates)} days with {args.parallel} parallel workers")

    total_comments = 0

    if args.parallel == 1:
        # Sequential processing
        for target_date in dates:
            count = process_day(target_date, args.top_stories)
            total_comments += count
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(process_day, d, args.top_stories): d
                for d in dates
            }
            for future in as_completed(futures):
                target_date = futures[future]
                try:
                    count = future.result()
                    total_comments += count
                except Exception as e:
                    logger.error(f"Error processing {target_date}: {e}")

    logger.info(f"Backfill complete! Total comments: {total_comments}")

    # Run dbt to rebuild models with new data
    logger.info("Running dbt to rebuild Hacker News models...")
    import subprocess
    result = subprocess.run(
        ["make", "dbt-run-hacker-news"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        logger.info("dbt models rebuilt successfully")
    else:
        logger.error(f"dbt failed: {result.stderr}")


if __name__ == "__main__":
    main()

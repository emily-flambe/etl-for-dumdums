#!/usr/bin/env python3
"""Backfill Hacker News comment sentiment for historical data.

This script fetches comments from a specific date range and analyzes sentiment
using parallel API calls for faster processing.
"""

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import httpx
from google.cloud import bigquery

from lib import bigquery as bq
from sources.hacker_news import clean_html, HNCommentsSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def analyze_sentiment_single(text: str, account_id: str, api_token: str) -> dict:
    """Analyze sentiment for a single text using Cloudflare Workers AI."""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/huggingface/distilbert-sst-2-int8"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Skip empty or very short texts
    if not text or len(text) < 10:
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "NEUTRAL",
            "sentiment_category": "neutral",
        }

    # Truncate very long texts
    truncated = text[:1000] if len(text) > 1000 else text

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, headers=headers, json={"text": truncated})
            response.raise_for_status()
            data = response.json()

            if data.get("success") and data.get("result"):
                result = data["result"][0]
                label = result.get("label", "NEUTRAL")
                score = result.get("score", 0.5)

                # Convert to -1 to 1 scale
                if label == "POSITIVE":
                    sentiment_score = score
                else:
                    sentiment_score = -score

                # Categorize
                if sentiment_score > 0.25:
                    category = "positive"
                elif sentiment_score < -0.25:
                    category = "negative"
                else:
                    category = "neutral"

                return {
                    "sentiment_score": round(sentiment_score, 4),
                    "sentiment_label": label,
                    "sentiment_category": category,
                }
    except Exception as e:
        logger.debug(f"Sentiment API error: {e}")

    return {
        "sentiment_score": 0.0,
        "sentiment_label": "ERROR",
        "sentiment_category": "neutral",
    }


def analyze_sentiment_parallel(
    rows: list[dict],
    account_id: str,
    api_token: str,
    max_workers: int = 10,
) -> list[dict]:
    """Analyze sentiment for multiple rows in parallel."""

    # Clean texts first
    for row in rows:
        row["_clean_text"] = clean_html(row.get("text", ""))

    results = [None] * len(rows)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_idx = {
            executor.submit(
                analyze_sentiment_single,
                row["_clean_text"],
                account_id,
                api_token
            ): i
            for i, row in enumerate(rows)
        }

        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                logger.warning(f"Error processing row {idx}: {e}")
                results[idx] = {
                    "sentiment_score": 0.0,
                    "sentiment_label": "ERROR",
                    "sentiment_category": "neutral",
                }

            completed += 1
            if completed % 100 == 0:
                logger.info(f"Processed {completed}/{len(rows)} comments")

    # Merge results into rows
    for row, sentiment in zip(rows, results):
        row["sentiment_score"] = sentiment["sentiment_score"]
        row["sentiment_label"] = sentiment["sentiment_label"]
        row["sentiment_category"] = sentiment["sentiment_category"]
        del row["_clean_text"]

    return rows


def fetch_comments_for_date_range(
    start_date: str,
    end_date: str,
    top_stories_per_day: int = 30,
) -> list[dict]:
    """Fetch comments for a specific date range from BigQuery public dataset."""
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
          AND DATE(`timestamp`) BETWEEN @start_date AND @end_date
          AND deleted IS NOT TRUE
          AND dead IS NOT TRUE
          AND descendants > 0
    ),
    top_stories AS (
        SELECT id, story_day
        FROM stories_ranked
        WHERE rank_in_day <= @top_stories_per_day
    )
    SELECT
        c.id,
        c.parent as parent_id,
        c.parent as story_id,
        c.`by` as author,
        c.text,
        c.`timestamp` as posted_at,
        DATE(c.`timestamp`) as posted_day
    FROM `bigquery-public-data.hacker_news.full` c
    JOIN top_stories ts ON c.parent = ts.id
    WHERE c.type = 'comment'
      AND c.deleted IS NOT TRUE
      AND c.dead IS NOT TRUE
      AND c.text IS NOT NULL
      AND c.text != ''
    ORDER BY c.`timestamp` DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter("top_stories_per_day", "INT64", top_stories_per_day),
        ]
    )

    logger.info(f"Fetching comments for {start_date} to {end_date}...")
    result = client.query(query, job_config=job_config).result()

    rows = []
    for row in result:
        rows.append({
            "id": row.id,
            "parent_id": row.parent_id,
            "story_id": row.story_id,
            "author": row.author,
            "text": row.text,
            "posted_at": row.posted_at.isoformat() if row.posted_at else None,
            "posted_day": row.posted_day.isoformat() if row.posted_day else None,
        })

    return rows


def load_comments_to_bigquery(rows: list[dict]):
    """Load comments with sentiment to BigQuery using merge."""
    if not rows:
        logger.info("No rows to load")
        return

    # Use the existing source's load mechanism
    source = HNCommentsSource()
    client = bq.get_client()

    # Load to temp table and merge
    temp_table = f"{client.project}.raw_data.temp_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logger.info(f"Loading {len(rows)} rows to temp table {temp_table}...")

    job_config = bigquery.LoadJobConfig(
        schema=source.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    client.load_table_from_json(rows, temp_table, job_config=job_config).result()

    # Merge into main table
    target_table = f"{client.project}.{source.dataset_id}.{source.table_id}"

    merge_query = f"""
    MERGE `{target_table}` T
    USING `{temp_table}` S
    ON T.{source.primary_key} = S.{source.primary_key}
    WHEN MATCHED THEN
        UPDATE SET
            parent_id = S.parent_id,
            story_id = S.story_id,
            author = S.author,
            text = S.text,
            posted_at = S.posted_at,
            posted_day = S.posted_day,
            sentiment_score = S.sentiment_score,
            sentiment_label = S.sentiment_label,
            sentiment_category = S.sentiment_category
    WHEN NOT MATCHED THEN
        INSERT ROW
    """

    logger.info(f"Merging into {target_table}...")
    client.query(merge_query).result()

    # Cleanup temp table
    client.delete_table(temp_table)
    logger.info("Merge complete")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Hacker News comment sentiment for historical data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to backfill (default: 7)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for backfill in YYYY-MM-DD format (default: day before earliest existing data)",
    )
    parser.add_argument(
        "--top-stories",
        type=int,
        default=30,
        help="Top N stories by activity per day (default: 30)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel workers for sentiment analysis (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and analyze but don't save to BigQuery",
    )
    args = parser.parse_args()

    # Check Cloudflare credentials
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_WORKERS_AI_TOKEN")

    if not account_id or not api_token:
        logger.error("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_WORKERS_AI_TOKEN required")
        sys.exit(1)

    # Determine date range
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        # Find earliest existing date and go back from there
        client = bq.get_client()
        result = client.query("""
            SELECT MIN(posted_day) as earliest
            FROM hacker_news.raw_comments
            WHERE sentiment_score IS NOT NULL
        """).result()

        earliest = None
        for row in result:
            earliest = row.earliest

        if earliest:
            end_date = earliest - timedelta(days=1)
            logger.info(f"Earliest existing data: {earliest}, backfilling from {end_date}")
        else:
            end_date = datetime.now().date() - timedelta(days=1)
            logger.info(f"No existing data, starting from {end_date}")

    start_date = end_date - timedelta(days=args.days - 1)

    logger.info(f"Backfilling {args.days} days: {start_date} to {end_date}")
    logger.info(f"Using {args.workers} parallel workers")

    # Fetch comments
    rows = fetch_comments_for_date_range(
        start_date.isoformat(),
        end_date.isoformat(),
        args.top_stories,
    )

    logger.info(f"Fetched {len(rows)} comments")

    if not rows:
        logger.info("No comments to process")
        return

    # Analyze sentiment in parallel
    logger.info(f"Analyzing sentiment with {args.workers} workers...")
    rows = analyze_sentiment_parallel(rows, account_id, api_token, args.workers)

    # Load to BigQuery
    if args.dry_run:
        logger.info("Dry run - not saving to BigQuery")
        # Show sample
        for row in rows[:5]:
            print(f"  {row['posted_day']}: score={row['sentiment_score']:.2f} ({row['sentiment_category']})")
    else:
        load_comments_to_bigquery(rows)
        logger.info(f"Backfill complete: {len(rows)} comments processed")


if __name__ == "__main__":
    main()

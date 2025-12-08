"""
Hacker News data source.

Fetches stories from BigQuery public dataset and syncs to local BigQuery.
Includes sentiment analysis via Cloudflare Workers AI.
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from google.cloud import bigquery

from lib.source import Source
from lib import bigquery as bq

logger = logging.getLogger(__name__)

# Default lookback periods
DEFAULT_LOOKBACK_DAYS = 30  # Stories
DEFAULT_COMMENTS_LOOKBACK_DAYS = 7  # Comments (limited due to Cloudflare AI rate limits)
FULL_LOOKBACK_DAYS = 365 * 5  # 5 years


class HNStoriesSource(Source):
    """Fetches stories from Hacker News BigQuery public dataset.

    This source queries the public dataset and syncs to local BigQuery,
    allowing for faster queries and consistent patterns with other sources.
    """

    dataset_id = "hacker_news"
    table_id = "raw_stories"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("domain", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("descendants", "INTEGER"),
        bigquery.SchemaField("posted_at", "TIMESTAMP"),
        bigquery.SchemaField("posted_week", "DATE"),
    ]

    def __init__(self, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch stories from BigQuery public dataset."""
        logger.info(f"Fetching HN stories from last {self.lookback_days} days...")

        client = bq.get_client()

        query = r"""
        SELECT
            id,
            title,
            url,
            -- Extract domain from URL
            CASE
                WHEN url IS NOT NULL AND url != ''
                THEN REGEXP_EXTRACT(url, r'^https?://(?:www\.)?([^/]+)')
                ELSE NULL
            END as domain,
            `by` as author,
            score,
            descendants,
            `timestamp` as posted_at,
            DATE_TRUNC(DATE(`timestamp`), WEEK(MONDAY)) as posted_week
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = 'story'
          AND `timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
          AND deleted IS NOT TRUE
          AND dead IS NOT TRUE
        ORDER BY `timestamp` DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", self.lookback_days)
            ]
        )

        logger.info("Executing query against bigquery-public-data.hacker_news.full...")
        result = client.query(query, job_config=job_config).result()

        rows = []
        for row in result:
            rows.append({
                "id": row.id,
                "title": row.title,
                "url": row.url,
                "domain": row.domain,
                "author": row.author,
                "score": row.score,
                "descendants": row.descendants,
                "posted_at": row.posted_at.isoformat() if row.posted_at else None,
                "posted_week": row.posted_week.isoformat() if row.posted_week else None,
            })

        logger.info(f"Fetched {len(rows)} stories")
        return rows

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform is a no-op since we already shaped data in the query."""
        return raw_data


def clean_html(text: str) -> str:
    """Remove HTML tags and entities from text."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Remove HTML entities
    text = re.sub(r'&[a-z]+;', ' ', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def analyze_sentiment_batch(
    texts: list[str],
    account_id: str,
    api_token: str,
    batch_size: int = 50,
) -> list[dict[str, Any]]:
    """Analyze sentiment for a batch of texts using Cloudflare Workers AI.

    Uses the distilbert-sst-2-int8 model which returns POSITIVE/NEGATIVE labels.

    Args:
        texts: List of text strings to analyze
        account_id: Cloudflare account ID
        api_token: Cloudflare API token
        batch_size: Number of texts to process per API call

    Returns:
        List of dicts with sentiment_score, sentiment_label, sentiment_category
    """
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/huggingface/distilbert-sst-2-int8"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    results = []

    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]

            for text in batch:
                # Skip empty or very short texts
                if not text or len(text) < 10:
                    results.append({
                        "sentiment_score": 0.0,
                        "sentiment_label": "NEUTRAL",
                        "sentiment_category": "neutral",
                    })
                    continue

                # Truncate very long texts (model has token limits)
                truncated = text[:1000] if len(text) > 1000 else text

                try:
                    response = client.post(url, headers=headers, json={"text": truncated})
                    response.raise_for_status()
                    data = response.json()

                    if data.get("success") and data.get("result"):
                        result = data["result"][0]
                        label = result.get("label", "NEUTRAL")
                        score = result.get("score", 0.5)

                        # Convert to -1 to 1 scale
                        # POSITIVE with high confidence -> positive score
                        # NEGATIVE with high confidence -> negative score
                        if label == "POSITIVE":
                            sentiment_score = score  # 0.5 to 1.0
                        else:
                            sentiment_score = -score  # -0.5 to -1.0

                        # Categorize based on score
                        if sentiment_score > 0.25:
                            category = "positive"
                        elif sentiment_score < -0.25:
                            category = "negative"
                        else:
                            category = "neutral"

                        results.append({
                            "sentiment_score": round(sentiment_score, 4),
                            "sentiment_label": label,
                            "sentiment_category": category,
                        })
                    else:
                        results.append({
                            "sentiment_score": 0.0,
                            "sentiment_label": "UNKNOWN",
                            "sentiment_category": "neutral",
                        })
                except Exception as e:
                    logger.warning(f"Sentiment API error: {e}")
                    results.append({
                        "sentiment_score": 0.0,
                        "sentiment_label": "ERROR",
                        "sentiment_category": "neutral",
                    })

            # Rate limiting - be nice to the API
            if i + batch_size < len(texts):
                time.sleep(0.1)

            if (i + batch_size) % 500 == 0:
                logger.info(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} comments for sentiment")

    return results


class HNCommentsSource(Source):
    """Fetches comments from Hacker News BigQuery public dataset.

    This source queries comments that are direct replies to stories (top-level comments).
    Nested comments are excluded for simplicity - they can be added later via recursive CTE.

    Sentiment analysis is performed via Cloudflare Workers AI during ETL.
    """

    dataset_id = "hacker_news"
    table_id = "raw_comments"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("parent_id", "INTEGER"),
        bigquery.SchemaField("story_id", "INTEGER"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("posted_at", "TIMESTAMP"),
        bigquery.SchemaField("posted_month", "DATE"),
        # Sentiment fields (computed via Cloudflare Workers AI)
        bigquery.SchemaField("sentiment_score", "FLOAT"),
        bigquery.SchemaField("sentiment_label", "STRING"),
        bigquery.SchemaField("sentiment_category", "STRING"),
    ]

    def __init__(self, lookback_days: int = DEFAULT_COMMENTS_LOOKBACK_DAYS):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch top-level comments from BigQuery public dataset."""
        logger.info(f"Fetching HN comments from last {self.lookback_days} days...")

        client = bq.get_client()

        # Query top-level comments only (direct replies to stories)
        # This is simpler and covers the majority of sentiment signal
        query = r"""
        SELECT
            c.id,
            c.parent as parent_id,
            c.parent as story_id,
            c.`by` as author,
            c.text,
            c.`timestamp` as posted_at,
            DATE_TRUNC(DATE(c.`timestamp`), MONTH) as posted_month
        FROM `bigquery-public-data.hacker_news.full` c
        JOIN `bigquery-public-data.hacker_news.full` p ON c.parent = p.id
        WHERE c.type = 'comment'
          AND p.type = 'story'
          AND c.`timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
          AND c.deleted IS NOT TRUE
          AND c.dead IS NOT TRUE
          AND c.text IS NOT NULL
          AND c.text != ''
        ORDER BY c.`timestamp` DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", self.lookback_days)
            ]
        )

        logger.info("Executing query against bigquery-public-data.hacker_news.full...")
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
                "posted_month": row.posted_month.isoformat() if row.posted_month else None,
            })

        logger.info(f"Fetched {len(rows)} comments")
        return rows

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform comments and add sentiment analysis via Cloudflare Workers AI."""
        # Check for Cloudflare credentials
        account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
        api_token = os.environ.get("CLOUDFLARE_WORKERS_AI_TOKEN")

        if not account_id or not api_token:
            logger.warning(
                "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_WORKERS_AI_TOKEN not set. "
                "Skipping sentiment analysis."
            )
            # Return data without sentiment
            for row in raw_data:
                row["sentiment_score"] = None
                row["sentiment_label"] = None
                row["sentiment_category"] = None
            return raw_data

        logger.info(f"Running sentiment analysis on {len(raw_data)} comments...")

        # Clean text and run sentiment analysis
        cleaned_texts = [clean_html(row.get("text", "")) for row in raw_data]
        sentiments = analyze_sentiment_batch(cleaned_texts, account_id, api_token)

        # Merge sentiment results into rows
        for row, sentiment in zip(raw_data, sentiments):
            row["sentiment_score"] = sentiment["sentiment_score"]
            row["sentiment_label"] = sentiment["sentiment_label"]
            row["sentiment_category"] = sentiment["sentiment_category"]

        logger.info("Sentiment analysis complete")
        return raw_data

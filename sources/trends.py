"""
Google Trends data source.

Fetches keyword interest over time using pytrends.
Keywords are configured via environment variable TRENDS_KEYWORDS (comma-separated).
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery
from pytrends.request import TrendReq

from lib.source import Source

logger = logging.getLogger(__name__)

# Rate limit delay between requests (seconds)
RATE_LIMIT_DELAY = 5


def get_keywords() -> list[str]:
    """Get keywords to track from environment variable."""
    keywords_str = os.environ.get("TRENDS_KEYWORDS", "")
    if not keywords_str:
        raise ValueError(
            "TRENDS_KEYWORDS environment variable is not set. "
            "Set it to a comma-separated list of keywords, e.g., 'campaign,election,vote'"
        )
    return [kw.strip() for kw in keywords_str.split(",") if kw.strip()]


class GoogleTrendsSource(Source):
    """Fetches interest over time data from Google Trends.

    Tracks configured keywords and returns daily interest scores (0-100).
    Due to rate limits, this source fetches data for all keywords in batches
    of up to 5 (pytrends limit).
    """

    dataset_id = "trends"
    table_id = "raw_interest_over_time"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("interest", "INTEGER"),
        bigquery.SchemaField("is_partial", "BOOLEAN"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]

    def __init__(self, timeframe: str = "today 3-m", geo: str = "US"):
        """Initialize the trends source.

        Args:
            timeframe: Pytrends timeframe string. Options include:
                - 'today 3-m' (last 3 months, daily data)
                - 'today 12-m' (last 12 months, weekly data)
                - 'today 5-y' (last 5 years, weekly data)
                - 'YYYY-MM-DD YYYY-MM-DD' (custom date range)
            geo: Two-letter country code (e.g., 'US', 'GB') or '' for worldwide.
        """
        self.timeframe = timeframe
        self.geo = geo
        self.keywords = get_keywords()

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch interest over time for all configured keywords."""
        logger.info(f"Fetching Google Trends data for keywords: {self.keywords}")
        logger.info(f"Timeframe: {self.timeframe}, Geo: {self.geo or 'Worldwide'}")

        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))

        all_data = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        # Process keywords in batches of 5 (pytrends limit)
        for i in range(0, len(self.keywords), 5):
            batch = self.keywords[i : i + 5]
            logger.info(f"Fetching batch {i // 5 + 1}: {batch}")

            try:
                pytrends.build_payload(
                    batch,
                    cat=0,
                    timeframe=self.timeframe,
                    geo=self.geo,
                    gprop="",
                )

                df = pytrends.interest_over_time()

                if df.empty:
                    logger.warning(f"No data returned for batch: {batch}")
                    continue

                # Convert DataFrame to records
                for keyword in batch:
                    if keyword not in df.columns:
                        logger.warning(f"Keyword '{keyword}' not in response")
                        continue

                    for date_idx, row in df.iterrows():
                        all_data.append(
                            {
                                "keyword": keyword,
                                "date": date_idx,
                                "interest": row[keyword],
                                "is_partial": row.get("isPartial", False),
                                "geo": self.geo or "WORLD",
                                "fetched_at": fetched_at,
                            }
                        )

                logger.info(f"Retrieved {len(df)} data points for batch")

            except Exception as e:
                logger.error(f"Error fetching batch {batch}: {e}")
                raise

            # Rate limit delay between batches
            if i + 5 < len(self.keywords):
                logger.info(f"Waiting {RATE_LIMIT_DELAY}s for rate limit...")
                time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw data to BigQuery format."""
        return [
            {
                # Create unique ID from keyword + date + geo
                "id": f"{record['keyword']}_{record['date'].strftime('%Y-%m-%d')}_{record['geo']}",
                "keyword": record["keyword"],
                "date": record["date"].strftime("%Y-%m-%d"),
                "interest": int(record["interest"]),
                "is_partial": record["is_partial"],
                "geo": record["geo"],
                "fetched_at": record["fetched_at"],
            }
            for record in raw_data
        ]

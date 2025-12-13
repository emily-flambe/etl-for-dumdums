"""
FDA Food Events data source.

Fetches adverse food event reports from BigQuery public dataset and syncs to local BigQuery.
Data source: bigquery-public-data.fda_food.food_events

These are consumer-reported adverse reactions to food products, including symptoms,
outcomes, and product information. The reactions field contains comma-separated
symptoms that can be categorized via regex matching.
"""

import logging
from typing import Any

from google.cloud import bigquery

from lib.source import Source
from lib import bigquery as bq

logger = logging.getLogger(__name__)

# Default: 90 days lookback for incremental sync
DEFAULT_LOOKBACK_DAYS = 90

# Full sync: 10 years of data (2015-2025 covers peak reporting period)
FULL_LOOKBACK_DAYS = 365 * 10


class FDAFoodEventsSource(Source):
    """Fetches food adverse event reports from FDA BigQuery public dataset.

    This source queries the public dataset containing consumer-reported adverse
    reactions to food products. Key fields include:
    - reactions: Comma-separated symptoms (e.g., "DIARRHOEA, VOMITING, NAUSEA")
    - outcomes: Medical outcomes (e.g., "Hospitalization", "Visited Emergency Room")
    - products_*: Product information including brand name and industry category

    The reactions field is ideal for regex-based categorization into symptom groups
    (gastrointestinal, allergic, respiratory, cardiovascular, neurological, etc.)
    """

    dataset_id = "fda_food"
    table_id = "raw_food_events"
    primary_key = "report_number"
    schema = [
        bigquery.SchemaField("report_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("reactions", "STRING"),
        bigquery.SchemaField("outcomes", "STRING"),
        bigquery.SchemaField("products_brand_name", "STRING"),
        bigquery.SchemaField("products_industry_code", "STRING"),
        bigquery.SchemaField("products_role", "STRING"),
        bigquery.SchemaField("products_industry_name", "STRING"),
        bigquery.SchemaField("date_created", "DATE"),
        bigquery.SchemaField("date_started", "DATE"),
        bigquery.SchemaField("consumer_gender", "STRING"),
        bigquery.SchemaField("consumer_age", "FLOAT64"),
        bigquery.SchemaField("consumer_age_unit", "STRING"),
    ]

    def __init__(self, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
        """Initialize with lookback period.

        Args:
            lookback_days: Number of days to look back from today for events
        """
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch food events data from BigQuery public dataset.

        Returns:
            List of event records as dictionaries.

        Raises:
            Exception: If BigQuery query fails.
        """
        logger.info(f"Fetching FDA food events from last {self.lookback_days} days...")

        client = bq.get_client()

        query = """
        SELECT
            report_number,
            reactions,
            outcomes,
            products_brand_name,
            products_industry_code,
            products_role,
            products_industry_name,
            date_created,
            date_started,
            consumer_gender,
            consumer_age,
            consumer_age_unit
        FROM `bigquery-public-data.fda_food.food_events`
        WHERE date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
          AND date_created <= CURRENT_DATE()
        ORDER BY date_created DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", self.lookback_days)
            ]
        )

        logger.info("Executing query against bigquery-public-data.fda_food.food_events...")
        try:
            result = client.query(query, job_config=job_config).result()
        except Exception as e:
            logger.error(f"Failed to fetch FDA food events from BigQuery: {e}")
            raise

        rows = []
        for row in result:
            rows.append({
                "report_number": row.report_number,
                "reactions": row.reactions,
                "outcomes": row.outcomes,
                "products_brand_name": row.products_brand_name,
                "products_industry_code": row.products_industry_code,
                "products_role": row.products_role,
                "products_industry_name": row.products_industry_name,
                "date_created": row.date_created.isoformat() if row.date_created else None,
                "date_started": row.date_started.isoformat() if row.date_started else None,
                "consumer_gender": row.consumer_gender,
                "consumer_age": row.consumer_age,
                "consumer_age_unit": row.consumer_age_unit,
            })

        logger.info(f"Fetched {len(rows)} food events")
        return rows

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform is a no-op since we already shaped data in the query."""
        return raw_data

"""
Iowa Liquor Sales data source.

Fetches sales data from BigQuery public dataset and syncs to local BigQuery.
This is a large dataset (~33M rows), so incremental syncs are recommended.
"""

import logging
from typing import Any

from google.cloud import bigquery

from lib.source import Source
from lib import bigquery as bq

logger = logging.getLogger(__name__)

# Default lookback periods
DEFAULT_LOOKBACK_DAYS = 90  # ~3 months of recent sales
FULL_LOOKBACK_DAYS = 365 * 13  # ~13 years (data goes back to 2012)


class IowaLiquorSalesSource(Source):
    """Fetches sales from Iowa Liquor Sales BigQuery public dataset.

    This source queries the public dataset and syncs to local BigQuery.
    The data includes liquor sales transactions from Iowa stores.
    """

    dataset_id = "iowa_liquor"
    table_id = "raw_sales"
    primary_key = "invoice_and_item_number"
    schema = [
        bigquery.SchemaField("invoice_and_item_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("store_number", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("zip_code", "STRING"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("category_name", "STRING"),
        bigquery.SchemaField("vendor_number", "STRING"),
        bigquery.SchemaField("vendor_name", "STRING"),
        bigquery.SchemaField("item_number", "STRING"),
        bigquery.SchemaField("item_description", "STRING"),
        bigquery.SchemaField("pack", "INTEGER"),
        bigquery.SchemaField("bottle_volume_ml", "INTEGER"),
        bigquery.SchemaField("state_bottle_cost", "FLOAT"),
        bigquery.SchemaField("state_bottle_retail", "FLOAT"),
        bigquery.SchemaField("bottles_sold", "INTEGER"),
        bigquery.SchemaField("sale_dollars", "FLOAT"),
        bigquery.SchemaField("volume_sold_liters", "FLOAT"),
        bigquery.SchemaField("volume_sold_gallons", "FLOAT"),
        # Derived fields for aggregation
        bigquery.SchemaField("sale_month", "DATE"),
        bigquery.SchemaField("sale_year", "INTEGER"),
    ]

    def __init__(self, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch sales from BigQuery public dataset."""
        logger.info(f"Fetching Iowa liquor sales from last {self.lookback_days} days...")

        client = bq.get_client()

        query = """
        SELECT
            invoice_and_item_number,
            date,
            store_number,
            store_name,
            address,
            city,
            zip_code,
            county,
            category,
            category_name,
            vendor_number,
            vendor_name,
            item_number,
            item_description,
            pack,
            bottle_volume_ml,
            state_bottle_cost,
            state_bottle_retail,
            bottles_sold,
            sale_dollars,
            volume_sold_liters,
            volume_sold_gallons,
            -- Derived fields
            DATE_TRUNC(date, MONTH) as sale_month,
            EXTRACT(YEAR FROM date) as sale_year
        FROM `bigquery-public-data.iowa_liquor_sales.sales`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        ORDER BY date DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", self.lookback_days)
            ]
        )

        logger.info("Executing query against bigquery-public-data.iowa_liquor_sales.sales...")
        result = client.query(query, job_config=job_config).result()

        rows = []
        for row in result:
            rows.append({
                "invoice_and_item_number": row.invoice_and_item_number,
                "date": row.date.isoformat() if row.date else None,
                "store_number": row.store_number,
                "store_name": row.store_name,
                "address": row.address,
                "city": row.city,
                "zip_code": row.zip_code,
                "county": row.county,
                "category": row.category,
                "category_name": row.category_name,
                "vendor_number": row.vendor_number,
                "vendor_name": row.vendor_name,
                "item_number": row.item_number,
                "item_description": row.item_description,
                "pack": row.pack,
                "bottle_volume_ml": row.bottle_volume_ml,
                "state_bottle_cost": float(row.state_bottle_cost) if row.state_bottle_cost else None,
                "state_bottle_retail": float(row.state_bottle_retail) if row.state_bottle_retail else None,
                "bottles_sold": row.bottles_sold,
                "sale_dollars": float(row.sale_dollars) if row.sale_dollars else None,
                "volume_sold_liters": float(row.volume_sold_liters) if row.volume_sold_liters else None,
                "volume_sold_gallons": float(row.volume_sold_gallons) if row.volume_sold_gallons else None,
                "sale_month": row.sale_month.isoformat() if row.sale_month else None,
                "sale_year": row.sale_year,
            })

        logger.info(f"Fetched {len(rows)} sales records")
        return rows

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform is a no-op since we already shaped data in the query."""
        return raw_data

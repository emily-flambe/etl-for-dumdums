"""
Oura data source.

Fetches sleep, readiness, and activity data from Oura API v2.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from google.cloud import bigquery

from lib.source import Source

logger = logging.getLogger(__name__)

OURA_API_URL = "https://api.ouraring.com/v2"


def get_token() -> str:
    """Get Oura API token from environment."""
    token = os.environ.get("OURA_API_TOKEN")
    if not token:
        raise ValueError("OURA_API_TOKEN environment variable is not set")
    return token


def get_headers() -> dict[str, str]:
    """Get headers for Oura API requests."""
    return {
        "Authorization": f"Bearer {get_token()}",
    }


def fetch_paginated_with_token(url: str, params: dict = None) -> list[dict]:
    """Fetch all pages from an Oura API endpoint using next_token pagination."""
    headers = get_headers()
    all_items = []
    next_token = None
    page = 1
    params = params or {}

    while True:
        logger.info(f"Fetching page {page} from {url}...")

        request_params = {**params}
        if next_token:
            request_params["next_token"] = next_token

        response = requests.get(
            url,
            headers=headers,
            params=request_params,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        items = data.get("data", [])

        if not items:
            break

        all_items.extend(items)
        logger.info(f"Retrieved {len(items)} items on page {page}")

        # Check for next_token
        next_token = data.get("next_token")
        if not next_token:
            break

        page += 1

    logger.info(f"Total items fetched from {url}: {len(all_items)}")
    return all_items


class OuraSleepSource(Source):
    """Fetches daily sleep summaries from Oura.

    Uses the daily_sleep endpoint which provides one aggregated record per day
    with sleep score and contributor scores.
    """

    dataset_id = "oura"
    table_id = "raw_sleep"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("day", "DATE"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("contributor_deep_sleep", "INTEGER"),
        bigquery.SchemaField("contributor_efficiency", "INTEGER"),
        bigquery.SchemaField("contributor_latency", "INTEGER"),
        bigquery.SchemaField("contributor_rem_sleep", "INTEGER"),
        bigquery.SchemaField("contributor_restfulness", "INTEGER"),
        bigquery.SchemaField("contributor_timing", "INTEGER"),
        bigquery.SchemaField("contributor_total_sleep", "INTEGER"),
    ]

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=self.lookback_days)

        logger.info(f"Fetching daily sleep data from {start_date} to {end_date}")

        url = f"{OURA_API_URL}/usercollection/daily_sleep"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        return fetch_paginated_with_token(url, params)

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": record["id"],
                "day": record.get("day"),
                "score": record.get("score"),
                "contributor_deep_sleep": record.get("contributors", {}).get("deep_sleep") if record.get("contributors") else None,
                "contributor_efficiency": record.get("contributors", {}).get("efficiency") if record.get("contributors") else None,
                "contributor_latency": record.get("contributors", {}).get("latency") if record.get("contributors") else None,
                "contributor_rem_sleep": record.get("contributors", {}).get("rem_sleep") if record.get("contributors") else None,
                "contributor_restfulness": record.get("contributors", {}).get("restfulness") if record.get("contributors") else None,
                "contributor_timing": record.get("contributors", {}).get("timing") if record.get("contributors") else None,
                "contributor_total_sleep": record.get("contributors", {}).get("total_sleep") if record.get("contributors") else None,
            }
            for record in raw_data
        ]


class OuraReadinessSource(Source):
    """Fetches daily readiness scores from Oura."""

    dataset_id = "oura"
    table_id = "raw_daily_readiness"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("day", "DATE"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("temperature_deviation", "FLOAT"),
        bigquery.SchemaField("contributor_activity_balance", "INTEGER"),
        bigquery.SchemaField("contributor_body_temperature", "INTEGER"),
        bigquery.SchemaField("contributor_hrv_balance", "INTEGER"),
        bigquery.SchemaField("contributor_previous_day_activity", "INTEGER"),
        bigquery.SchemaField("contributor_previous_night", "INTEGER"),
        bigquery.SchemaField("contributor_recovery_index", "INTEGER"),
        bigquery.SchemaField("contributor_resting_heart_rate", "INTEGER"),
        bigquery.SchemaField("contributor_sleep_balance", "INTEGER"),
    ]

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=self.lookback_days)

        logger.info(f"Fetching readiness data from {start_date} to {end_date}")

        url = f"{OURA_API_URL}/usercollection/daily_readiness"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        return fetch_paginated_with_token(url, params)

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": record["id"],
                "day": record.get("day"),
                "score": record.get("score"),
                "temperature_deviation": record.get("temperature_deviation"),
                "contributor_activity_balance": record.get("contributors", {}).get("activity_balance") if record.get("contributors") else None,
                "contributor_body_temperature": record.get("contributors", {}).get("body_temperature") if record.get("contributors") else None,
                "contributor_hrv_balance": record.get("contributors", {}).get("hrv_balance") if record.get("contributors") else None,
                "contributor_previous_day_activity": record.get("contributors", {}).get("previous_day_activity") if record.get("contributors") else None,
                "contributor_previous_night": record.get("contributors", {}).get("previous_night") if record.get("contributors") else None,
                "contributor_recovery_index": record.get("contributors", {}).get("recovery_index") if record.get("contributors") else None,
                "contributor_resting_heart_rate": record.get("contributors", {}).get("resting_heart_rate") if record.get("contributors") else None,
                "contributor_sleep_balance": record.get("contributors", {}).get("sleep_balance") if record.get("contributors") else None,
            }
            for record in raw_data
        ]


class OuraActivitySource(Source):
    """Fetches daily activity summaries from Oura."""

    dataset_id = "oura"
    table_id = "raw_daily_activity"
    primary_key = "id"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("day", "DATE"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("active_calories", "INTEGER"),
        bigquery.SchemaField("total_calories", "INTEGER"),
        bigquery.SchemaField("steps", "INTEGER"),
        bigquery.SchemaField("equivalent_walking_distance", "INTEGER"),
        bigquery.SchemaField("high_activity_time", "INTEGER"),
        bigquery.SchemaField("medium_activity_time", "INTEGER"),
        bigquery.SchemaField("low_activity_time", "INTEGER"),
        bigquery.SchemaField("sedentary_time", "INTEGER"),
        bigquery.SchemaField("resting_time", "INTEGER"),
        bigquery.SchemaField("contributor_meet_daily_targets", "INTEGER"),
        bigquery.SchemaField("contributor_move_every_hour", "INTEGER"),
        bigquery.SchemaField("contributor_recovery_time", "INTEGER"),
        bigquery.SchemaField("contributor_stay_active", "INTEGER"),
        bigquery.SchemaField("contributor_training_frequency", "INTEGER"),
        bigquery.SchemaField("contributor_training_volume", "INTEGER"),
    ]

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days

    def fetch(self) -> list[dict[str, Any]]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=self.lookback_days)

        logger.info(f"Fetching activity data from {start_date} to {end_date}")

        url = f"{OURA_API_URL}/usercollection/daily_activity"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        return fetch_paginated_with_token(url, params)

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": record["id"],
                "day": record.get("day"),
                "score": record.get("score"),
                "active_calories": record.get("active_calories"),
                "total_calories": record.get("total_calories"),
                "steps": record.get("steps"),
                "equivalent_walking_distance": record.get("equivalent_walking_distance"),
                "high_activity_time": record.get("high_activity_time"),
                "medium_activity_time": record.get("medium_activity_time"),
                "low_activity_time": record.get("low_activity_time"),
                "sedentary_time": record.get("sedentary_time"),
                "resting_time": record.get("resting_time"),
                "contributor_meet_daily_targets": record.get("contributors", {}).get("meet_daily_targets") if record.get("contributors") else None,
                "contributor_move_every_hour": record.get("contributors", {}).get("move_every_hour") if record.get("contributors") else None,
                "contributor_recovery_time": record.get("contributors", {}).get("recovery_time") if record.get("contributors") else None,
                "contributor_stay_active": record.get("contributors", {}).get("stay_active") if record.get("contributors") else None,
                "contributor_training_frequency": record.get("contributors", {}).get("training_frequency") if record.get("contributors") else None,
                "contributor_training_volume": record.get("contributors", {}).get("training_volume") if record.get("contributors") else None,
            }
            for record in raw_data
        ]

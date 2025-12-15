"""
Stock price data source.

Fetches daily OHLCV data from Yahoo Finance using yfinance.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf
from google.cloud import bigquery

from lib.source import Source

logger = logging.getLogger(__name__)

# Stock tickers organized by sector
TICKERS_BY_SECTOR = {
    "Technology": ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMD", "CRM"],
    "Healthcare": ["JNJ", "UNH", "PFE", "ABBV", "MRK", "LLY"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "OXY", "NEE"],
    "Industrial": ["CAT", "HON", "BA", "UPS", "GE", "DE"],
    "Consumer Retail": ["AMZN", "WMT", "COST", "HD", "NKE", "SBUX", "TGT"],
}

# Flat list of all tickers
ALL_TICKERS = [ticker for tickers in TICKERS_BY_SECTOR.values() for ticker in tickers]

# Reverse lookup: ticker -> sector
TICKER_TO_SECTOR = {
    ticker: sector
    for sector, tickers in TICKERS_BY_SECTOR.items()
    for ticker in tickers
}


class StockPricesSource(Source):
    """Fetches daily stock prices from Yahoo Finance.

    Uses yfinance to get OHLCV data for a configurable set of tickers.
    Primary key is composite: ticker + date.
    """

    dataset_id = "stocks"
    table_id = "raw_prices"
    primary_key = "id"  # Composite key: {ticker}_{date}
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("adj_close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]

    def __init__(self, lookback_days: int = 30, tickers: list[str] | None = None):
        """Initialize the stock prices source.

        Args:
            lookback_days: Number of days of historical data to fetch
            tickers: List of ticker symbols. Defaults to ALL_TICKERS.
        """
        self.lookback_days = lookback_days
        self.tickers = tickers or ALL_TICKERS

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch stock price data from Yahoo Finance."""
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=self.lookback_days)

        logger.info(
            f"Fetching stock data for {len(self.tickers)} tickers "
            f"from {start_date} to {end_date}"
        )

        # yfinance can fetch multiple tickers at once
        tickers_str = " ".join(self.tickers)
        df = yf.download(
            tickers_str,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            progress=False,
            group_by="ticker",
        )

        if df.empty:
            logger.warning("No data returned from yfinance")
            return []

        # Convert MultiIndex DataFrame to list of records
        records = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        # Handle single ticker case (no MultiIndex)
        if len(self.tickers) == 1:
            ticker = self.tickers[0]
            for date_idx, row in df.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                records.append({
                    "ticker": ticker,
                    "date": date_str,
                    "open": row.get("Open"),
                    "high": row.get("High"),
                    "low": row.get("Low"),
                    "close": row.get("Close"),
                    "adj_close": row.get("Adj Close"),
                    "volume": row.get("Volume"),
                    "fetched_at": fetched_at,
                })
        else:
            # Multiple tickers - data is grouped by ticker
            for ticker in self.tickers:
                if ticker not in df.columns.get_level_values(0):
                    logger.warning(f"No data for ticker {ticker}")
                    continue

                ticker_df = df[ticker]
                for date_idx, row in ticker_df.iterrows():
                    date_str = date_idx.strftime("%Y-%m-%d")
                    records.append({
                        "ticker": ticker,
                        "date": date_str,
                        "open": row.get("Open"),
                        "high": row.get("High"),
                        "low": row.get("Low"),
                        "close": row.get("Close"),
                        "adj_close": row.get("Adj Close"),
                        "volume": row.get("Volume"),
                        "fetched_at": fetched_at,
                    })

        logger.info(f"Fetched {len(records)} price records")
        return records

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw data to BigQuery row format."""
        rows = []
        for record in raw_data:
            ticker = record["ticker"]
            date = record["date"]

            # Handle NaN values from pandas
            def clean_float(val):
                if val is None:
                    return None
                try:
                    import math
                    if math.isnan(val):
                        return None
                    return float(val)
                except (TypeError, ValueError):
                    return None

            def clean_int(val):
                if val is None:
                    return None
                try:
                    import math
                    if math.isnan(val):
                        return None
                    return int(val)
                except (TypeError, ValueError):
                    return None

            rows.append({
                "id": f"{ticker}_{date}",
                "ticker": ticker,
                "sector": TICKER_TO_SECTOR.get(ticker),
                "date": date,
                "open": clean_float(record.get("open")),
                "high": clean_float(record.get("high")),
                "low": clean_float(record.get("low")),
                "close": clean_float(record.get("close")),
                "adj_close": clean_float(record.get("adj_close")),
                "volume": clean_int(record.get("volume")),
                "fetched_at": record.get("fetched_at"),
            })

        return rows

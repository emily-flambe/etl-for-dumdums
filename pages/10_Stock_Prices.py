"""
Stock Prices dashboard.
"""

from datetime import datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from data import load_sector_performance, load_stock_prices

st.title("Stock Prices")

st.caption("Daily stock price data for 32 major tickers across 5 sectors. Source: [Yahoo Finance](https://finance.yahoo.com/) via [yfinance](https://github.com/ranaroussi/yfinance)")

# Load data
try:
    df = load_stock_prices()
    sector_df = load_sector_performance()
except Exception as e:
    st.error(f"Could not load stock data: {e}")
    st.info(
        "Make sure you have:\n"
        "1. Run `make sync-stocks` to fetch data from Yahoo Finance\n"
        "2. Run `make dbt-stocks` to build the dbt models"
    )
    st.stop()

if df.empty:
    st.warning("No stock data available. Run `make sync-stocks` to fetch data.")
    st.stop()

# Convert date column to proper datetime for Altair compatibility
df["trade_date"] = pd.to_datetime(df["trade_date"])


# Filter options
sectors = sorted(df["sector"].dropna().unique())
all_tickers = sorted(df["ticker"].unique())
min_date = df["trade_date"].min()
max_date = df["trade_date"].max()

# Date range options
DATE_RANGES = {
    "1D": 1,
    "7D": 7,
    "30D": 30,
    "90D": 90,
    "YTD": None,  # Special case
    "1Y": 365,
}

# Initialize session state for pills
if "sector_pills" not in st.session_state:
    st.session_state["sector_pills"] = sectors
if "ticker_pills" not in st.session_state:
    st.session_state["ticker_pills"] = all_tickers[:10]
if "date_range_pill" not in st.session_state:
    st.session_state["date_range_pill"] = "30D"

# Filters section
with st.expander("Filters", expanded=False):
    # Date range pills (single select)
    st.markdown("**Date Range**")
    selected_date_range = st.pills(
        "Date Range",
        list(DATE_RANGES.keys()),
        selection_mode="single",
        label_visibility="collapsed",
        key="date_range_pill",
        default="30D",
    )

    # Sectors pills
    st.markdown("**Sectors**")
    col1, col2, col3 = st.columns([1, 1, 6])
    with col1:
        if st.button("All", key="sectors_all", use_container_width=True):
            st.session_state["sector_pills"] = sectors
            st.rerun()
    with col2:
        if st.button("Clear", key="sectors_clear", use_container_width=True):
            st.session_state["sector_pills"] = []
            st.rerun()

    selected_sectors = st.pills(
        "Sectors",
        sectors,
        selection_mode="multi",
        label_visibility="collapsed",
        key="sector_pills",
    )

    # Get available tickers based on selected sectors
    available_tickers = sorted(
        df[df["sector"].isin(selected_sectors)]["ticker"].unique()
    ) if selected_sectors else all_tickers

    # Tickers pills
    st.markdown("**Tickers**")
    col1, col2, col3 = st.columns([1, 1, 6])
    with col1:
        if st.button("All", key="tickers_all", use_container_width=True):
            st.session_state["ticker_pills"] = available_tickers
            st.rerun()
    with col2:
        if st.button("Clear", key="tickers_clear", use_container_width=True):
            st.session_state["ticker_pills"] = []
            st.rerun()

    selected_tickers = st.pills(
        "Tickers",
        available_tickers,
        selection_mode="multi",
        label_visibility="collapsed",
        key="ticker_pills",
    )

# Calculate date range based on selection
if selected_date_range == "YTD":
    start_date = pd.Timestamp(datetime(max_date.year, 1, 1))
else:
    days = DATE_RANGES.get(selected_date_range, 30)
    start_date = max_date - timedelta(days=days)

start_date = max(start_date, min_date)
end_date = max_date

# Apply filters
filtered = df.copy()
if selected_sectors:
    filtered = filtered[filtered["sector"].isin(selected_sectors)]
if selected_tickers:
    filtered = filtered[filtered["ticker"].isin(selected_tickers)]
filtered = filtered[
    (filtered["trade_date"] >= start_date)
    & (filtered["trade_date"] <= end_date)
]

# --- Sector Performance Overview ---
st.header("Sector Performance")

if not sector_df.empty:
    cols = st.columns(len(sector_df))
    for i, (_, row) in enumerate(sector_df.iterrows()):
        sentiment_emoji = {
            "bullish": "",
            "bearish": "",
            "neutral": ""
        }.get(row.get("sector_sentiment", ""), "")

        cols[i].metric(
            f"{row['sector']} {sentiment_emoji}",
            f"{row['avg_daily_change_pct']:+.2f}%",
            delta=f"{row['gainers']:.0f}G / {row['losers']:.0f}L",
            delta_color="normal" if row['gainers'] >= row['losers'] else "inverse",
        )

# --- Stock Performance & Today's Movers (side by side) ---
perf_col, movers_col = st.columns([3, 2])

# Get most recent day's data for Today's Movers
latest = filtered[
    (filtered["recency_rank"] == 1) & (filtered["close_price"].notna())
].copy()

with perf_col:
    st.header("Stock Performance")
    st.caption(f"{start_date.strftime('%b %d, %Y')} – {end_date.strftime('%b %d, %Y')}")

    # Get performance for each selected ticker over the date range
    if not filtered.empty and selected_tickers:
        # Calculate performance for each ticker
        stock_perf = []
        for ticker in selected_tickers:
            ticker_data = filtered[filtered["ticker"] == ticker].sort_values("trade_date")
            if len(ticker_data) >= 2:
                first_price = ticker_data.iloc[0]["close_price"]
                last_price = ticker_data.iloc[-1]["close_price"]
                if first_price and last_price and first_price > 0:
                    change_pct = ((last_price - first_price) / first_price) * 100
                    stock_perf.append({
                        "Ticker": ticker,
                        "Change %": change_pct,
                        "Sector": ticker_data.iloc[0]["sector"],
                        "Start": first_price,
                        "End": last_price,
                    })

        if stock_perf:
            # Sort by change percentage and convert to dataframe
            stock_perf = sorted(stock_perf, key=lambda x: x["Change %"], reverse=True)
            perf_df = pd.DataFrame(stock_perf)

            # Apply conditional formatting (subtle)
            def color_change(val):
                if val > 0:
                    return "background-color: rgba(46, 125, 50, 0.1); color: #2e7d32"
                elif val < 0:
                    return "background-color: rgba(198, 40, 40, 0.1); color: #c62828"
                return ""

            styled_df = (
                perf_df.style
                .format({"Start": "${:.2f}", "End": "${:.2f}", "Change %": "{:+.2f}%"})
                .map(color_change, subset=["Change %"])
            )

            st.dataframe(
                styled_df,
                use_container_width=False,
                hide_index=True,
            )
        else:
            st.info("Not enough data to calculate stock performance for the selected range.")
    else:
        st.info("Select tickers to see performance.")

with movers_col:
    st.header("Today's Movers")

    if not latest.empty:
        st.subheader("Top Gainers")
        gainers = latest[latest["close_change_pct"] > 0].nlargest(5, "close_change_pct")[
            ["ticker", "close_change_pct", "sector", "close_price"]
        ].copy()
        gainers.columns = ["Ticker", "Change %", "Sector", "Price"]

        if not gainers.empty:
            styled_gainers = (
                gainers.style
                .format({"Price": "${:.2f}", "Change %": "{:+.2f}%"})
                .map(lambda _: "background-color: rgba(46, 125, 50, 0.1); color: #2e7d32", subset=["Change %"])
            )
            st.dataframe(styled_gainers, use_container_width=False, hide_index=True)
        else:
            st.info("No gainers today.")

        st.subheader("Top Losers")
        losers = latest[latest["close_change_pct"] < 0].nsmallest(5, "close_change_pct")[
            ["ticker", "close_change_pct", "sector", "close_price"]
        ].copy()
        losers.columns = ["Ticker", "Change %", "Sector", "Price"]

        if not losers.empty:
            styled_losers = (
                losers.style
                .format({"Price": "${:.2f}", "Change %": "{:+.2f}%"})
                .map(lambda _: "background-color: rgba(198, 40, 40, 0.1); color: #c62828", subset=["Change %"])
            )
            st.dataframe(styled_losers, use_container_width=False, hide_index=True)
        else:
            st.info("No losers today.")

# --- Price Chart ---
st.header("Price History")

# Filter out rows with missing close prices
price_data = filtered[filtered["close_price"].notna()].copy()

if not price_data.empty:
    # Option to normalize prices
    normalize = st.checkbox("Normalize prices (start at 100)", value=True)

    if normalize:
        st.caption("If you invested 100 dollars in each stock at the start of this period, the Y-axis shows what that investment would be worth now.")
        # Get first price for each ticker in the filtered range
        first_prices = (
            price_data.sort_values("trade_date")
            .groupby("ticker")["close_price"]
            .first()
            .to_dict()
        )
        price_data["display_price"] = price_data.apply(
            lambda row: (row["close_price"] / first_prices.get(row["ticker"], 1)) * 100
            if first_prices.get(row["ticker"]) else row["close_price"],
            axis=1
        )
        y_title = "Normalized Price (Start = 100)"
    else:
        price_data["display_price"] = price_data["close_price"]
        y_title = "Close Price ($)"

    # Sort by date for proper line rendering
    price_data = price_data.sort_values(["ticker", "trade_date"]).reset_index(drop=True)

    price_chart = (
        alt.Chart(price_data)
        .mark_line(point=True)
        .encode(
            x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format="%b %d")),
            y=alt.Y("display_price:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=alt.Color("ticker:N", title="Ticker"),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Date", format="%b %d, %Y"),
                alt.Tooltip("ticker:N", title="Ticker"),
                alt.Tooltip("sector:N", title="Sector"),
                alt.Tooltip("close_price:Q", title="Close", format="$.2f"),
                alt.Tooltip("display_price:Q", title="Normalized", format=".2f"),
            ],
        )
        .properties(height=400)
    )

    st.altair_chart(price_chart, use_container_width=True)

# --- Raw Data Table ---
st.header("Raw Data")

display_cols = [
    "trade_date", "ticker", "sector", "open_price", "high_price", "low_price",
    "close_price", "volume", "close_change_pct", "close_7d_ma", "close_30d_ma",
    "position_in_52w_range", "ma_trend", "volume_trend"
]

display_df = filtered[display_cols].copy()
display_df = display_df.sort_values(["trade_date", "ticker"], ascending=[False, True])

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "trade_date": st.column_config.DateColumn("Date", width="small"),
        "ticker": st.column_config.TextColumn("Ticker", width="small"),
        "sector": st.column_config.TextColumn("Sector", width="medium"),
        "open_price": st.column_config.NumberColumn("Open", format="$%.2f", width="small"),
        "high_price": st.column_config.NumberColumn("High", format="$%.2f", width="small"),
        "low_price": st.column_config.NumberColumn("Low", format="$%.2f", width="small"),
        "close_price": st.column_config.NumberColumn("Close", format="$%.2f", width="small"),
        "volume": st.column_config.NumberColumn("Volume", format="%d", width="small"),
        "close_change_pct": st.column_config.NumberColumn("Change %", format="%+.2f%%", width="small"),
        "close_7d_ma": st.column_config.NumberColumn("7d MA", format="$%.2f", width="small"),
        "close_30d_ma": st.column_config.NumberColumn("30d MA", format="$%.2f", width="small"),
        "position_in_52w_range": st.column_config.NumberColumn("52W Pos", format="%.1f%%", width="small"),
        "ma_trend": st.column_config.TextColumn("MA Trend", width="small"),
        "volume_trend": st.column_config.TextColumn("Vol Trend", width="small"),
    },
)

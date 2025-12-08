"""
Google Trends dashboard.
"""

from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st

from data import load_keyword_trends

st.set_page_config(page_title="Google Trends", layout="wide")
st.title("Google Trends")

# Load data
try:
    df = load_keyword_trends()
except Exception as e:
    st.error(f"Could not load Google Trends data: {e}")
    st.info(
        "Make sure you have:\n"
        "1. Set the `TRENDS_KEYWORDS` environment variable (comma-separated keywords)\n"
        "2. Run `make sync-trends` to fetch data from Google Trends\n"
        "3. Run `make dbt-trends` to build the dbt models"
    )
    st.stop()

if df.empty:
    st.warning("No trends data available. Run `make sync-trends` to fetch data.")
    st.info(
        "Set the TRENDS_KEYWORDS environment variable to a comma-separated list "
        "of keywords to track, e.g., 'campaign,election,vote'"
    )
    st.stop()

# Convert date column to proper datetime for Altair compatibility
df["date"] = pd.to_datetime(df["date"])

# Sidebar filters
st.sidebar.header("Filters")

# Keyword filter
keywords = sorted(df["keyword"].unique())
selected_keywords = st.sidebar.multiselect(
    "Keywords",
    keywords,
    default=keywords[:5] if len(keywords) > 5 else keywords,
)

# Date range filter
min_date = df["date"].min()
max_date = df["date"].max()
default_start = max(min_date, max_date - timedelta(days=90))
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Geo filter
geos = sorted(df["geo"].unique())
selected_geo = st.sidebar.selectbox("Region", geos, index=0)

# Apply filters
filtered = df.copy()
if selected_keywords:
    filtered = filtered[filtered["keyword"].isin(selected_keywords)]
if len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["date"] >= pd.Timestamp(start_date))
        & (filtered["date"] <= pd.Timestamp(end_date))
    ]
filtered = filtered[filtered["geo"] == selected_geo]

# Get latest data for metrics
latest = filtered[filtered["recency_rank"] == 1]

# Metrics row
st.subheader("Current Interest Levels")
if not latest.empty:
    cols = st.columns(min(len(selected_keywords), 5))
    for i, (_, row) in enumerate(latest.iterrows()):
        if i < len(cols):
            delta = row["interest_wow_change"]
            delta_str = f"{delta:+.0f} WoW" if pd.notna(delta) else None
            cols[i].metric(
                row["keyword"],
                f"{row['interest']:.0f}",
                delta=delta_str,
            )

# Interest over time chart
st.subheader("Interest Over Time")

line_chart = (
    alt.Chart(filtered)
    .mark_line(point=True)
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", values=filtered["date"].drop_duplicates().sort_values().tolist())),
        y=alt.Y("interest:Q", title="Interest (0-100)", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("keyword:N", title="Keyword"),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("keyword:N", title="Keyword"),
            alt.Tooltip("interest:Q", title="Interest"),
            alt.Tooltip("interest_7d_avg:Q", title="7-day Avg", format=".1f"),
        ],
    )
    .properties(height=400)
)

st.altair_chart(line_chart, use_container_width=True)

# 7-day rolling average chart
st.subheader("7-Day Rolling Average")

avg_chart = (
    alt.Chart(filtered)
    .mark_line()
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", values=filtered["date"].drop_duplicates().sort_values().tolist())),
        y=alt.Y("interest_7d_avg:Q", title="7-Day Avg Interest"),
        color=alt.Color("keyword:N", title="Keyword"),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("keyword:N", title="Keyword"),
            alt.Tooltip("interest_7d_avg:Q", title="7-day Avg", format=".1f"),
            alt.Tooltip("interest_30d_avg:Q", title="30-day Avg", format=".1f"),
        ],
    )
    .properties(height=300)
)

st.altair_chart(avg_chart, use_container_width=True)

# Week-over-week changes
st.subheader("Week-over-Week Changes")

# Get most recent week's changes
recent_changes = filtered[filtered["recency_rank"] <= 7].copy()
if not recent_changes.empty:
    wow_data = (
        recent_changes.groupby("keyword")["interest_wow_change"]
        .mean()
        .reset_index()
        .dropna()
    )
    wow_data.columns = ["Keyword", "Avg WoW Change"]
    wow_data = wow_data.sort_values("Avg WoW Change", ascending=True)

    # Add color category
    wow_data["change_direction"] = wow_data["Avg WoW Change"].apply(
        lambda x: "Increasing" if x > 0 else "Decreasing"
    )

    wow_chart = (
        alt.Chart(wow_data)
        .mark_bar()
        .encode(
            x=alt.X("Avg WoW Change:Q", title="Average Week-over-Week Change"),
            y=alt.Y("Keyword:N", sort="-x", title=None),
            color=alt.Color(
                "change_direction:N",
                scale=alt.Scale(
                    domain=["Increasing", "Decreasing"],
                    range=["#22c55e", "#ef4444"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Keyword:N"),
                alt.Tooltip("Avg WoW Change:Q", format="+.1f"),
            ],
        )
        .properties(height=max(150, len(wow_data) * 30))
    )

    st.altair_chart(wow_chart, use_container_width=True)

# Peak detection
st.subheader("Recent Peaks")

peaks = filtered[filtered["is_local_peak"] == True].copy()
if not peaks.empty:
    peaks = peaks.sort_values("date", ascending=False).head(20)
    peaks_display = peaks[["date", "keyword", "interest", "interest_7d_avg"]].copy()
    peaks_display.columns = ["Date", "Keyword", "Interest", "7-Day Avg"]

    st.dataframe(
        peaks_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date", width="small"),
            "Keyword": st.column_config.TextColumn("Keyword", width="medium"),
            "Interest": st.column_config.NumberColumn("Interest", width="small"),
            "7-Day Avg": st.column_config.NumberColumn(
                "7-Day Avg", format="%.1f", width="small"
            ),
        },
    )
else:
    st.info("No peaks detected in the selected date range")

# Data table
st.subheader("Raw Data")

display_df = filtered.copy()
display_df = display_df.sort_values(["date", "keyword"], ascending=[False, True])

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "date": st.column_config.DateColumn("Date", width="small"),
        "keyword": st.column_config.TextColumn("Keyword", width="medium"),
        "interest": st.column_config.NumberColumn("Interest", width="small"),
        "interest_7d_avg": st.column_config.NumberColumn(
            "7d Avg", format="%.1f", width="small"
        ),
        "interest_30d_avg": st.column_config.NumberColumn(
            "30d Avg", format="%.1f", width="small"
        ),
        "interest_wow_change": st.column_config.NumberColumn(
            "WoW", format="%+.0f", width="small"
        ),
        "is_local_peak": st.column_config.CheckboxColumn("Peak", width="small"),
    },
    column_order=[
        "date",
        "keyword",
        "interest",
        "interest_7d_avg",
        "interest_30d_avg",
        "interest_wow_change",
        "is_local_peak",
    ],
)

"""
Hacker News Trends dashboard.
"""

from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st

from data import load_hn_weekly_stats, load_hn_domain_stats, load_hn_keyword_trends

st.set_page_config(page_title="Hacker News Trends", layout="wide")
st.title("Hacker News Trends")

# Load data
weekly_stats = load_hn_weekly_stats()
domain_stats = load_hn_domain_stats()
keyword_trends = load_hn_keyword_trends()

# Convert week columns to datetime for Altair compatibility
weekly_stats["week"] = pd.to_datetime(weekly_stats["week"])
domain_stats["week"] = pd.to_datetime(domain_stats["week"])
keyword_trends["week"] = pd.to_datetime(keyword_trends["week"])

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_week = weekly_stats["week"].min()
max_week = weekly_stats["week"].max()
# Default to last 2 years
default_start = max(min_week, max_week - timedelta(days=730))
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_week),
    min_value=min_week,
    max_value=max_week,
)

# Apply date filter to all dataframes
if len(date_range) == 2:
    start_date, end_date = date_range
    weekly_filtered = weekly_stats[
        (weekly_stats["week"] >= pd.Timestamp(start_date))
        & (weekly_stats["week"] <= pd.Timestamp(end_date))
    ]
    domain_filtered = domain_stats[
        (domain_stats["week"] >= pd.Timestamp(start_date))
        & (domain_stats["week"] <= pd.Timestamp(end_date))
    ]
    keyword_filtered = keyword_trends[
        (keyword_trends["week"] >= pd.Timestamp(start_date))
        & (keyword_trends["week"] <= pd.Timestamp(end_date))
    ]
else:
    weekly_filtered = weekly_stats
    domain_filtered = domain_stats
    keyword_filtered = keyword_trends

# Metrics row
st.subheader("Summary")
col1, col2, col3, col4 = st.columns(4)

total_stories = weekly_filtered["story_count"].sum()
avg_weekly_posts = weekly_filtered["story_count"].mean()
avg_score = weekly_filtered["avg_score"].mean()
total_authors = weekly_filtered["unique_authors"].sum()

col1.metric("Total Stories", f"{total_stories:,}")
col2.metric("Avg Weekly Posts", f"{avg_weekly_posts:,.0f}")
col3.metric("Avg Score", f"{avg_score:.1f}")
col4.metric("Unique Authors", f"{total_authors:,}")

# Weekly Activity chart
st.subheader("Weekly Activity")

activity_chart = alt.Chart(weekly_filtered).mark_line(point=True).encode(
    x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=weekly_filtered["week"].tolist())),
    y=alt.Y("story_count:Q", title="Stories"),
    tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "story_count:Q", "avg_score:Q", "unique_authors:Q"],
).properties(height=300)

st.altair_chart(activity_chart, use_container_width=True)

# Domain Trends section
st.subheader("Domain Trends")

# Get top domains by total story count in filtered period
top_domains = (
    domain_filtered.groupby("domain")["story_count"]
    .sum()
    .sort_values(ascending=False)
    .head(50)
    .index.tolist()
)

# Default selection - top 5 domains
default_domains = top_domains[:5] if len(top_domains) >= 5 else top_domains

selected_domains = st.multiselect(
    "Select domains to compare",
    options=top_domains,
    default=default_domains,
    max_selections=10,
)

if selected_domains:
    domain_chart_data = domain_filtered[domain_filtered["domain"].isin(selected_domains)]

    domain_chart = alt.Chart(domain_chart_data).mark_line(point=True).encode(
        x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=domain_chart_data["week"].drop_duplicates().sort_values().tolist())),
        y=alt.Y("story_count:Q", title="Stories"),
        color=alt.Color("domain:N", legend=alt.Legend(title="Domain")),
        tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "domain:N", "story_count:Q", "avg_score:Q"],
    ).properties(height=350)

    st.altair_chart(domain_chart, use_container_width=True)
else:
    st.info("Select at least one domain to see trends.")

# Keyword Trends section
st.subheader("Technology Keyword Trends")

# Get all keywords
all_keywords = keyword_filtered["keyword"].unique().tolist()

# Default selection - some popular tech keywords
default_keywords = [k for k in ["AI", "Python", "Rust", "React", "LLM"] if k in all_keywords]

selected_keywords = st.multiselect(
    "Select keywords to compare",
    options=sorted(all_keywords),
    default=default_keywords,
    max_selections=10,
)

if selected_keywords:
    keyword_chart_data = keyword_filtered[keyword_filtered["keyword"].isin(selected_keywords)]

    keyword_chart = alt.Chart(keyword_chart_data).mark_line(point=True).encode(
        x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=keyword_chart_data["week"].drop_duplicates().sort_values().tolist())),
        y=alt.Y("mention_count:Q", title="Mentions"),
        color=alt.Color("keyword:N", legend=alt.Legend(title="Keyword")),
        tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "keyword:N", "mention_count:Q", "avg_score:Q"],
    ).properties(height=350)

    st.altair_chart(keyword_chart, use_container_width=True)
else:
    st.info("Select at least one keyword to see trends.")

# Top Domains table (most recent week)
st.subheader("Top Domains (Most Recent Week)")

if not domain_filtered.empty:
    latest_week = domain_filtered["week"].max()
    latest_domains = (
        domain_filtered[domain_filtered["week"] == latest_week]
        .sort_values("story_count", ascending=False)
        .head(20)
    )

    st.dataframe(
        latest_domains,
        use_container_width=True,
        hide_index=True,
        column_config={
            "week": st.column_config.DateColumn("Week", width="small"),
            "domain": st.column_config.TextColumn("Domain", width="medium"),
            "story_count": st.column_config.NumberColumn("Stories", width="small"),
            "total_score": st.column_config.NumberColumn("Total Score", format="%d", width="small"),
            "avg_score": st.column_config.NumberColumn("Avg Score", format="%.1f", width="small"),
        },
        column_order=["domain", "story_count", "total_score", "avg_score"],
    )
else:
    st.info("No domain data available for the selected period.")

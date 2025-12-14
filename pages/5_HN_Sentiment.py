"""
Hacker News Sentiment Analysis dashboard.

Visualizes sentiment trends for technology keywords based on HN comment analysis.
"""

import altair as alt
import pandas as pd
import streamlit as st

from data import load_hn_keyword_sentiment

st.title("Hacker News Sentiment Trends")

st.markdown("""
Sentiment analysis of [Hacker News](https://news.ycombinator.com/) comments to gauge community
opinion on technology topics. Comments are matched to stories containing tracked keywords,
then analyzed for positive/negative sentiment.

**About the Data:**
- **Source:** [BigQuery Public Dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=hacker_news)
  (`bigquery-public-data.hacker_news.full`)
- **Sentiment Model:** [Cloudflare Workers AI](https://developers.cloudflare.com/workers-ai/)
  using DistilBERT (`@cf/huggingface/distilbert-sst-2-int8`)
- **Updates:** Daily sync with sentiment computed during ETL

**How It Works:**
1. Stories with tracked keywords (AI, React, etc.) are identified
2. Comments on those stories are extracted
3. Each comment is scored for sentiment (positive/negative) using Cloudflare AI
4. Results are aggregated by keyword and day for trend visualization

*Note: Sentiment scores reflect the tone of discussion, not factual accuracy or quality.*
""")

df = load_hn_keyword_sentiment()

if df.empty:
    st.warning("No sentiment data available. Run the sync and dbt pipeline first.")
    st.code("make run-hacker-news")
    st.stop()

# Convert day to datetime for Altair
df["day"] = pd.to_datetime(df["day"])

# Keyword selection with pills
st.subheader("Select Keywords")

keywords = sorted(df["keyword"].unique())

# Initialize session state with defaults on first load
if "keyword_pills" not in st.session_state:
    st.session_state["keyword_pills"] = [k for k in ["AI", "OpenAI", "Google", "Nvidia", "Vibe Coding"] if k in keywords]

# Select/deselect all buttons
col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    if st.button("Select All", use_container_width=True):
        st.session_state["keyword_pills"] = keywords
        st.rerun()
with col2:
    if st.button("Clear All", use_container_width=True):
        st.session_state["keyword_pills"] = []
        st.rerun()

selected_keywords = st.pills(
    "Keywords",
    keywords,
    selection_mode="multi",
    label_visibility="collapsed",
    key="keyword_pills",
)

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date = df["day"].min().date()
max_date = df["day"].max().date()

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Handle single date selection
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

min_comments = st.sidebar.slider(
    "Min comments per day",
    min_value=1,
    max_value=50,
    value=3,
)

# Apply filters
date_mask = (df["day"].dt.date >= start_date) & (df["day"].dt.date <= end_date)
if selected_keywords:
    filtered = df[
        (df["keyword"].isin(selected_keywords)) &
        (df["comment_count"] >= min_comments) &
        date_mask
    ]
else:
    filtered = df[(df["comment_count"] >= min_comments) & date_mask]

if filtered.empty:
    st.warning("No data matches the current filters. Try adjusting the keyword selection or minimum comment threshold.")
    st.stop()

# Metrics for most recent day
st.subheader("Latest Day")
latest_day = filtered["day"].max()
latest = filtered[filtered["day"] == latest_day].sort_values("comment_count", ascending=False)

if not latest.empty:
    cols = st.columns(min(len(latest), 5))
    for i, (_, row) in enumerate(latest.iterrows()):
        if i >= len(cols):
            break
        delta = row["sentiment_dod_change"]
        delta_str = f"{delta:+.2f}" if pd.notna(delta) else None
        cols[i].metric(
            row["keyword"],
            f"{row['avg_sentiment']:.2f}",
            delta=delta_str,
            help=f"{int(row['comment_count'])} comments, {row['positive_pct']:.0f}% positive",
        )

# Sentiment over time chart
st.subheader("Sentiment Over Time")

if selected_keywords:
    sentiment_chart = (
        alt.Chart(filtered)
        .mark_line(point=True)
        .encode(
            x=alt.X("day:T", title="Day", axis=alt.Axis(format="%b %d")),
            y=alt.Y("avg_sentiment:Q", title="Avg Sentiment", scale=alt.Scale(domain=[-1, 1])),
            color=alt.Color("keyword:N", title="Keyword"),
            tooltip=[
                alt.Tooltip("day:T", title="Day", format="%b %d, %Y"),
                alt.Tooltip("keyword:N", title="Keyword"),
                alt.Tooltip("avg_sentiment:Q", title="Avg Sentiment", format=".2f"),
                alt.Tooltip("comment_count:Q", title="Comments"),
                alt.Tooltip("positive_pct:Q", title="Positive %", format=".1f"),
            ],
        )
        .properties(height=400)
    )

    # Add zero line for reference
    zero_line = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
        strokeDash=[5, 5], color="gray"
    ).encode(y="y:Q")

    st.altair_chart(sentiment_chart + zero_line, use_container_width=True)
else:
    st.info("Select at least one keyword to see sentiment trends.")

# Sentiment distribution over time (stacked area charts)
st.subheader("Sentiment Distribution")

if selected_keywords:
    # Prepare data for stacked chart
    dist_data = filtered.melt(
        id_vars=["day", "keyword"],
        value_vars=["positive_pct", "neutral_pct", "negative_pct"],
        var_name="sentiment_type",
        value_name="percentage",
    )
    dist_data["sentiment_type"] = dist_data["sentiment_type"].map({
        "positive_pct": "Positive",
        "neutral_pct": "Neutral",
        "negative_pct": "Negative",
    })

    # One chart per keyword
    for keyword in selected_keywords:
        kw_data = dist_data[dist_data["keyword"] == keyword]
        if kw_data.empty:
            continue

        st.write(f"**{keyword}**")

        area_chart = (
            alt.Chart(kw_data)
            .mark_area()
            .encode(
                x=alt.X("day:T", title="Day", axis=alt.Axis(format="%b %d")),
                y=alt.Y("percentage:Q", title="Percentage", stack="normalize"),
                color=alt.Color(
                    "sentiment_type:N",
                    scale=alt.Scale(
                        domain=["Positive", "Neutral", "Negative"],
                        range=["#22c55e", "#94a3b8", "#ef4444"],
                    ),
                    title="Sentiment",
                ),
                tooltip=[
                    alt.Tooltip("day:T", title="Day", format="%b %d, %Y"),
                    alt.Tooltip("sentiment_type:N", title="Type"),
                    alt.Tooltip("percentage:Q", title="Percentage", format=".1f"),
                ],
            )
            .properties(height=200)
        )

        st.altair_chart(area_chart, use_container_width=True)
else:
    st.info("Select keywords above to see sentiment distribution charts.")

# Data table
st.subheader("Raw Data")

display_df = filtered.sort_values(["day", "keyword"], ascending=[False, True]).copy()
# Format for display
display_df["day"] = display_df["day"].dt.strftime("%Y-%m-%d")

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "day": st.column_config.TextColumn("Day", width="small"),
        "keyword": st.column_config.TextColumn("Keyword", width="small"),
        "comment_count": st.column_config.NumberColumn("Comments", format="%d", width="small"),
        "story_count": st.column_config.NumberColumn("Stories", format="%d", width="small"),
        "avg_sentiment": st.column_config.NumberColumn("Avg Sentiment", format="%.3f", width="small"),
        "positive_pct": st.column_config.NumberColumn("Positive %", format="%.1f", width="small"),
        "negative_pct": st.column_config.NumberColumn("Negative %", format="%.1f", width="small"),
        "neutral_pct": st.column_config.NumberColumn("Neutral %", format="%.1f", width="small"),
        "sentiment_dod_change": st.column_config.NumberColumn("DoD Change", format="%.3f", width="small"),
    },
    column_order=[
        "day", "keyword", "comment_count", "story_count",
        "avg_sentiment", "positive_pct", "negative_pct", "neutral_pct",
        "sentiment_dod_change"
    ],
)

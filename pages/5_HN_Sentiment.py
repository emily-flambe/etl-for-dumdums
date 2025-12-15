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
opinion on technology topics.

**Methodology:**
1. Each day, the **top 100 stories by comment count** are selected from the HN public dataset
2. **Top-level comments** (direct replies to stories) are extracted from these stories
3. Each comment is analyzed for sentiment using Cloudflare Workers AI (DistilBERT)
4. Comments are matched to keywords if the **story title** contains that keyword (case-insensitive)
5. Results are aggregated by keyword and week

**Data Source:** [BigQuery Public Dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=hacker_news)
(`bigquery-public-data.hacker_news.full`)

**Limitations:**
- Only top-level comments are analyzed (not nested replies)
- Keywords are matched against story titles only, not comment text
- A story may match multiple keywords, so comments can appear in multiple categories

*Sentiment scores reflect tone of discussion (-1 = negative, +1 = positive), not factual accuracy.*
""")

df_daily = load_hn_keyword_sentiment()

if df_daily.empty:
    st.warning("No sentiment data available. Run the sync and dbt pipeline first.")
    st.code("make run-hacker-news")
    st.stop()

# Convert day to datetime
df_daily["day"] = pd.to_datetime(df_daily["day"])

# Aggregate by week for smoother trends with more data
df_daily["week"] = df_daily["day"].dt.to_period("W").dt.start_time

df = (
    df_daily.groupby(["week", "keyword"])
    .agg({
        "comment_count": "sum",
        "story_count": "sum",
        # Weighted averages by comment count
        "avg_sentiment": lambda x: (x * df_daily.loc[x.index, "comment_count"]).sum() / df_daily.loc[x.index, "comment_count"].sum(),
        "positive_pct": lambda x: (x * df_daily.loc[x.index, "comment_count"]).sum() / df_daily.loc[x.index, "comment_count"].sum(),
        "negative_pct": lambda x: (x * df_daily.loc[x.index, "comment_count"]).sum() / df_daily.loc[x.index, "comment_count"].sum(),
        "neutral_pct": lambda x: (x * df_daily.loc[x.index, "comment_count"]).sum() / df_daily.loc[x.index, "comment_count"].sum(),
    })
    .reset_index()
)

# Calculate week-over-week change
df = df.sort_values(["keyword", "week"])
df["sentiment_wow_change"] = df.groupby("keyword")["avg_sentiment"].diff()
df = df.sort_values(["week", "comment_count"], ascending=[False, False])

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

# Filter options
min_date = df["week"].min().date()
max_date = df["week"].max().date()

# Filters section
with st.expander("Filters", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    with col2:
        min_comments = st.slider(
            "Min comments per week",
            min_value=1,
            max_value=100,
            value=10,
        )

# Handle single date selection
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

# Apply filters
date_mask = (df["week"].dt.date >= start_date) & (df["week"].dt.date <= end_date)
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

# Metrics for most recent week
st.subheader("Latest Week")
latest_week = filtered["week"].max()
latest = filtered[filtered["week"] == latest_week].sort_values("comment_count", ascending=False)

if not latest.empty:
    cols = st.columns(min(len(latest), 5))
    for i, (_, row) in enumerate(latest.iterrows()):
        if i >= len(cols):
            break
        delta = row["sentiment_wow_change"]
        delta_str = f"{delta:+.2f}" if pd.notna(delta) else None
        cols[i].metric(
            row["keyword"],
            f"{row['avg_sentiment']:.2f}",
            delta=delta_str,
            help=f"{int(row['comment_count'])} comments, {row['positive_pct']:.0f}% positive",
        )

# Sentiment over time chart
st.subheader("Weekly Sentiment Trends")
st.caption("Average sentiment score per week (-1 = negative, +1 = positive)")

if selected_keywords:
    # Get actual week values for axis ticks
    week_values = sorted(filtered["week"].unique())

    sentiment_chart = (
        alt.Chart(filtered)
        .mark_line(point=alt.OverlayMarkDef(size=60, filled=True))
        .encode(
            x=alt.X("week:T", title="Week Starting",
                   scale=alt.Scale(domain=[min(week_values), max(week_values)]),
                   axis=alt.Axis(format="%b %d", values=week_values)),
            y=alt.Y("avg_sentiment:Q", title="Avg Sentiment", scale=alt.Scale(domain=[-1, 1])),
            color=alt.Color("keyword:N", title="Keyword"),
            tooltip=[
                alt.Tooltip("week:T", title="Week of", format="%b %d, %Y"),
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
st.subheader("Weekly Sentiment Distribution")
st.caption("Percentage of comments by sentiment category, aggregated by week")

if selected_keywords:
    # Prepare data for stacked chart
    dist_data = filtered.melt(
        id_vars=["week", "keyword"],
        value_vars=["positive_pct", "neutral_pct", "negative_pct"],
        var_name="sentiment_type",
        value_name="percentage",
    )
    dist_data["sentiment_type"] = dist_data["sentiment_type"].map({
        "positive_pct": "Positive",
        "neutral_pct": "Neutral",
        "negative_pct": "Negative",
    })

    # Get consistent week values across all keywords for axis ticks
    all_week_values = sorted(dist_data["week"].unique())

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
                x=alt.X("week:T", title="Week Starting",
                       scale=alt.Scale(domain=[min(all_week_values), max(all_week_values)]),
                       axis=alt.Axis(format="%b %d", values=all_week_values)),
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
                    alt.Tooltip("week:T", title="Week of", format="%b %d, %Y"),
                    alt.Tooltip("sentiment_type:N", title="Type"),
                    alt.Tooltip("percentage:Q", title="Percentage", format=".1f"),
                ],
            )
            .properties(height=200)
        )

        # Add vertical tick marks to show data points
        tick_marks = (
            alt.Chart(kw_data[kw_data["sentiment_type"] == "Positive"])
            .mark_rule(color="white", opacity=0.4, strokeWidth=1)
            .encode(x="week:T")
        )

        st.altair_chart(area_chart + tick_marks, use_container_width=True)
else:
    st.info("Select keywords above to see sentiment distribution charts.")

# Data table
st.subheader("Weekly Data")

display_df = filtered.sort_values(["week", "keyword"], ascending=[False, True]).copy()
# Format for display
display_df["week"] = display_df["week"].dt.strftime("%Y-%m-%d")

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "week": st.column_config.TextColumn("Week", width="small"),
        "keyword": st.column_config.TextColumn("Keyword", width="small"),
        "comment_count": st.column_config.NumberColumn("Comments", format="%d", width="small"),
        "story_count": st.column_config.NumberColumn("Stories", format="%d", width="small"),
        "avg_sentiment": st.column_config.NumberColumn("Avg Sentiment", format="%.3f", width="small"),
        "positive_pct": st.column_config.NumberColumn("Positive %", format="%.1f", width="small"),
        "negative_pct": st.column_config.NumberColumn("Negative %", format="%.1f", width="small"),
        "neutral_pct": st.column_config.NumberColumn("Neutral %", format="%.1f", width="small"),
        "sentiment_wow_change": st.column_config.NumberColumn("WoW Change", format="%.3f", width="small"),
    },
    column_order=[
        "week", "keyword", "comment_count", "story_count",
        "avg_sentiment", "positive_pct", "negative_pct", "neutral_pct",
        "sentiment_wow_change"
    ],
)

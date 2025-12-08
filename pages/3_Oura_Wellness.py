"""
Oura Wellness dashboard.
"""

from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from app_data import load_oura_daily

st.set_page_config(page_title="Oura Wellness", layout="wide")
st.title("Oura Wellness")

# Load data
df = load_oura_daily()

# Convert date column to proper datetime for Altair compatibility
df["day"] = pd.to_datetime(df["day"])

# Convert nullable Int64 columns to float for Altair compatibility
int_cols = ["sleep_score", "readiness_score", "activity_score", "steps",
            "active_calories", "total_calories", "walking_distance_meters"]
for col in int_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date = df["day"].min()
max_date = df["day"].max()
# Default to last 30 days
default_start = max(min_date, max_date - timedelta(days=30))
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Category filters
sleep_categories = ["excellent", "good", "fair", "poor"]
selected_sleep_cats = st.sidebar.pills(
    "Sleep Category", sleep_categories, selection_mode="multi"
)

readiness_categories = ["optimal", "good", "fair", "poor"]
selected_readiness_cats = st.sidebar.pills(
    "Readiness Category", readiness_categories, selection_mode="multi"
)

# Apply filters
filtered = df.copy()
if len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["day"] >= pd.Timestamp(start_date))
        & (filtered["day"] <= pd.Timestamp(end_date))
    ]
if selected_sleep_cats:
    filtered = filtered[filtered["sleep_category"].isin(selected_sleep_cats)]
if selected_readiness_cats:
    filtered = filtered[filtered["readiness_category"].isin(selected_readiness_cats)]

# Metrics row
st.subheader("Averages")
col1, col2, col3, col4, col5 = st.columns(5)

avg_sleep = filtered["sleep_score"].mean()
avg_readiness = filtered["readiness_score"].mean()
avg_activity = filtered["activity_score"].mean()
avg_wellness = filtered["combined_wellness_score"].mean()
avg_steps = filtered["steps"].mean()

col1.metric("Sleep Score", f"{avg_sleep:.0f}" if pd.notna(avg_sleep) else "N/A")
col2.metric("Readiness Score", f"{avg_readiness:.0f}" if pd.notna(avg_readiness) else "N/A")
col3.metric("Activity Score", f"{avg_activity:.0f}" if pd.notna(avg_activity) else "N/A")
col4.metric("Wellness Score", f"{avg_wellness:.0f}" if pd.notna(avg_wellness) else "N/A")
col5.metric("Avg Steps", f"{avg_steps:,.0f}" if pd.notna(avg_steps) else "N/A")

# Scores over time chart
st.subheader("Scores Over Time")

# Prepare data for multi-line chart
chart_df = filtered[["day", "sleep_score", "readiness_score", "activity_score"]].copy()
chart_df = chart_df.melt(id_vars=["day"], var_name="Metric", value_name="Score")
chart_df["Metric"] = chart_df["Metric"].map({
    "sleep_score": "Sleep",
    "readiness_score": "Readiness",
    "activity_score": "Activity",
})

line_chart = alt.Chart(chart_df).mark_line(point=True).encode(
    x=alt.X("day:T", title="Date"),
    y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 100])),
    color=alt.Color("Metric:N", scale=alt.Scale(
        domain=["Sleep", "Readiness", "Activity"],
        range=["#6366f1", "#22c55e", "#f59e0b"]
    )),
    tooltip=["day:T", "Metric:N", "Score:Q"],
).properties(height=300)

st.altair_chart(line_chart, use_container_width=True)

# Steps over time
st.subheader("Daily Steps")

# Add color category for steps
filtered["steps_color"] = filtered["steps"].apply(
    lambda x: "10k+" if x >= 10000 else ("7.5k+" if x >= 7500 else "<7.5k")
)

steps_chart = alt.Chart(filtered).mark_bar().encode(
    x=alt.X("day:T", title="Date"),
    y=alt.Y("steps:Q", title="Steps"),
    color=alt.Color("steps_color:N", scale=alt.Scale(
        domain=["10k+", "7.5k+", "<7.5k"],
        range=["#22c55e", "#f59e0b", "#ef4444"]
    ), legend=alt.Legend(title="Steps")),
    tooltip=["day:T", "steps:Q", "activity_category:N"],
).properties(height=250)

# Add 10k goal line
goal_line = alt.Chart(pd.DataFrame({"y": [10000]})).mark_rule(
    strokeDash=[5, 5], color="gray"
).encode(y="y:Q")

st.altair_chart(steps_chart + goal_line, use_container_width=True)

# Distribution charts
st.subheader("Score Distributions")
col1, col2, col3 = st.columns(3)

with col1:
    st.write("**Sleep Categories**")
    sleep_dist = filtered["sleep_category"].value_counts().reset_index()
    sleep_dist.columns = ["Category", "Count"]
    sleep_chart = alt.Chart(sleep_dist).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["excellent", "good", "fair", "poor"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        )),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=200)
    st.altair_chart(sleep_chart, use_container_width=True)

with col2:
    st.write("**Readiness Categories**")
    readiness_dist = filtered["readiness_category"].value_counts().reset_index()
    readiness_dist.columns = ["Category", "Count"]
    readiness_chart = alt.Chart(readiness_dist).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["optimal", "good", "fair", "poor"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        )),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=200)
    st.altair_chart(readiness_chart, use_container_width=True)

with col3:
    st.write("**Activity Categories**")
    activity_dist = filtered["activity_category"].value_counts().reset_index()
    activity_dist.columns = ["Category", "Count"]
    activity_chart = alt.Chart(activity_dist).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["very_active", "active", "moderate", "sedentary"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        )),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=200)
    st.altair_chart(activity_chart, use_container_width=True)

# Data table
st.subheader("Daily Data")

display_df = filtered.copy()
display_df = display_df.sort_values("day", ascending=False)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "day": st.column_config.DateColumn("Date", width="small"),
        "sleep_score": st.column_config.NumberColumn("Sleep", width="small"),
        "readiness_score": st.column_config.NumberColumn("Readiness", width="small"),
        "activity_score": st.column_config.NumberColumn("Activity", width="small"),
        "combined_wellness_score": st.column_config.NumberColumn("Wellness", width="small"),
        "steps": st.column_config.NumberColumn("Steps", format="%d", width="small"),
        "sleep_category": st.column_config.TextColumn("Sleep Cat", width="small"),
        "readiness_category": st.column_config.TextColumn("Readiness Cat", width="small"),
        "activity_category": st.column_config.TextColumn("Activity Cat", width="small"),
        "walking_distance_meters": st.column_config.NumberColumn("Walking (m)", format="%d", width="small"),
        "active_calories": st.column_config.NumberColumn("Active Cal", format="%d", width="small"),
        "total_calories": st.column_config.NumberColumn("Total Cal", format="%d", width="small"),
    },
    column_order=[
        "day",
        "combined_wellness_score",
        "sleep_score",
        "readiness_score",
        "activity_score",
        "steps",
        "sleep_category",
        "readiness_category",
        "activity_category",
        "active_calories",
        "total_calories",
        "walking_distance_meters",
    ],
)

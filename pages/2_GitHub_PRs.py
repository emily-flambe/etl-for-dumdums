"""
GitHub Pull Requests dashboard.

Shows team PR activity trends, review responsiveness, and code volume metrics.
"""

from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from data import load_pull_requests, load_reviewer_activity

st.set_page_config(page_title="GitHub PRs", layout="wide")
st.title("GitHub Pull Requests")

# Load data
try:
    prs = load_pull_requests()
    reviewer_activity = load_reviewer_activity()
except Exception as e:
    st.error(f"Could not load GitHub data: {e}")
    st.info("Make sure you have run `make sync-github` and `make dbt` to sync and transform GitHub data.")
    st.stop()

# Convert timestamps for filtering
prs["created_at"] = pd.to_datetime(prs["created_at"])
prs["merged_at"] = pd.to_datetime(prs["merged_at"])
prs["updated_at"] = pd.to_datetime(prs["updated_at"])
reviewer_activity["pr_created_at"] = pd.to_datetime(reviewer_activity["pr_created_at"])

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter (default: last 90 days)
min_date = prs["created_at"].min()
max_date = prs["created_at"].max()
default_start = max(min_date, max_date - timedelta(days=90))
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Repo filter
repos = sorted(prs["repo"].dropna().unique().tolist())
selected_repos = st.sidebar.multiselect("Repos", repos, default=repos)

# Author filter
authors = sorted(prs["author_username"].dropna().unique().tolist())
selected_authors = st.sidebar.multiselect("Authors", authors, default=authors)


# Apply filters to PRs
filtered_prs = prs.copy()
if len(date_range) == 2:
    start_date, end_date = date_range
    # Convert to timezone-aware timestamps to match BigQuery's UTC timestamps
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    filtered_prs = filtered_prs[
        (filtered_prs["created_at"] >= start_ts)
        & (filtered_prs["created_at"] <= end_ts)
    ]
# Always apply repo and author filters (empty selection = no results)
filtered_prs = filtered_prs[filtered_prs["repo"].isin(selected_repos)]
filtered_prs = filtered_prs[filtered_prs["author_username"].isin(selected_authors)]

# Apply filters to reviewer activity
filtered_activity = reviewer_activity.copy()
if len(date_range) == 2:
    filtered_activity = filtered_activity[
        (filtered_activity["pr_created_at"] >= start_ts)
        & (filtered_activity["pr_created_at"] <= end_ts)
    ]
# Always apply repo filter (empty selection = no results)
filtered_activity = filtered_activity[filtered_activity["pr_repo"].isin(selected_repos)]

# Calculate metrics
prs_opened = len(filtered_prs)
prs_merged = len(filtered_prs[filtered_prs["merged_at"].notna()])
avg_time_to_merge = filtered_prs["cycle_time_hours"].mean()
avg_time_to_first_review = filtered_prs["time_to_first_review_hours"].mean()
avg_time_to_first_response = filtered_activity["time_to_first_response_hours"].mean()

lines_added = filtered_prs["additions"].sum()
lines_deleted = filtered_prs["deletions"].sum()
net_lines = lines_added - lines_deleted
files_changed = filtered_prs["changed_files"].sum()
total_reviews = filtered_prs["review_count"].sum()

# Metrics Row 1: PR Activity
st.subheader("PR Activity")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("PRs Opened", prs_opened)
col2.metric("PRs Merged", prs_merged)
col3.metric("Avg Time to Merge", f"{avg_time_to_merge:.0f}h" if pd.notna(avg_time_to_merge) else "N/A")
col4.metric("Avg Time to First Review", f"{avg_time_to_first_review:.0f}h" if pd.notna(avg_time_to_first_review) else "N/A")
col5.metric("Avg Time to First Response", f"{avg_time_to_first_response:.0f}h" if pd.notna(avg_time_to_first_response) else "N/A")

# Metrics Row 2: Code Volume
st.subheader("Code Volume")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Lines Added", f"{lines_added:,}")
col2.metric("Lines Deleted", f"{lines_deleted:,}")
col3.metric("Net Lines", f"{net_lines:+,}")
col4.metric("Files Changed", f"{files_changed:,}")
col5.metric("Total Reviews", f"{total_reviews:,}")

# Prepare weekly aggregations
filtered_prs["week"] = filtered_prs["created_at"].dt.to_period("W").dt.start_time
filtered_prs["merged_week"] = filtered_prs["merged_at"].dt.to_period("W").dt.start_time

# Weekly PR counts
weekly_opened = filtered_prs.groupby("week").size().reset_index(name="PRs Opened")
weekly_merged = filtered_prs[filtered_prs["merged_at"].notna()].groupby("merged_week").size().reset_index(name="PRs Merged")
weekly_merged = weekly_merged.rename(columns={"merged_week": "week"})

weekly_activity = pd.merge(weekly_opened, weekly_merged, on="week", how="outer").fillna(0)
weekly_activity = weekly_activity.sort_values("week")

# All timing metrics from merged PRs only, grouped by merge week for consistency
# This ensures: Time to First Response <= Time to Approval <= Time to Merge
merged_prs_with_timing = filtered_prs[filtered_prs["merged_at"].notna()].copy()

weekly_cycle = (
    merged_prs_with_timing[merged_prs_with_timing["cycle_time_hours"].notna()]
    .groupby("merged_week")["cycle_time_hours"]
    .mean()
    .reset_index(name="Avg Cycle Time (hours)")
)
weekly_cycle = weekly_cycle.rename(columns={"merged_week": "week"})
weekly_cycle = weekly_cycle.sort_values("week")

weekly_review = (
    merged_prs_with_timing[merged_prs_with_timing["time_to_first_review_hours"].notna()]
    .groupby("merged_week")["time_to_first_review_hours"]
    .mean()
    .reset_index(name="Avg Time to Approval (hours)")
)
weekly_review = weekly_review.rename(columns={"merged_week": "week"})
weekly_review = weekly_review.sort_values("week")

# For first response, join activity data to merged PRs
filtered_activity["week"] = filtered_activity["pr_created_at"].dt.to_period("W").dt.start_time
merged_pr_ids = set(merged_prs_with_timing["pull_request_id"].astype(str).tolist())
merged_activity = filtered_activity[filtered_activity["pull_request_id"].astype(str).isin(merged_pr_ids)].copy()
# Use merged_week from the PR data
pr_merge_weeks = merged_prs_with_timing[["pull_request_id", "merged_week"]].copy()
pr_merge_weeks["pull_request_id"] = pr_merge_weeks["pull_request_id"].astype(str)
merged_activity["pull_request_id"] = merged_activity["pull_request_id"].astype(str)
merged_activity = merged_activity.merge(
    pr_merge_weeks,
    on="pull_request_id",
    how="left"
)
weekly_response = (
    merged_activity[merged_activity["time_to_first_response_hours"].notna()]
    .groupby("merged_week")["time_to_first_response_hours"]
    .mean()
    .reset_index(name="Avg Time to First Response (hours)")
)
weekly_response = weekly_response.rename(columns={"merged_week": "week"})
weekly_response = weekly_response.sort_values("week")

# Weekly code volume
weekly_code = (
    filtered_prs.groupby("week")
    .agg({"additions": "sum", "deletions": "sum"})
    .reset_index()
)
weekly_code.columns = ["week", "Lines Added", "Lines Deleted"]
weekly_code = weekly_code.sort_values("week")

# Charts
st.subheader("Trends")

# PR Activity Chart - just show PRs opened per week
st.write("**PRs Opened Per Week**")
# Format week as string for nominal x-axis (gives better control over bar positioning)
weekly_opened_display = weekly_opened.copy()
weekly_opened_display["week_label"] = weekly_opened_display["week"].dt.strftime("%b %d")
week_order = weekly_opened_display.sort_values("week")["week_label"].tolist()

activity_chart = alt.Chart(weekly_opened_display).mark_bar(color="#6366f1", size=20).encode(
    x=alt.X("week_label:N", title="Week", sort=week_order, axis=alt.Axis(labelAngle=0)),
    y=alt.Y("PRs Opened:Q", title="PRs"),
    tooltip=["week_label:N", "PRs Opened:Q"],
).properties(height=250)
st.altair_chart(activity_chart, use_container_width=True)

# Combined Response Times Chart
st.write("**Response Times (Weekly)**")

# Prepare data for combined chart
timing_data = []
if len(weekly_cycle) > 0:
    cycle_df = weekly_cycle.copy()
    cycle_df["Metric"] = "Time to Merge"
    cycle_df = cycle_df.rename(columns={"Avg Cycle Time (hours)": "Hours"})
    timing_data.append(cycle_df[["week", "Metric", "Hours"]])

if len(weekly_review) > 0:
    review_df = weekly_review.copy()
    review_df["Metric"] = "Time to Approval"
    review_df = review_df.rename(columns={"Avg Time to Approval (hours)": "Hours"})
    timing_data.append(review_df[["week", "Metric", "Hours"]])

if len(weekly_response) > 0:
    response_df = weekly_response.copy()
    response_df["Metric"] = "Time to First Comment or Approval"
    response_df = response_df.rename(columns={"Avg Time to First Response (hours)": "Hours"})
    timing_data.append(response_df[["week", "Metric", "Hours"]])

if timing_data:
    combined_timing = pd.concat(timing_data, ignore_index=True)
    # Get unique weeks for x-axis alignment
    timing_weeks = combined_timing["week"].drop_duplicates().sort_values().tolist()
    timing_chart = alt.Chart(combined_timing).mark_line(point=True).encode(
        x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=timing_weeks)),
        y=alt.Y("Hours:Q", title="Hours"),
        color=alt.Color("Metric:N", scale=alt.Scale(
            domain=["Time to Merge", "Time to Approval", "Time to First Comment or Approval"],
            range=["#f59e0b", "#22c55e", "#6366f1"]
        ), legend=alt.Legend(orient="bottom", labelLimit=0)),
        tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "Metric:N", alt.Tooltip("Hours:Q", format=".1f")],
    ).properties(height=300)
    st.altair_chart(timing_chart, use_container_width=True)
else:
    st.info("No timing data available")

# Code Volume Chart
st.write("**Code Volume Over Time (Weekly)**")
# Check if there's actual data (not all zeros/nulls)
has_code_data = len(weekly_code) > 0 and (weekly_code["Lines Added"].sum() > 0 or weekly_code["Lines Deleted"].sum() > 0)
if has_code_data:
    # Format week as string for nominal x-axis (required for xOffset to work)
    weekly_code_display = weekly_code.copy()
    weekly_code_display["week_label"] = weekly_code_display["week"].dt.strftime("%b %d")
    week_order = weekly_code_display.sort_values("week")["week_label"].tolist()

    code_chart_data = weekly_code_display.melt(id_vars=["week", "week_label"], var_name="Metric", value_name="Lines")
    code_chart = alt.Chart(code_chart_data).mark_bar().encode(
        x=alt.X("week_label:N", title="Week", sort=week_order),
        y=alt.Y("Lines:Q", title="Lines of Code"),
        color=alt.Color("Metric:N", scale=alt.Scale(
            domain=["Lines Added", "Lines Deleted"],
            range=["#22c55e", "#ef4444"]
        ), legend=alt.Legend(orient="bottom")),
        xOffset="Metric:N",
        tooltip=["week_label:N", "Metric:N", "Lines:Q"],
    ).properties(height=250)
    st.altair_chart(code_chart, use_container_width=True)
else:
    st.info("No code volume data available. Re-run `make sync-github` to fetch additions/deletions.")

# Leaderboards
st.subheader("Leaderboards")
col1, col2 = st.columns(2)

with col1:
    st.write("**PRs Reviewed by Teammate**")
    # Count unique PRs each reviewer commented on (each PR counts once)
    prs_reviewed = (
        filtered_activity.groupby("reviewer_username")["pull_request_id"]
        .nunique()
        .reset_index(name="PRs Reviewed")
        .sort_values("PRs Reviewed", ascending=False)
    )
    if len(prs_reviewed) > 0:
        prs_reviewed.index = range(1, len(prs_reviewed) + 1)
        prs_reviewed.index.name = "Rank"
        st.dataframe(
            prs_reviewed,
            use_container_width=True,
            column_config={
                "reviewer_username": st.column_config.TextColumn("Reviewer"),
                "PRs Reviewed": st.column_config.NumberColumn("PRs Reviewed"),
            },
        )
    else:
        st.info("No review activity in selected range")

with col2:
    st.write("**Avg Time to Merge by Author**")
    # Average cycle time for merged PRs by author (ascending = fastest first)
    merged_prs = filtered_prs[filtered_prs["merged_at"].notna() & filtered_prs["cycle_time_hours"].notna()]
    time_to_merge = (
        merged_prs.groupby("author_username")["cycle_time_hours"]
        .mean()
        .reset_index(name="Avg Hours to Merge")
        .sort_values("Avg Hours to Merge", ascending=True)
    )
    if len(time_to_merge) > 0:
        time_to_merge["Avg Hours to Merge"] = time_to_merge["Avg Hours to Merge"].round(1)
        time_to_merge.index = range(1, len(time_to_merge) + 1)
        time_to_merge.index.name = "Rank"
        st.dataframe(
            time_to_merge,
            use_container_width=True,
            column_config={
                "author_username": st.column_config.TextColumn("Author"),
                "Avg Hours to Merge": st.column_config.NumberColumn("Avg Hours"),
            },
        )
    else:
        st.info("No merged PRs in selected range")

# Data table (collapsed by default)
with st.expander("View PR Data"):
    display_df = filtered_prs.copy()
    display_df = display_df.sort_values("created_at", ascending=False)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "pr_number": st.column_config.NumberColumn("PR #", width="small"),
            "repo": st.column_config.TextColumn("Repo", width="medium"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "author_username": st.column_config.TextColumn("Author", width="small"),
            "state": st.column_config.TextColumn("State", width="small"),
            "pr_outcome": st.column_config.TextColumn("Outcome", width="small"),
            "created_at": st.column_config.DatetimeColumn("Created", width="medium"),
            "merged_at": st.column_config.DatetimeColumn("Merged", width="medium"),
            "cycle_time_hours": st.column_config.NumberColumn("Cycle Time (h)", width="small"),
            "additions": st.column_config.NumberColumn("Added", width="small"),
            "deletions": st.column_config.NumberColumn("Deleted", width="small"),
            "review_count": st.column_config.NumberColumn("Reviews", width="small"),
            "comment_count": st.column_config.NumberColumn("Comments", width="small"),
        },
        column_order=[
            "pr_number", "repo", "title", "author_username", "pr_outcome",
            "created_at", "merged_at", "cycle_time_hours",
            "additions", "deletions", "review_count", "comment_count"
        ],
    )

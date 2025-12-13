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

# Toggle for author breakdown
show_by_author = st.sidebar.toggle("Show by Author", value=False)

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
avg_time_to_first_comment = filtered_activity["time_to_first_comment_hours"].mean()

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
col5.metric("Avg Time to First Comment", f"{avg_time_to_first_comment:.0f}h" if pd.notna(avg_time_to_first_comment) else "N/A")

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

# Weekly cycle time
weekly_cycle = (
    filtered_prs[filtered_prs["cycle_time_hours"].notna()]
    .groupby("merged_week")["cycle_time_hours"]
    .mean()
    .reset_index(name="Avg Cycle Time (hours)")
)
weekly_cycle = weekly_cycle.rename(columns={"merged_week": "week"})
weekly_cycle = weekly_cycle.sort_values("week")

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
activity_chart = alt.Chart(weekly_opened).mark_bar(color="#6366f1").encode(
    x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=weekly_opened["week"].tolist())),
    y=alt.Y("PRs Opened:Q", title="PRs"),
    tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "PRs Opened:Q"],
).properties(height=250)
st.altair_chart(activity_chart, use_container_width=True)

# Cycle Time Chart
if len(weekly_cycle) > 0:
    st.write("**Average Time to Merge (Weekly)**")
    cycle_chart = alt.Chart(weekly_cycle).mark_line(point=True, color="#f59e0b").encode(
        x=alt.X("week:T", title="Week", axis=alt.Axis(format="%b %d", values=weekly_cycle["week"].tolist())),
        y=alt.Y("Avg Cycle Time (hours):Q", title="Hours"),
        tooltip=[alt.Tooltip("week:T", format="%b %d, %Y"), "Avg Cycle Time (hours):Q"],
    ).properties(height=200)
    st.altair_chart(cycle_chart, use_container_width=True)

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
        )),
        xOffset="Metric:N",
        tooltip=["week_label:N", "Metric:N", "Lines:Q"],
    ).properties(height=250)
    st.altair_chart(code_chart, use_container_width=True)
else:
    st.info("No code volume data available. Re-run `make sync-github` to fetch additions/deletions.")

# Breakdown section (conditional on toggle)
st.subheader("Breakdown")

if show_by_author:
    # Author breakdown charts
    col1, col2 = st.columns(2)

    with col1:
        st.write("**PRs Merged by Author**")
        author_merged = (
            filtered_prs[filtered_prs["merged_at"].notna()]
            .groupby("author_username")
            .size()
            .reset_index(name="PRs Merged")
            .sort_values("PRs Merged", ascending=False)
            .head(10)
        )
        if len(author_merged) > 0:
            author_chart = alt.Chart(author_merged).mark_bar(color="#22c55e").encode(
                x=alt.X("PRs Merged:Q"),
                y=alt.Y("author_username:N", sort="-x", title="Author"),
                tooltip=["author_username:N", "PRs Merged:Q"],
            ).properties(height=250)
            st.altair_chart(author_chart, use_container_width=True)
        else:
            st.info("No merged PRs in selected range")

    with col2:
        st.write("**Avg Time to First Review by Reviewer**")
        reviewer_response = (
            filtered_activity[filtered_activity["time_to_first_review_hours"].notna()]
            .groupby("reviewer_username")["time_to_first_review_hours"]
            .mean()
            .reset_index(name="Avg Hours")
            .sort_values("Avg Hours", ascending=True)
            .head(10)
        )
        if len(reviewer_response) > 0:
            reviewer_chart = alt.Chart(reviewer_response).mark_bar(color="#6366f1").encode(
                x=alt.X("Avg Hours:Q", title="Hours"),
                y=alt.Y("reviewer_username:N", sort="x", title="Reviewer"),
                tooltip=["reviewer_username:N", "Avg Hours:Q"],
            ).properties(height=250)
            st.altair_chart(reviewer_chart, use_container_width=True)
        else:
            st.info("No review data in selected range")

    # Second row of author charts
    col1, col2 = st.columns(2)

    with col1:
        st.write("**Avg Time to First Comment by Reviewer**")
        commenter_response = (
            filtered_activity[filtered_activity["time_to_first_comment_hours"].notna()]
            .groupby("reviewer_username")["time_to_first_comment_hours"]
            .mean()
            .reset_index(name="Avg Hours")
            .sort_values("Avg Hours", ascending=True)
            .head(10)
        )
        if len(commenter_response) > 0:
            commenter_chart = alt.Chart(commenter_response).mark_bar(color="#8b5cf6").encode(
                x=alt.X("Avg Hours:Q", title="Hours"),
                y=alt.Y("reviewer_username:N", sort="x", title="Reviewer"),
                tooltip=["reviewer_username:N", "Avg Hours:Q"],
            ).properties(height=250)
            st.altair_chart(commenter_chart, use_container_width=True)
        else:
            st.info("No comment data in selected range")

    with col2:
        st.write("**Review Activity by Reviewer**")
        review_activity_by_user = (
            filtered_activity
            .groupby("reviewer_username")
            .agg({"review_count": "sum", "comment_count": "sum"})
            .reset_index()
        )
        review_activity_by_user["total_activity"] = (
            review_activity_by_user["review_count"] + review_activity_by_user["comment_count"]
        )
        review_activity_by_user = review_activity_by_user.sort_values("total_activity", ascending=False).head(10)

        if len(review_activity_by_user) > 0:
            activity_data = review_activity_by_user.melt(
                id_vars=["reviewer_username"],
                value_vars=["review_count", "comment_count"],
                var_name="Type",
                value_name="Count"
            )
            activity_data["Type"] = activity_data["Type"].map({
                "review_count": "Reviews",
                "comment_count": "Comments"
            })
            activity_bar = alt.Chart(activity_data).mark_bar().encode(
                x=alt.X("Count:Q"),
                y=alt.Y("reviewer_username:N", sort="-x", title="Reviewer"),
                color=alt.Color("Type:N", scale=alt.Scale(
                    domain=["Reviews", "Comments"],
                    range=["#22c55e", "#6366f1"]
                )),
                tooltip=["reviewer_username:N", "Type:N", "Count:Q"],
            ).properties(height=250)
            st.altair_chart(activity_bar, use_container_width=True)
        else:
            st.info("No activity data in selected range")

else:
    # Team-level breakdown: PR outcomes bar chart
    st.write("**PR Outcomes**")
    outcome_counts = filtered_prs["pr_outcome"].value_counts().reset_index()
    outcome_counts.columns = ["Outcome", "Count"]

    outcome_chart = alt.Chart(outcome_counts).mark_bar().encode(
        x=alt.X("Count:Q", title="PRs"),
        y=alt.Y("Outcome:N", sort="-x", title=None),
        color=alt.Color("Outcome:N", scale=alt.Scale(
            domain=["merged", "open", "closed_without_merge"],
            range=["#22c55e", "#6366f1", "#ef4444"]
        ), legend=None),
        tooltip=["Outcome:N", "Count:Q"],
    ).properties(height=150)
    st.altair_chart(outcome_chart, use_container_width=True)

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

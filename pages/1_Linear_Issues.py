"""
Linear Issues dashboard.
"""

from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from app_data import load_issues

st.set_page_config(page_title="Linear Issues", layout="wide")
st.title("Linear Issues")

# Load data
df = load_issues()

# Sidebar filters
st.sidebar.header("Filters")

states = sorted(df["state"].dropna().unique().tolist())
selected_states = st.sidebar.pills("State", states, selection_mode="multi")

assignees = ["Unassigned"] + sorted(df["assignee_name"].dropna().unique().tolist())
selected_assignees = st.sidebar.pills("Assignee", assignees, selection_mode="multi", default=assignees)

# Sort cycles by start date (oldest first)
cycle_df = df[["cycle_name", "cycle_starts_at"]].dropna().drop_duplicates()
cycle_df = cycle_df.sort_values("cycle_starts_at", ascending=True)
cycles = cycle_df["cycle_name"].tolist()
# Default to cycles that have already started
started_cycles = cycle_df[cycle_df["cycle_starts_at"].dt.date <= date.today()]["cycle_name"].tolist()
selected_cycles = st.sidebar.pills("Cycle", cycles, selection_mode="multi", default=started_cycles)

projects = ["All"] + sorted(df["project_name"].dropna().unique().tolist())
selected_project = st.sidebar.selectbox("Project", projects)

# Issue type filter (parent/child/standalone)
issue_types = ["Parent", "Child", "Standalone"]
selected_issue_types = st.sidebar.pills("Issue Type", issue_types, selection_mode="multi")

# Date range filter
st.sidebar.subheader("Date Range")
min_date = df["created_at"].min().date()
max_date = df["created_at"].max().date()
date_range = st.sidebar.date_input(
    "Created between",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Apply filters
filtered = df.copy()
if selected_states:
    filtered = filtered[filtered["state"].isin(selected_states)]
else:
    pass  # Show all when nothing selected
if selected_assignees:
    # Handle "Unassigned" specially (null assignee_name)
    include_unassigned = "Unassigned" in selected_assignees
    named_assignees = [a for a in selected_assignees if a != "Unassigned"]
    if include_unassigned:
        filtered = filtered[
            (filtered["assignee_name"].isin(named_assignees)) | (filtered["assignee_name"].isna())
        ]
    else:
        filtered = filtered[filtered["assignee_name"].isin(named_assignees)]
if selected_cycles:
    filtered = filtered[filtered["cycle_name"].isin(selected_cycles)]
if selected_project != "All":
    filtered = filtered[filtered["project_name"] == selected_project]
if selected_issue_types:
    # Build mask for selected issue types
    type_mask = pd.Series([False] * len(filtered), index=filtered.index)
    if "Parent" in selected_issue_types:
        type_mask |= filtered["is_parent"] == True
    if "Child" in selected_issue_types:
        type_mask |= filtered["is_child"] == True
    if "Standalone" in selected_issue_types:
        type_mask |= (filtered["is_parent"] == False) & (filtered["is_child"] == False)
    filtered = filtered[type_mask]
if len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["created_at"].dt.date >= start_date)
        & (filtered["created_at"].dt.date <= end_date)
    ]

# Metrics row
st.subheader("Overview")
col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
col1.metric("Total", len(filtered))
col2.metric("Total Points", f"{filtered['estimate'].sum():.0f}")
col3.metric("Parents", len(filtered[filtered["is_parent"] == True]))
col4.metric("Children", len(filtered[filtered["is_child"] == True]))
col5.metric("Backlog", len(filtered[filtered["state"] == "Backlog"]))
col6.metric("In Progress", len(filtered[filtered["state"] == "In Progress"]))
col7.metric("Done", len(filtered[filtered["state"] == "Done"]))
col8.metric("Avg Days Open", f"{filtered['days_since_created'].mean():.0f}")

# Charts row
st.subheader("Charts")

st.write("**Points by Assignee**")
assignee_points = (
    filtered.groupby(filtered["assignee_name"].fillna("Unassigned"))["estimate"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)
assignee_points.columns = ["Assignee", "Points"]

chart = alt.Chart(assignee_points).mark_bar().encode(
    x=alt.X("Points:Q"),
    y=alt.Y("Assignee:N", sort="-x"),
)
st.altair_chart(chart, use_container_width=True)

# Second charts row
with st.container():
    st.write("**Points Completed by Cycle**")
    done_states = ["Done", "Done Pending Deployment"]
    completed = filtered[filtered["state"].isin(done_states)].copy()
    # Group by cycle and sum points, keeping cycle_starts_at for sorting
    cycle_completed = (
        completed.groupby(["cycle_name", "cycle_starts_at"])["estimate"]
        .sum()
        .reset_index(name="Points")
    )
    cycle_completed = cycle_completed.dropna(subset=["cycle_name"])
    cycle_completed = cycle_completed.sort_values("cycle_starts_at")
    st.bar_chart(cycle_completed, x="cycle_name", y="Points", x_label="Cycle", y_label="Points")

# Data table
st.subheader("Issues")

# Add URL column and issue type for display
display_df = filtered.copy()
display_df["url"] = "https://linear.app/ddx/issue/" + display_df["identifier"]

# Sort by project, state, assignee, then points (ascending)
display_df = display_df.sort_values(
    by=["project_name", "state", "assignee_name", "estimate"],
    ascending=[True, True, True, True],
    na_position="last",
)


# Create issue type column
def get_issue_type(row):
    if row["is_parent"]:
        return f"Parent ({row['child_count']} children)"
    elif row["is_child"]:
        return f"Child of {row['parent_identifier']}"
    return "Standalone"


display_df["type"] = display_df.apply(get_issue_type, axis=1)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "identifier": st.column_config.TextColumn("ID", width="small"),
        "url": st.column_config.LinkColumn("Link", display_text="Open", width="small"),
        "type": st.column_config.TextColumn("Type", width="medium"),
        "title": st.column_config.TextColumn("Title", width="large"),
        "state": st.column_config.TextColumn("State", width="small"),
        "estimate": st.column_config.NumberColumn("Points", width="small"),
        "assignee_name": st.column_config.TextColumn("Assignee", width="medium"),
        "cycle_name": st.column_config.TextColumn("Cycle", width="medium"),
        "project_name": st.column_config.TextColumn("Project", width="medium"),
        "labels": st.column_config.ListColumn("Labels", width="medium"),
        "days_since_created": st.column_config.NumberColumn("Days Open", width="small"),
    },
    column_order=["identifier", "url", "project_name", "title", "state", "estimate", "assignee_name", "labels", "cycle_name", "type", "days_since_created"],
)

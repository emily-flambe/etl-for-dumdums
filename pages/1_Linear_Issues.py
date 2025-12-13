"""
Linear Issues dashboard.
"""

from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from data import load_issues

st.set_page_config(page_title="Linear Issues", layout="wide")
st.title("Linear Issues")

# Load data
df = load_issues()

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter (at top)
st.sidebar.subheader("Date Range")
min_date = df["created_at"].min().date()
max_date = df["created_at"].max().date()
date_range = st.sidebar.date_input(
    "Created between",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

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

# SDLC Label Breakdown by Cycle
st.subheader("How do Exchange Engineers Spend Their Time?")

# Extract SDLC labels from issues
sdlc_labels = ["SDLC:Drudgery", "SDLC:InternalSupport", "SDLC:QualityDebt", "SDLC:NewStuff"]
sdlc_colors = {
    "SDLC:Drudgery": "#9ca3af",
    "SDLC:InternalSupport": "#14b8a6",
    "SDLC:QualityDebt": "#f97316",
    "SDLC:NewStuff": "#6366f1",
}

# Filter to completed issues, explode labels, and filter for SDLC labels
done_states = ["Done", "Done Pending Deployment"]
sdlc_data = filtered[
    filtered["state"].isin(done_states) & filtered["cycle_name"].notna() & filtered["estimate"].notna()
].copy()
sdlc_data = sdlc_data.explode("labels")
sdlc_data = sdlc_data[sdlc_data["labels"].isin(sdlc_labels)]

if len(sdlc_data) > 0:
    # Create cycle display label with dates
    cycle_info = (
        sdlc_data[["cycle_name", "cycle_starts_at", "cycle_ends_at"]]
        .drop_duplicates()
        .dropna()
    )
    cycle_info["cycle_label"] = cycle_info.apply(
        lambda r: f"{r['cycle_name']}\n({r['cycle_starts_at'].strftime('%Y-%m-%d')} - {r['cycle_ends_at'].strftime('%Y-%m-%d')})",
        axis=1,
    )
    cycle_label_map = dict(zip(cycle_info["cycle_name"], cycle_info["cycle_label"]))
    cycle_sort_order = cycle_info.sort_values("cycle_starts_at")["cycle_label"].tolist()

    # Aggregate by cycle and SDLC label
    sdlc_agg = (
        sdlc_data.groupby(["cycle_name", "labels"])["estimate"]
        .sum()
        .reset_index(name="estimate")
    )
    sdlc_agg["cycle_label"] = sdlc_agg["cycle_name"].map(cycle_label_map)
    sdlc_agg = sdlc_agg.dropna(subset=["cycle_label"])

    # Calculate percentages per cycle
    cycle_totals = sdlc_agg.groupby("cycle_label")["estimate"].sum().reset_index(name="total")
    sdlc_agg = sdlc_agg.merge(cycle_totals, on="cycle_label")
    sdlc_agg["pct"] = (sdlc_agg["estimate"] / sdlc_agg["total"] * 100).round(0).astype(int)
    sdlc_agg["pct_label"] = sdlc_agg["pct"].astype(str) + "%"

    # Chart 1: Percentage stacked bar
    pct_chart = alt.Chart(sdlc_agg).mark_bar().encode(
        x=alt.X("cycle_label:N", title="", sort=cycle_sort_order, axis=alt.Axis(labelAngle=0, labelLimit=0, labelExpr="split(datum.label, '\\n')")),
        y=alt.Y("pct:Q", title="% of Total Estimate", stack="normalize", axis=alt.Axis(format=".0%")),
        color=alt.Color(
            "labels:N",
            title="SDLC Label",
            scale=alt.Scale(domain=sdlc_labels, range=[sdlc_colors[l] for l in sdlc_labels]),
            sort=sdlc_labels,
        ),
        order=alt.Order("labels:N", sort="descending"),
        tooltip=["cycle_label:N", "labels:N", "estimate:Q", "pct_label:N"],
    ).properties(height=350)

    pct_text = alt.Chart(sdlc_agg).mark_text(dy=0, color="white", fontSize=11).encode(
        x=alt.X("cycle_label:N", sort=cycle_sort_order),
        y=alt.Y("pct:Q", stack="normalize", bandPosition=0.5),
        text="pct_label:N",
        order=alt.Order("labels:N", sort="descending"),
    )

    st.altair_chart(pct_chart + pct_text, use_container_width=True)

    # Chart 2: Absolute estimate stacked bar
    abs_chart = alt.Chart(sdlc_agg).mark_bar().encode(
        x=alt.X("cycle_label:N", title="", sort=cycle_sort_order, axis=alt.Axis(labelAngle=0, labelLimit=0, labelExpr="split(datum.label, '\\n')")),
        y=alt.Y("estimate:Q", title="Estimate", stack=True),
        color=alt.Color(
            "labels:N",
            title="SDLC Label",
            scale=alt.Scale(domain=sdlc_labels, range=[sdlc_colors[l] for l in sdlc_labels]),
            sort=sdlc_labels,
        ),
        order=alt.Order("labels:N", sort="descending"),
        tooltip=["cycle_label:N", "labels:N", "estimate:Q"],
    ).properties(height=350)

    abs_text = alt.Chart(sdlc_agg).mark_text(dy=0, color="white", fontSize=11).encode(
        x=alt.X("cycle_label:N", sort=cycle_sort_order),
        y=alt.Y("estimate:Q", stack=True, bandPosition=0.5),
        text=alt.Text("estimate:Q", format=".0f"),
        order=alt.Order("labels:N", sort="descending"),
    )

    st.altair_chart(abs_chart + abs_text, use_container_width=True)
else:
    st.info("No issues with SDLC labels found in the selected filters.")

# Points Completed by Assignee table
st.subheader("Points Completed by Assignee")

# Filter to completed issues with assigned owners
done_states = ["Done", "Done Pending Deployment"]
completed_assigned = filtered[
    (filtered["state"].isin(done_states)) & (filtered["assignee_name"].notna())
].copy()

if len(completed_assigned) > 0:
    # Get cycle order by start date
    cycle_order_df = completed_assigned[["cycle_name", "cycle_starts_at"]].dropna().drop_duplicates()
    cycle_order_df = cycle_order_df.sort_values("cycle_starts_at")
    cycle_order = cycle_order_df["cycle_name"].tolist()

    # Pivot: assignees as rows, cycles as columns
    assignee_pivot = completed_assigned.pivot_table(
        index="assignee_name",
        columns="cycle_name",
        values="estimate",
        aggfunc="sum",
        fill_value=0,
    )

    # Reorder columns by cycle start date
    assignee_pivot = assignee_pivot[[c for c in cycle_order if c in assignee_pivot.columns]]

    # Add Total column
    assignee_pivot["Total"] = assignee_pivot.sum(axis=1)

    # Sort by Total descending
    assignee_pivot = assignee_pivot.sort_values("Total", ascending=False)

    # Reset index to make assignee_name a column
    assignee_pivot = assignee_pivot.reset_index()
    assignee_pivot = assignee_pivot.rename(columns={"assignee_name": "Assignee"})

    # Convert to int for cleaner display
    for col in assignee_pivot.columns:
        if col != "Assignee":
            assignee_pivot[col] = assignee_pivot[col].astype(int)

    # Add column totals row
    totals = {"Assignee": "Total"}
    for col in assignee_pivot.columns:
        if col != "Assignee":
            totals[col] = assignee_pivot[col].sum()

    # Build HTML table manually with inline styles
    html = '<table style="width: 100%; border-collapse: collapse;">'

    # Header row
    html += '<tr>'
    for col in assignee_pivot.columns:
        html += f'<th style="text-align: center; padding: 8px; background-color: #f0f2f6; border: 1px solid #ddd; color: black;">{col}</th>'
    html += '</tr>'

    # Data rows
    for _, row in assignee_pivot.iterrows():
        html += '<tr>'
        for col in assignee_pivot.columns:
            html += f'<td style="text-align: center; padding: 8px; border: 1px solid #ddd; color: black;">{row[col]}</td>'
        html += '</tr>'

    # Totals row
    html += '<tr style="font-weight: bold; background-color: #f9f9f9;">'
    for col in assignee_pivot.columns:
        html += f'<td style="text-align: center; padding: 8px; border: 1px solid #ddd; color: black;">{totals[col]}</td>'
    html += '</tr>'

    html += '</table>'

    st.markdown(html, unsafe_allow_html=True)
else:
    st.info("No completed issues with assignees in selected filters")

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

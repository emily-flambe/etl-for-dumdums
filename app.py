"""
Streamlit app for exploring Linear issues data.

Run with: make app
"""

import os

import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

st.set_page_config(page_title="Linear Issues", layout="wide")
st.title("Linear Issues")


@st.cache_resource
def get_client():
    """Create BigQuery client from service account file."""
    return bigquery.Client.from_service_account_json(
        os.environ["GCP_SA_KEY_FILE"],
        project=os.environ["GCP_PROJECT_ID"],
    )


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_issues():
    """Load issues from BigQuery."""
    client = get_client()
    query = """
    SELECT
        identifier,
        title,
        state,
        priority,
        assignee_name,
        cycle_name,
        labels,
        days_since_created,
        created_at,
        updated_at
    FROM linear.fct_issues
    ORDER BY updated_at DESC
    """
    return client.query(query).to_dataframe()


# Load data
df = load_issues()

# Sidebar filters
st.sidebar.header("Filters")

states = ["All"] + sorted(df["state"].dropna().unique().tolist())
selected_state = st.sidebar.selectbox("State", states)

assignees = ["All"] + sorted(df["assignee_name"].dropna().unique().tolist())
selected_assignee = st.sidebar.selectbox("Assignee", assignees)

cycles = ["All"] + sorted(df["cycle_name"].dropna().unique().tolist())
selected_cycle = st.sidebar.selectbox("Cycle", cycles)

# Apply filters
filtered = df.copy()
if selected_state != "All":
    filtered = filtered[filtered["state"] == selected_state]
if selected_assignee != "All":
    filtered = filtered[filtered["assignee_name"] == selected_assignee]
if selected_cycle != "All":
    filtered = filtered[filtered["cycle_name"] == selected_cycle]

# Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Issues", len(filtered))
col2.metric("In Progress", len(filtered[filtered["state"] == "In Progress"]))
col3.metric("Done", len(filtered[filtered["state"] == "Done"]))
col4.metric("Avg Days Open", f"{filtered['days_since_created'].mean():.0f}")

# Display data
st.dataframe(
    filtered,
    use_container_width=True,
    hide_index=True,
    column_config={
        "identifier": st.column_config.TextColumn("ID", width="small"),
        "title": st.column_config.TextColumn("Title", width="large"),
        "state": st.column_config.TextColumn("State", width="small"),
        "priority": st.column_config.NumberColumn("Priority", width="small"),
        "assignee_name": st.column_config.TextColumn("Assignee", width="medium"),
        "cycle_name": st.column_config.TextColumn("Cycle", width="medium"),
        "labels": st.column_config.ListColumn("Labels", width="medium"),
        "days_since_created": st.column_config.NumberColumn("Days Open", width="small"),
    },
)

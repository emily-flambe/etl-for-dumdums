"""
FDA Food Recalls dashboard with US geographic visualization and topic analysis.

Shows food recall distribution and density across US states.
Data from: bigquery-public-data.fda_food.food_enforcement
"""

import altair as alt
import pandas as pd
import streamlit as st
from vega_datasets import data as vega_data

from data import load_fda_recalls_by_state, load_fda_recalls_by_topic, load_fda_recall_topics

st.set_page_config(page_title="FDA Food Recalls", layout="wide")
st.title("FDA Food Recalls")

st.markdown("""
Geographic distribution of FDA food enforcement recalls across US states.
Data includes recalls initiated from January 2025 onwards.
""")

# Load data
recalls_by_state = load_fda_recalls_by_state()
recalls_by_topic = load_fda_recalls_by_topic()
recalls_with_topics = load_fda_recall_topics()

if recalls_with_topics.empty:
    st.warning("No recall data available. Run the sync and dbt pipeline first.")
    st.code("make run-fda-food")
    st.stop()

# Convert dates to datetime for filtering
recalls_with_topics["recall_initiation_date"] = pd.to_datetime(recalls_with_topics["recall_initiation_date"])

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date = recalls_with_topics["recall_initiation_date"].min()
max_date = recalls_with_topics["recall_initiation_date"].max()

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Classification filter
classifications = ["All"] + sorted(recalls_with_topics["classification"].dropna().unique().tolist())
selected_classification = st.sidebar.selectbox(
    "Classification",
    options=classifications,
    help="Class I = Most serious (dangerous), Class II = Moderate, Class III = Minor"
)

# Build topic options organized by category
topic_options = ["All Topics", "Pathogen (Any)", "Allergen (Any)"]
# Add individual topics in logical order
pathogens = ["Listeria", "Salmonella", "E. coli", "Other Pathogen"]
allergens = ["Milk/Dairy", "Eggs", "Peanuts", "Tree Nuts", "Wheat/Gluten", "Soy", "Fish", "Shellfish", "Sesame"]
other_topics = ["Foreign Material", "Labeling", "Temperature"]
topic_options.extend(pathogens + allergens + other_topics)

# Topic filter using pills (single-select) in main area
st.subheader("Filter by Topic")
selected_topic = st.pills(
    "Topic",
    topic_options,
    selection_mode="single",
    default="All Topics",
    label_visibility="collapsed",
)

# Handle None selection (default to All Topics)
if selected_topic is None:
    selected_topic = "All Topics"

# Apply filters to data with topics
filtered_data = recalls_with_topics.copy()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_data = filtered_data[
        (filtered_data["recall_initiation_date"] >= pd.Timestamp(start_date)) &
        (filtered_data["recall_initiation_date"] <= pd.Timestamp(end_date))
    ]

if selected_classification != "All":
    filtered_data = filtered_data[filtered_data["classification"] == selected_classification]

# Topic filtering uses the boolean flags or rollup flags
if selected_topic != "All Topics":
    if selected_topic == "Pathogen (Any)":
        filtered_data = filtered_data[filtered_data["has_pathogen"] == True]
    elif selected_topic == "Allergen (Any)":
        filtered_data = filtered_data[filtered_data["has_allergen"] == True]
    else:
        # Map topic name to boolean column
        topic_column_map = {
            "Listeria": "is_listeria",
            "Salmonella": "is_salmonella",
            "E. coli": "is_ecoli",
            "Other Pathogen": "is_other_pathogen",
            "Milk/Dairy": "is_milk",
            "Eggs": "is_eggs",
            "Peanuts": "is_peanuts",
            "Tree Nuts": "is_tree_nuts",
            "Wheat/Gluten": "is_wheat",
            "Soy": "is_soy",
            "Fish": "is_fish",
            "Shellfish": "is_shellfish",
            "Sesame": "is_sesame",
            "Foreign Material": "is_foreign_material",
            "Labeling": "is_labeling",
            "Temperature": "is_temperature",
        }
        if selected_topic in topic_column_map:
            col = topic_column_map[selected_topic]
            filtered_data = filtered_data[filtered_data[col] == True]

# Re-aggregate by state with filters applied
filtered_by_state = (
    filtered_data.groupby("state_code")
    .agg(
        total_recalls=("recall_number", "count"),
        class_i_recalls=("classification", lambda x: (x == "Class I").sum()),
        class_ii_recalls=("classification", lambda x: (x == "Class II").sum()),
        class_iii_recalls=("classification", lambda x: (x == "Class III").sum()),
    )
    .reset_index()
)

# Add FIPS codes for mapping
fips_mapping = {
    'AL': 1, 'AK': 2, 'AZ': 4, 'AR': 5, 'CA': 6, 'CO': 8, 'CT': 9, 'DE': 10,
    'FL': 12, 'GA': 13, 'HI': 15, 'ID': 16, 'IL': 17, 'IN': 18, 'IA': 19, 'KS': 20,
    'KY': 21, 'LA': 22, 'ME': 23, 'MD': 24, 'MA': 25, 'MI': 26, 'MN': 27, 'MS': 28,
    'MO': 29, 'MT': 30, 'NE': 31, 'NV': 32, 'NH': 33, 'NJ': 34, 'NM': 35, 'NY': 36,
    'NC': 37, 'ND': 38, 'OH': 39, 'OK': 40, 'OR': 41, 'PA': 42, 'RI': 44, 'SC': 45,
    'SD': 46, 'TN': 47, 'TX': 48, 'UT': 49, 'VT': 50, 'VA': 51, 'WA': 53, 'WV': 54,
    'WI': 55, 'WY': 56, 'DC': 11, 'PR': 72
}

state_names = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire',
    'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
    'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
    'PR': 'Puerto Rico'
}

filtered_by_state["id"] = filtered_by_state["state_code"].map(fips_mapping)
filtered_by_state["state_name"] = filtered_by_state["state_code"].map(state_names)

# Handle unmapped states gracefully
unmapped = filtered_by_state[filtered_by_state["id"].isna()]
if not unmapped.empty:
    unmapped_codes = unmapped["state_code"].unique().tolist()
    st.sidebar.warning(f"Excluding {len(unmapped)} recalls from unmapped regions: {unmapped_codes}")
    filtered_by_state = filtered_by_state.dropna(subset=["id"])

# Metrics row
st.subheader("Summary")
col1, col2, col3, col4 = st.columns(4)

total_recalls = filtered_data.shape[0]
class_i_count = (filtered_data["classification"] == "Class I").sum()
class_ii_count = (filtered_data["classification"] == "Class II").sum()
states_affected = filtered_by_state.shape[0]

col1.metric("Total Recalls", f"{total_recalls:,}")
col2.metric("Class I (Dangerous)", f"{class_i_count:,}")
col3.metric("Class II (Moderate)", f"{class_ii_count:,}")
col4.metric("States Affected", f"{states_affected}")

# US Map
st.subheader("Recall Distribution by State")

# Load US states geography
states_geo = alt.topo_feature(vega_data.us_10m.url, "states")

# Create the choropleth map
# Base map with state outlines
background = alt.Chart(states_geo).mark_geoshape(
    fill="lightgray",
    stroke="white",
    strokeWidth=0.5,
).project(
    type="albersUsa"
).properties(
    width=800,
    height=500
)

# Choropleth layer with recall data
if not filtered_by_state.empty:
    choropleth = alt.Chart(states_geo).mark_geoshape(
        stroke="white",
        strokeWidth=0.5,
    ).encode(
        color=alt.Color(
            "total_recalls:Q",
            scale=alt.Scale(scheme="reds"),
            legend=alt.Legend(title="Total Recalls"),
        ),
        tooltip=[
            alt.Tooltip("state_name:N", title="State"),
            alt.Tooltip("total_recalls:Q", title="Total Recalls"),
            alt.Tooltip("class_i_recalls:Q", title="Class I"),
            alt.Tooltip("class_ii_recalls:Q", title="Class II"),
            alt.Tooltip("class_iii_recalls:Q", title="Class III"),
        ],
    ).transform_lookup(
        lookup="id",
        from_=alt.LookupData(filtered_by_state, "id", ["state_name", "total_recalls", "class_i_recalls", "class_ii_recalls", "class_iii_recalls"]),
    ).project(
        type="albersUsa"
    ).properties(
        width=800,
        height=500
    )

    st.altair_chart(background + choropleth, use_container_width=True)
else:
    st.altair_chart(background, use_container_width=True)
    st.info("No data for selected filters.")

# Topic distribution chart
st.subheader("Recalls by Topic")

# Filter topics to show individual topics only (not rollups) and only those with recall_count > 0
topic_chart_data = recalls_by_topic[
    (~recalls_by_topic["topic_category"].str.contains("Rollup", na=False)) &
    (recalls_by_topic["recall_count"] > 0)
].copy()

if not topic_chart_data.empty:
    # Color by category
    category_colors = {
        "Pathogen": "#dc2626",
        "Allergen": "#f97316",
        "Physical": "#8b5cf6",
        "Labeling": "#06b6d4",
        "Process": "#10b981",
        "Other": "#6b7280"
    }

    topic_chart = alt.Chart(topic_chart_data).mark_bar().encode(
        x=alt.X("recall_count:Q", title="Number of Recalls"),
        y=alt.Y("topic:N", sort="-x", title="Topic"),
        color=alt.Color(
            "topic_category:N",
            scale=alt.Scale(
                domain=list(category_colors.keys()),
                range=list(category_colors.values())
            ),
            legend=alt.Legend(title="Category"),
        ),
        tooltip=[
            alt.Tooltip("topic:N", title="Topic"),
            alt.Tooltip("topic_category:N", title="Category"),
            alt.Tooltip("recall_count:Q", title="Recalls"),
            alt.Tooltip("class_i_count:Q", title="Class I"),
            alt.Tooltip("states_affected:Q", title="States Affected"),
        ],
    ).properties(height=400)

    st.altair_chart(topic_chart, use_container_width=True)

# Classification breakdown chart
st.subheader("Recalls by Classification")

if not filtered_data.empty:
    class_counts = filtered_data["classification"].value_counts().reset_index()
    class_counts.columns = ["classification", "count"]

    # Sort by severity
    class_order = ["Class I", "Class II", "Class III"]
    class_counts["sort_order"] = class_counts["classification"].map(
        {c: i for i, c in enumerate(class_order)}
    )
    class_counts = class_counts.sort_values("sort_order")

    class_chart = alt.Chart(class_counts).mark_bar().encode(
        x=alt.X("classification:N", sort=class_order, title="Classification"),
        y=alt.Y("count:Q", title="Number of Recalls"),
        color=alt.Color(
            "classification:N",
            scale=alt.Scale(
                domain=class_order,
                range=["#dc2626", "#f97316", "#facc15"]  # Red, orange, yellow
            ),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("classification:N", title="Classification"),
            alt.Tooltip("count:Q", title="Recalls"),
        ],
    ).properties(height=300)

    st.altair_chart(class_chart, use_container_width=True)

# Top states table
st.subheader("Top States by Recall Count")

if not filtered_by_state.empty:
    top_states = filtered_by_state.nlargest(15, "total_recalls")

    st.dataframe(
        top_states,
        use_container_width=True,
        hide_index=True,
        column_config={
            "state_code": st.column_config.TextColumn("State", width="small"),
            "state_name": st.column_config.TextColumn("State Name", width="medium"),
            "total_recalls": st.column_config.NumberColumn("Total", format="%d", width="small"),
            "class_i_recalls": st.column_config.NumberColumn("Class I", format="%d", width="small"),
            "class_ii_recalls": st.column_config.NumberColumn("Class II", format="%d", width="small"),
            "class_iii_recalls": st.column_config.NumberColumn("Class III", format="%d", width="small"),
            "id": None,  # Hide FIPS code
        },
        column_order=["state_code", "state_name", "total_recalls", "class_i_recalls", "class_ii_recalls", "class_iii_recalls"],
    )

# Recent recalls table
st.subheader("Recent Recalls")

recent_recalls = filtered_data.head(20).copy()
if not recent_recalls.empty:
    recent_recalls["recall_initiation_date"] = recent_recalls["recall_initiation_date"].dt.strftime("%Y-%m-%d")

    # Format topics array as comma-separated string for display
    if "topics" in recent_recalls.columns:
        recent_recalls["topics_display"] = recent_recalls["topics"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else "None identified"
        )

    st.dataframe(
        recent_recalls,
        use_container_width=True,
        hide_index=True,
        column_config={
            "recall_number": st.column_config.TextColumn("Recall #", width="small"),
            "recall_initiation_date": st.column_config.TextColumn("Date", width="small"),
            "classification": st.column_config.TextColumn("Class", width="small"),
            "state_code": st.column_config.TextColumn("State", width="small"),
            "recalling_firm": st.column_config.TextColumn("Firm", width="medium"),
            "topics_display": st.column_config.TextColumn("Topics", width="medium"),
            "reason_for_recall": st.column_config.TextColumn("Reason", width="large"),
            # Hide all other columns
            "topics": None,
            "topic_count": None,
            "has_pathogen": None,
            "has_allergen": None,
            "is_listeria": None,
            "is_salmonella": None,
            "is_ecoli": None,
            "is_other_pathogen": None,
            "is_milk": None,
            "is_eggs": None,
            "is_peanuts": None,
            "is_tree_nuts": None,
            "is_wheat": None,
            "is_soy": None,
            "is_fish": None,
            "is_shellfish": None,
            "is_sesame": None,
            "is_foreign_material": None,
            "is_labeling": None,
            "is_temperature": None,
        },
        column_order=["recall_number", "recall_initiation_date", "classification", "state_code", "recalling_firm", "topics_display", "reason_for_recall"],
    )

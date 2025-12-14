"""
FDA Food Adverse Events dashboard.

Visualizes adverse event reports from the FDA CFSAN Adverse Event Reporting System (CAERS).
Shows reaction categories, product industries, and trends over time.
"""

import altair as alt
import pandas as pd
import streamlit as st

from data import (
    load_fda_events_by_reaction,
    load_fda_events_by_product,
    load_fda_events_monthly,
    load_fda_events_monthly_by_industry,
)

st.title("FDA Food Adverse Events")

st.markdown("""
Consumer-reported adverse reactions to foods, dietary supplements, and cosmetics. Data comes from
the [CAERS (CFSAN Adverse Event Reporting System)](https://open.fda.gov/apis/food/event/),
the FDA's database for tracking health problems potentially linked to these products.

**About the Data:**
- **Source:** [BigQuery Public Dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=fda_food&t=food_events)
  (`bigquery-public-data.fda_food.food_events`)
- **Coverage:** 2004 to present
- **Updates:** Quarterly from FDA

**Important Limitations:**
- Reporting is **voluntary** for consumers and healthcare professionals
- A report does **not** prove the product caused the reaction
- Higher report counts may reflect product popularity, not higher risk

*Note: This data should not be used to make medical decisions. Consult a healthcare provider for health concerns.*
""")

# Load all data
reaction_df = load_fda_events_by_reaction()
product_df = load_fda_events_by_product()
monthly_df = load_fda_events_monthly()
monthly_industry_df = load_fda_events_monthly_by_industry()

if reaction_df.empty:
    st.warning("No event data available. Run the sync and dbt pipeline first.")
    st.code("make run-fda-food-events")
    st.stop()

# Prepare data
monthly_df["month"] = pd.to_datetime(monthly_df["month"])
monthly_industry_df["month"] = pd.to_datetime(monthly_industry_df["month"])

# --- Filters Section ---
st.subheader("Filters")

# Date range filter with date selectors
min_date = monthly_df["month"].min().date()
max_date = monthly_df["month"].max().date()
# Default to last 10 years
default_start = pd.Timestamp(max_date) - pd.DateOffset(years=10)
default_start = max(default_start.date(), min_date)

col_start, col_end, col_spacer = st.columns([1, 1, 4])
with col_start:
    start_date = st.date_input(
        "Start Date",
        value=default_start,
        min_value=min_date,
        max_value=max_date,
    )
with col_end:
    end_date = st.date_input(
        "End Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

# Industry pills
st.markdown("**Product Industries**")
top_industries = product_df.nlargest(15, "event_count")["industry_name"].tolist()

# Initialize session state for industries
if "industry_pills" not in st.session_state:
    st.session_state["industry_pills"] = top_industries[:5]

col1, col2, col3 = st.columns([1, 1, 6])
with col1:
    if st.button("Select All", key="ind_all", use_container_width=True):
        st.session_state["industry_pills"] = top_industries
        st.rerun()
with col2:
    if st.button("Clear All", key="ind_clear", use_container_width=True):
        st.session_state["industry_pills"] = []
        st.rerun()

selected_industries = st.pills(
    "Industries",
    options=top_industries,
    selection_mode="multi",
    label_visibility="collapsed",
    default=[i for i in st.session_state["industry_pills"] if i in top_industries],
)

# Reaction category pills
st.markdown("**Reaction Categories**")
reaction_categories = ["Gastrointestinal", "Allergic", "Respiratory", "Cardiovascular", "Neurological", "Systemic"]

# Initialize session state for categories
if "category_pills" not in st.session_state:
    st.session_state["category_pills"] = reaction_categories.copy()

col1, col2, col3 = st.columns([1, 1, 6])
with col1:
    if st.button("Select All", key="cat_all", use_container_width=True):
        st.session_state["category_pills"] = reaction_categories.copy()
        st.rerun()
with col2:
    if st.button("Clear All", key="cat_clear", use_container_width=True):
        st.session_state["category_pills"] = []
        st.rerun()

selected_categories = st.pills(
    "Categories",
    options=reaction_categories,
    selection_mode="multi",
    label_visibility="collapsed",
    default=st.session_state["category_pills"],
)

st.divider()

# Apply date filter to monthly data
monthly_filtered = monthly_df[
    (monthly_df["month"].dt.date >= start_date) &
    (monthly_df["month"].dt.date <= end_date)
].copy()

monthly_industry_filtered = monthly_industry_df[
    (monthly_industry_df["month"].dt.date >= start_date) &
    (monthly_industry_df["month"].dt.date <= end_date)
].copy()

# --- Summary Metrics ---
st.subheader("Summary")

total_events = monthly_filtered["event_count"].sum()
total_hospitalizations = monthly_filtered["hospitalization_count"].sum()
total_deaths = monthly_filtered["death_count"].sum()
hosp_rate = (total_hospitalizations / total_events * 100) if total_events > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", f"{total_events:,}")
col2.metric("Hospitalizations", f"{total_hospitalizations:,}")
col3.metric("Deaths", f"{total_deaths:,}")
col4.metric("Hospitalization Rate", f"{hosp_rate:.1f}%")

# --- Reaction Category Trends ---
st.subheader("Reaction Category Trends Over Time")

# Reshape monthly data for category trends
category_cols = {
    "Gastrointestinal": "gastrointestinal_count",
    "Allergic": "allergic_count",
    "Respiratory": "respiratory_count",
    "Cardiovascular": "cardiovascular_count",
    "Neurological": "neurological_count",
    "Systemic": "systemic_count",
}

# Filter to selected categories
active_category_cols = {k: v for k, v in category_cols.items() if k in (selected_categories or [])}

if active_category_cols:
    # Melt data for multi-line chart
    category_trend_data = monthly_filtered[["month"] + list(active_category_cols.values())].melt(
        id_vars=["month"],
        var_name="category_col",
        value_name="count"
    )
    # Map column names back to readable names
    col_to_name = {v: k for k, v in category_cols.items()}
    category_trend_data["Category"] = category_trend_data["category_col"].map(col_to_name)

    category_trend_chart = (
        alt.Chart(category_trend_data)
        .mark_area(opacity=0.7)
        .encode(
            x=alt.X("month:T", title="Month", axis=alt.Axis(format="%b %Y")),
            y=alt.Y("count:Q", title="Number of Events", stack="zero"),
            color=alt.Color(
                "Category:N",
                scale=alt.Scale(scheme="tableau10"),
                legend=alt.Legend(orient="top")
            ),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%B %Y"),
                alt.Tooltip("Category:N", title="Category"),
                alt.Tooltip("count:Q", title="Events", format=",d"),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(category_trend_chart, use_container_width=True)
else:
    st.info("Select at least one reaction category to see trends.")

# --- Industry Trends ---
st.subheader("Event Trends by Product Industry")

if selected_industries:
    # Filter to selected industries and aggregate by month
    industry_trend_data = monthly_industry_filtered[
        monthly_industry_filtered["industry_name"].isin(selected_industries)
    ].copy()

    # Aggregate by month and industry
    industry_trend_agg = (
        industry_trend_data.groupby(["month", "industry_name"])
        .agg({"event_count": "sum"})
        .reset_index()
    )

    industry_trend_chart = (
        alt.Chart(industry_trend_agg)
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", title="Month", axis=alt.Axis(format="%b %Y")),
            y=alt.Y("event_count:Q", title="Number of Events"),
            color=alt.Color(
                "industry_name:N",
                title="Industry",
                scale=alt.Scale(scheme="category10"),
                legend=alt.Legend(orient="top", columns=2, labelLimit=400)
            ),
            strokeDash=alt.StrokeDash("industry_name:N", legend=None),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%B %Y"),
                alt.Tooltip("industry_name:N", title="Industry"),
                alt.Tooltip("event_count:Q", title="Events", format=",d"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(industry_trend_chart, use_container_width=True)
else:
    st.info("Select at least one industry to see trends.")

# --- Reaction vs Industry Heatmap ---
st.subheader("Reaction Categories by Industry")

if selected_industries:
    # Get data for selected industries
    heatmap_industries = product_df[product_df["industry_name"].isin(selected_industries)].copy()

    # Reshape for heatmap
    heatmap_data = []
    for _, row in heatmap_industries.iterrows():
        industry = row["industry_name"]
        total = row["event_count"]
        if total > 0:
            for cat, col in [("GI", "gastrointestinal_count"), ("Allergic", "allergic_count"),
                           ("Respiratory", "respiratory_count"), ("Cardiovascular", "cardiovascular_count"),
                           ("Neurological", "neurological_count"), ("Systemic", "systemic_count")]:
                pct = row[col] / total * 100 if col in row else 0
                heatmap_data.append({
                    "Industry": industry,
                    "Category": cat,
                    "Percentage": round(pct, 1),
                    "Count": row[col] if col in row else 0
                })

    if heatmap_data:
        heatmap_df = pd.DataFrame(heatmap_data)

        heatmap_chart = (
            alt.Chart(heatmap_df)
            .mark_rect()
            .encode(
                x=alt.X("Category:N", title="Reaction Category"),
                y=alt.Y("Industry:N", title="Product Industry", sort="-x", axis=alt.Axis(labelLimit=400)),
                color=alt.Color(
                    "Percentage:Q",
                    scale=alt.Scale(scheme="orangered"),
                    title="% of Events"
                ),
                tooltip=[
                    alt.Tooltip("Industry:N", title="Industry"),
                    alt.Tooltip("Category:N", title="Category"),
                    alt.Tooltip("Percentage:Q", title="% of Events", format=".1f"),
                    alt.Tooltip("Count:Q", title="Event Count", format=",d"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(heatmap_chart, use_container_width=True)
else:
    st.info("Select at least one industry to see the heatmap.")

# --- Top Reactions ---
st.subheader("Top Adverse Reactions")

col1, col2 = st.columns([2, 1])

with col1:
    top_reactions = reaction_df.head(15).copy()

    reaction_chart = (
        alt.Chart(top_reactions)
        .mark_bar()
        .encode(
            x=alt.X("event_count:Q", title="Number of Events"),
            y=alt.Y("reaction:N", title="Reaction", sort="-x"),
            color=alt.Color(
                "hospitalization_pct:Q",
                scale=alt.Scale(scheme="reds", domain=[0, 30]),
                title="Hospitalization %",
            ),
            tooltip=[
                alt.Tooltip("reaction:N", title="Reaction"),
                alt.Tooltip("event_count:Q", title="Events", format=",d"),
                alt.Tooltip("hospitalization_count:Q", title="Hospitalizations", format=",d"),
                alt.Tooltip("hospitalization_pct:Q", title="Hospitalization %", format=".1f"),
                alt.Tooltip("death_count:Q", title="Deaths", format=",d"),
            ],
        )
        .properties(height=450)
    )

    st.altair_chart(reaction_chart, use_container_width=True)

with col2:
    st.markdown("**Reaction Details**")
    st.dataframe(
        reaction_df.head(15)[
            ["reaction", "event_count", "hospitalization_pct", "death_count"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "reaction": st.column_config.TextColumn("Reaction"),
            "event_count": st.column_config.NumberColumn("Events", format="%d"),
            "hospitalization_pct": st.column_config.NumberColumn("Hosp %", format="%.1f"),
            "death_count": st.column_config.NumberColumn("Deaths", format="%d"),
        },
    )

# --- Products by Industry ---
st.subheader("All Product Industries")

col1, col2 = st.columns([2, 1])

with col1:
    top_products = product_df.head(15).copy()

    product_chart = (
        alt.Chart(top_products)
        .mark_bar()
        .encode(
            x=alt.X("event_count:Q", title="Number of Events"),
            y=alt.Y("industry_name:N", title="Industry", sort="-x", axis=alt.Axis(labelLimit=400)),
            color=alt.Color(
                "hospitalization_count:Q",
                scale=alt.Scale(scheme="blues"),
                title="Hospitalizations",
            ),
            tooltip=[
                alt.Tooltip("industry_name:N", title="Industry"),
                alt.Tooltip("event_count:Q", title="Events", format=",d"),
                alt.Tooltip("gastrointestinal_count:Q", title="GI Reactions", format=",d"),
                alt.Tooltip("allergic_count:Q", title="Allergic", format=",d"),
                alt.Tooltip("hospitalization_count:Q", title="Hospitalizations", format=",d"),
                alt.Tooltip("top_reaction:N", title="Top Reaction"),
            ],
        )
        .properties(height=450)
    )

    st.altair_chart(product_chart, use_container_width=True)

with col2:
    st.markdown("**Industry Details**")
    st.dataframe(
        product_df.head(15)[
            ["industry_name", "event_count", "hospitalization_count", "top_reaction"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "industry_name": st.column_config.TextColumn("Industry"),
            "event_count": st.column_config.NumberColumn("Events", format="%d"),
            "hospitalization_count": st.column_config.NumberColumn("Hosp", format="%d"),
            "top_reaction": st.column_config.TextColumn("Top Reaction"),
        },
    )

# --- Reaction Category Distribution ---
st.subheader("Overall Reaction Category Distribution")

# Aggregate reaction counts by category group from filtered data
category_data = pd.DataFrame({
    "Category": ["Gastrointestinal", "Allergic", "Respiratory", "Cardiovascular", "Neurological", "Systemic"],
    "Events": [
        monthly_filtered["gastrointestinal_count"].sum(),
        monthly_filtered["allergic_count"].sum(),
        monthly_filtered["respiratory_count"].sum(),
        monthly_filtered["cardiovascular_count"].sum(),
        monthly_filtered["neurological_count"].sum(),
        monthly_filtered["systemic_count"].sum(),
    ]
})

category_data = category_data.sort_values("Events", ascending=False)
total_cat = category_data["Events"].sum()
category_data["Percentage"] = (category_data["Events"] / total_cat * 100).round(1)

col1, col2 = st.columns([2, 1])

with col1:
    category_chart = (
        alt.Chart(category_data)
        .mark_bar()
        .encode(
            x=alt.X("Events:Q", title="Number of Events"),
            y=alt.Y("Category:N", title="Reaction Category", sort="-x"),
            color=alt.Color(
                "Category:N",
                scale=alt.Scale(scheme="tableau10"),
                legend=None
            ),
            tooltip=[
                alt.Tooltip("Category:N", title="Category"),
                alt.Tooltip("Events:Q", title="Events", format=",d"),
                alt.Tooltip("Percentage:Q", title="Share %", format=".1f"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(category_chart, use_container_width=True)

with col2:
    st.dataframe(
        category_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Category": st.column_config.TextColumn("Category"),
            "Events": st.column_config.NumberColumn("Events", format="%d"),
            "Percentage": st.column_config.NumberColumn("Share %", format="%.1f"),
        },
    )

# --- Monthly Data Table ---
with st.expander("View Monthly Data Table"):
    display_monthly = monthly_filtered.copy()
    display_monthly["month"] = display_monthly["month"].dt.strftime("%Y-%m")

    st.dataframe(
        display_monthly[["month", "event_count", "gastrointestinal_count", "allergic_count",
                         "respiratory_count", "hospitalization_count", "death_count"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "month": st.column_config.TextColumn("Month"),
            "event_count": st.column_config.NumberColumn("Events", format="%d"),
            "gastrointestinal_count": st.column_config.NumberColumn("GI", format="%d"),
            "allergic_count": st.column_config.NumberColumn("Allergic", format="%d"),
            "respiratory_count": st.column_config.NumberColumn("Respiratory", format="%d"),
            "hospitalization_count": st.column_config.NumberColumn("Hospitalizations", format="%d"),
            "death_count": st.column_config.NumberColumn("Deaths", format="%d"),
        },
    )

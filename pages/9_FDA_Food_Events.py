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

**Products Covered:**
- Foods (domestic and imported, excluding meat and poultry)
- Dietary supplements
- Cosmetics (hair products, makeup, soaps, lotions)

**Important Limitations:**
- Reporting is **voluntary** for consumers and healthcare professionals
- A report does **not** prove the product caused the reaction
- Reports lack extensive verification and represent a small fraction of actual events
- Higher report counts may reflect product popularity, not higher risk

*Note: This data should not be used to make medical decisions. Consult a healthcare provider for health concerns.*
""")

# Load all data
reaction_df = load_fda_events_by_reaction()
product_df = load_fda_events_by_product()
monthly_df = load_fda_events_monthly()

if reaction_df.empty:
    st.warning("No event data available. Run the sync and dbt pipeline first.")
    st.code("make run-fda-food-events")
    st.stop()

# --- Summary Metrics ---
st.subheader("Summary")

total_events = monthly_df["event_count"].sum()
total_hospitalizations = monthly_df["hospitalization_count"].sum()
total_gi = monthly_df["gastrointestinal_count"].sum()
total_allergic = monthly_df["allergic_count"].sum()
unique_reactions = len(reaction_df)
unique_industries = len(product_df)

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Events", f"{total_events:,}")
col2.metric("Hospitalizations", f"{total_hospitalizations:,}")
col3.metric("GI Reactions", f"{total_gi:,}")
col4.metric("Allergic Reactions", f"{total_allergic:,}")
col5.metric("Reaction Types", unique_reactions)
col6.metric("Product Categories", unique_industries)

# --- Monthly Trends ---
st.subheader("Monthly Event Trends")

# Prepare monthly data
monthly_df["month"] = pd.to_datetime(monthly_df["month"])
monthly_filtered = monthly_df[monthly_df["year"] >= 2015].copy()

# Metric selector
metric_options = {
    "Total Events": "event_count",
    "Hospitalizations": "hospitalization_count",
    "Gastrointestinal": "gastrointestinal_count",
    "Allergic": "allergic_count",
    "Respiratory": "respiratory_count",
    "Cardiovascular": "cardiovascular_count",
    "Neurological": "neurological_count",
}

selected_metric = st.selectbox(
    "Select metric to display",
    options=list(metric_options.keys()),
    index=0,
)

metric_col = metric_options[selected_metric]

trend_chart = (
    alt.Chart(monthly_filtered)
    .mark_line(point=True)
    .encode(
        x=alt.X("month:T", title="Month", axis=alt.Axis(format="%b %Y")),
        y=alt.Y(f"{metric_col}:Q", title=selected_metric),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%B %Y"),
            alt.Tooltip(f"{metric_col}:Q", title=selected_metric, format=",d"),
        ],
    )
    .properties(height=350)
)

st.altair_chart(trend_chart, use_container_width=True)

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
                scale=alt.Scale(scheme="reds"),
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
st.subheader("Events by Product Industry")

col1, col2 = st.columns([2, 1])

with col1:
    top_products = product_df.head(15).copy()

    product_chart = (
        alt.Chart(top_products)
        .mark_bar()
        .encode(
            x=alt.X("event_count:Q", title="Number of Events"),
            y=alt.Y("industry_name:N", title="Industry", sort="-x"),
            color=alt.Color(
                "event_count:Q",
                scale=alt.Scale(scheme="blues"),
                legend=None,
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

# --- Reaction Category Breakdown ---
st.subheader("Reaction Category Distribution")

# Aggregate reaction counts by category group
category_data = pd.DataFrame({
    "Category": ["Gastrointestinal", "Allergic", "Respiratory", "Cardiovascular", "Neurological", "Systemic"],
    "Events": [
        monthly_df["gastrointestinal_count"].sum(),
        monthly_df["allergic_count"].sum(),
        monthly_df["respiratory_count"].sum(),
        monthly_df["cardiovascular_count"].sum(),
        monthly_df["neurological_count"].sum(),
        monthly_df["systemic_count"].sum(),
    ]
})

category_data = category_data.sort_values("Events", ascending=False)
total_cat = category_data["Events"].sum()
category_data["Percentage"] = (category_data["Events"] / total_cat * 100).round(1)

category_chart = (
    alt.Chart(category_data)
    .mark_bar()
    .encode(
        x=alt.X("Events:Q", title="Number of Events"),
        y=alt.Y("Category:N", title="Category", sort="-x"),
        color=alt.Color(
            "Events:Q",
            scale=alt.Scale(scheme="viridis"),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("Category:N", title="Category"),
            alt.Tooltip("Events:Q", title="Events", format=",d"),
            alt.Tooltip("Percentage:Q", title="Share %", format=".1f"),
        ],
    )
    .properties(height=300)
)

st.altair_chart(category_chart, use_container_width=True)

# --- Raw Data Tables ---
st.subheader("Monthly Data")

display_monthly = monthly_df.copy()
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

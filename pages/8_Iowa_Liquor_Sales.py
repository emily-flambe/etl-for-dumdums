"""
Iowa Liquor Sales dashboard.

Visualizes liquor sales data from Iowa state, including trends by category,
geographic distribution, and vendor performance.
"""

import altair as alt
import pandas as pd
import streamlit as st

from data import (
    load_iowa_liquor_monthly,
    load_iowa_liquor_by_county,
    load_iowa_liquor_vendors,
)

st.set_page_config(page_title="Iowa Liquor Sales", layout="wide")
st.title("Iowa Liquor Sales Analytics")

st.markdown("""
Analysis of liquor sales transactions from Iowa state. Data sourced from the
BigQuery public dataset `bigquery-public-data.iowa_liquor_sales.sales`.
""")

# Load all data
monthly_df = load_iowa_liquor_monthly()
county_df = load_iowa_liquor_by_county()
vendor_df = load_iowa_liquor_vendors()

if monthly_df.empty:
    st.warning("No sales data available. Run the sync and dbt pipeline first.")
    st.code("make run-iowa-liquor")
    st.stop()

# Convert dates for Altair
monthly_df["sale_month"] = pd.to_datetime(monthly_df["sale_month"])

# --- Summary Metrics ---
st.subheader("Summary")

# Calculate totals
total_sales = monthly_df["total_sales"].sum()
total_bottles = monthly_df["total_bottles"].sum()
total_volume = monthly_df["total_volume_liters"].sum()
unique_categories = monthly_df["category_name"].nunique()
unique_counties = len(county_df)
unique_vendors = len(vendor_df)

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Sales", f"${total_sales:,.0f}")
col2.metric("Bottles Sold", f"{total_bottles:,.0f}")
col3.metric("Volume (L)", f"{total_volume:,.0f}")
col4.metric("Categories", unique_categories)
col5.metric("Counties", unique_counties)
col6.metric("Vendors", unique_vendors)

# --- Monthly Sales Trends ---
st.subheader("Monthly Sales Trends")

# Get top categories by total sales
top_categories = (
    monthly_df.groupby("category_name")["total_sales"]
    .sum()
    .nlargest(10)
    .index.tolist()
)

# Category filter
selected_categories = st.multiselect(
    "Select categories to display",
    options=top_categories,
    default=top_categories[:5],
    help="Showing top 10 categories by total sales",
)

if selected_categories:
    trend_data = monthly_df[monthly_df["category_name"].isin(selected_categories)]

    # Aggregate by month and category
    trend_agg = (
        trend_data.groupby(["sale_month", "category_name"])
        .agg({"total_sales": "sum"})
        .reset_index()
    )

    # Get unique month count for proper tick spacing
    unique_months = trend_agg["sale_month"].nunique()

    trend_chart = (
        alt.Chart(trend_agg)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "sale_month:T",
                title="Month",
                axis=alt.Axis(format="%b %Y", tickCount=unique_months),
            ),
            y=alt.Y("total_sales:Q", title="Sales ($)", axis=alt.Axis(format="$,.0f")),
            color=alt.Color("category_name:N", title="Category"),
            tooltip=[
                alt.Tooltip("sale_month:T", title="Month", format="%B %Y"),
                alt.Tooltip("category_name:N", title="Category"),
                alt.Tooltip("total_sales:Q", title="Sales", format="$,.0f"),
            ],
        )
        .properties(height=400)
    )

    st.altair_chart(trend_chart, use_container_width=True)
else:
    st.info("Select at least one category to see trends.")

# --- Top Categories Table ---
st.subheader("Top Categories by Sales")

# Get latest month data
latest_month = monthly_df["sale_month"].max()
latest_data = monthly_df[monthly_df["sale_month"] == latest_month].copy()

# Calculate month-over-month change
prev_month = latest_month - pd.DateOffset(months=1)
prev_data = monthly_df[monthly_df["sale_month"] == prev_month].copy()

if not prev_data.empty:
    prev_sales = prev_data.set_index("category_name")["total_sales"].to_dict()
    latest_data["prev_sales"] = latest_data["category_name"].map(prev_sales)
    # Handle NULL and zero values to avoid division errors
    latest_data["mom_change"] = latest_data.apply(
        lambda row: (
            (row["total_sales"] - row["prev_sales"]) / row["prev_sales"] * 100
            if pd.notna(row["prev_sales"]) and row["prev_sales"] != 0
            else None
        ),
        axis=1,
    )
else:
    latest_data["mom_change"] = None

# Display top 15 categories
display_cats = latest_data.nlargest(15, "total_sales")[
    ["category_name", "total_sales", "total_bottles", "avg_bottle_price", "store_count", "mom_change"]
]

st.dataframe(
    display_cats,
    use_container_width=True,
    hide_index=True,
    column_config={
        "category_name": st.column_config.TextColumn("Category"),
        "total_sales": st.column_config.NumberColumn("Sales ($)", format="%.0f"),
        "total_bottles": st.column_config.NumberColumn("Bottles", format="%d"),
        "avg_bottle_price": st.column_config.NumberColumn("Avg Price ($)", format="%.2f"),
        "store_count": st.column_config.NumberColumn("Stores", format="%d"),
        "mom_change": st.column_config.NumberColumn("MoM %", format="%.1f"),
    },
)

# --- Geographic Distribution ---
st.subheader("Sales by County")

col1, col2 = st.columns([2, 1])

with col1:
    # Bar chart of top counties
    top_counties = county_df.nlargest(20, "total_sales")

    county_chart = (
        alt.Chart(top_counties)
        .mark_bar()
        .encode(
            x=alt.X("total_sales:Q", title="Total Sales ($)", axis=alt.Axis(format="$,.0f")),
            y=alt.Y("county:N", title="County", sort="-x"),
            color=alt.Color(
                "total_sales:Q",
                scale=alt.Scale(scheme="blues"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("county:N", title="County"),
                alt.Tooltip("total_sales:Q", title="Sales", format="$,.0f"),
                alt.Tooltip("total_bottles:Q", title="Bottles", format=",d"),
                alt.Tooltip("store_count:Q", title="Stores"),
                alt.Tooltip("top_category:N", title="Top Category"),
            ],
        )
        .properties(height=500)
    )

    st.altair_chart(county_chart, use_container_width=True)

with col2:
    st.markdown("**Top 10 Counties**")
    st.dataframe(
        county_df.nlargest(10, "total_sales")[
            ["county", "total_sales", "store_count", "top_category"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "county": st.column_config.TextColumn("County"),
            "total_sales": st.column_config.NumberColumn("Sales ($)", format="%.0f"),
            "store_count": st.column_config.NumberColumn("Stores", format="%d"),
            "top_category": st.column_config.TextColumn("Top Category"),
        },
    )

# --- Vendor Performance ---
st.subheader("Top Vendors")

col1, col2 = st.columns([2, 1])

with col1:
    top_vendors = vendor_df.nlargest(15, "total_sales")

    vendor_chart = (
        alt.Chart(top_vendors)
        .mark_bar()
        .encode(
            x=alt.X("total_sales:Q", title="Total Sales ($)", axis=alt.Axis(format="$,.0f")),
            y=alt.Y("vendor_name:N", title="Vendor", sort="-x"),
            color=alt.Color(
                "total_sales:Q",
                scale=alt.Scale(scheme="greens"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("vendor_name:N", title="Vendor"),
                alt.Tooltip("total_sales:Q", title="Sales", format="$,.0f"),
                alt.Tooltip("total_bottles:Q", title="Bottles", format=",d"),
                alt.Tooltip("product_count:Q", title="Products"),
                alt.Tooltip("store_count:Q", title="Stores"),
                alt.Tooltip("top_product:N", title="Top Product"),
            ],
        )
        .properties(height=400)
    )

    st.altair_chart(vendor_chart, use_container_width=True)

with col2:
    st.markdown("**Vendor Details**")
    st.dataframe(
        vendor_df.nlargest(10, "total_sales")[
            ["vendor_name", "total_sales", "product_count", "avg_bottle_price"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "vendor_name": st.column_config.TextColumn("Vendor"),
            "total_sales": st.column_config.NumberColumn("Sales ($)", format="%.0f"),
            "product_count": st.column_config.NumberColumn("Products", format="%d"),
            "avg_bottle_price": st.column_config.NumberColumn("Avg Price ($)", format="%.2f"),
        },
    )

# --- Category Distribution Pie ---
st.subheader("Sales Distribution by Category")

# Get top 8 categories plus "Other"
cat_totals = monthly_df.groupby("category_name")["total_sales"].sum().reset_index()
cat_totals = cat_totals.sort_values("total_sales", ascending=False)

top_8 = cat_totals.head(8).copy()
other_total = cat_totals.iloc[8:]["total_sales"].sum()
other_row = pd.DataFrame([{"category_name": "Other", "total_sales": other_total}])
pie_data = pd.concat([top_8, other_row], ignore_index=True)

pie_chart = (
    alt.Chart(pie_data)
    .mark_arc(innerRadius=50)
    .encode(
        theta=alt.Theta("total_sales:Q", stack=True),
        color=alt.Color(
            "category_name:N",
            title="Category",
            scale=alt.Scale(scheme="category10"),
        ),
        tooltip=[
            alt.Tooltip("category_name:N", title="Category"),
            alt.Tooltip("total_sales:Q", title="Sales", format="$,.0f"),
        ],
    )
    .properties(height=400)
)

st.altair_chart(pie_chart, use_container_width=True)

# --- Raw Data ---
st.subheader("Monthly Data")

# Format for display
display_monthly = monthly_df.copy()
display_monthly["sale_month"] = display_monthly["sale_month"].dt.strftime("%Y-%m")

st.dataframe(
    display_monthly,
    use_container_width=True,
    hide_index=True,
    column_config={
        "sale_month": st.column_config.TextColumn("Month"),
        "category_name": st.column_config.TextColumn("Category"),
        "total_sales": st.column_config.NumberColumn("Sales ($)", format="%.0f"),
        "total_bottles": st.column_config.NumberColumn("Bottles", format="%d"),
        "total_volume_liters": st.column_config.NumberColumn("Volume (L)", format="%.0f"),
        "transaction_count": st.column_config.NumberColumn("Transactions", format="%d"),
        "avg_bottle_price": st.column_config.NumberColumn("Avg Price ($)", format="%.2f"),
        "store_count": st.column_config.NumberColumn("Stores", format="%d"),
    },
)

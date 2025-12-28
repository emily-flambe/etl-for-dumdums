"""
Oura Wellness dashboard.
"""

import os
from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from data import load_oura_daily


def compute_period_stats(df: pd.DataFrame, current_start: pd.Timestamp, current_end: pd.Timestamp,
                         prior_start: pd.Timestamp, prior_end: pd.Timestamp) -> dict:
    """Compute average metrics for current and prior periods."""
    current = df[(df["day"] >= current_start) & (df["day"] <= current_end)]
    prior = df[(df["day"] >= prior_start) & (df["day"] <= prior_end)]

    metrics = [
        "sleep_score", "readiness_score", "activity_score", "steps",
        "resting_heart_rate", "total_sleep_hours", "average_hrv",
    ]
    result = {}

    for metric in metrics:
        # Skip if column doesn't exist (for backwards compatibility)
        if metric not in df.columns:
            result[metric] = {
                "current": None, "prior": None, "change": None,
                "pct_change": None, "current_days": 0, "prior_days": 0,
            }
            continue

        curr_avg = current[metric].mean() if len(current) > 0 else None
        prior_avg = prior[metric].mean() if len(prior) > 0 else None

        if pd.notna(curr_avg) and pd.notna(prior_avg) and prior_avg != 0:
            pct_change = ((curr_avg - prior_avg) / prior_avg) * 100
        else:
            pct_change = None

        result[metric] = {
            "current": curr_avg,
            "prior": prior_avg,
            "change": curr_avg - prior_avg if pd.notna(curr_avg) and pd.notna(prior_avg) else None,
            "pct_change": pct_change,
            "current_days": len(current[current[metric].notna()]),
            "prior_days": len(prior[prior[metric].notna()]),
        }

    return result


def format_delta(value: float | None, is_pct: bool = False, higher_is_better: bool = True) -> str:
    """Format a delta value with sign."""
    if value is None:
        return "N/A"
    sign = "+" if value > 0 else ""
    if is_pct:
        return f"{sign}{value:.1f}%"
    return f"{sign}{value:.1f}"


def get_delta_color(value: float | None, higher_is_better: bool = True) -> str:
    """Get color for delta based on direction."""
    if value is None or value == 0:
        return "off"
    if higher_is_better:
        return "normal" if value > 0 else "inverse"
    return "inverse" if value > 0 else "normal"

load_dotenv()

# Password protection for public deployment
DEPLOYMENT_MODE = os.environ.get("DEPLOYMENT_MODE", "local")
OURA_PAGE_PASSWORD = os.environ.get("OURA_PAGE_PASSWORD", "")

def check_password():
    """Returns True if password is correct or not required."""
    if DEPLOYMENT_MODE != "public" or not OURA_PAGE_PASSWORD:
        return True

    if "oura_authenticated" not in st.session_state:
        st.session_state.oura_authenticated = False

    if st.session_state.oura_authenticated:
        return True

    st.title("Oura Wellness")
    st.info("This page contains personal health data and requires a password to access.")

    with st.form("password_form"):
        password = st.text_input("Enter password:", type="password", key="password_input")
        submit = st.form_submit_button("Submit")

        if submit:
            if password.strip() == OURA_PAGE_PASSWORD.strip():
                st.session_state.oura_authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")

    return False

if not check_password():
    st.stop()

st.title("Oura Wellness")

st.markdown("""
Personal health and wellness data from the [Oura Ring](https://ouraring.com/), a wearable device
that tracks sleep, activity, and readiness metrics using sensors that measure heart rate,
heart rate variability (HRV), body temperature, and movement.

**About the Data:**
- **Source:** [Oura API V2](https://cloud.ouraring.com/v2/docs) via Personal Access Token
- **Metrics:** Sleep scores, readiness scores, activity scores, steps, calories, and more
- **Update Frequency:** Synced daily from personal Oura account

**Score Categories:**
- **Sleep Score:** Overall sleep quality (0-100) based on sleep stages, timing, and efficiency
- **Readiness Score:** How recovered your body is and ready for strain (0-100)
- **Activity Score:** Daily movement and exercise levels (0-100)

*Note: This is personal data from a single Oura Ring user, not aggregated population data.*
""")

# Load data
df = load_oura_daily()

# Convert date column to proper datetime for Altair compatibility
df["day"] = pd.to_datetime(df["day"])

# Convert nullable Int64 columns to float for Altair compatibility
int_cols = [
    "sleep_score", "readiness_score", "activity_score", "steps",
    "active_calories", "total_calories", "walking_distance_meters",
    "resting_heart_rate", "average_hrv", "sleep_efficiency",
]
float_cols = ["total_sleep_hours", "average_heart_rate"]

for col in int_cols + float_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Filter options
min_date = df["day"].min()
max_date = df["day"].max()
default_start = max(min_date, max_date - timedelta(days=30))
sleep_categories = ["excellent", "good", "fair", "poor"]
readiness_categories = ["optimal", "good", "fair", "poor"]

# Filters section
with st.expander("Filters", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        date_range = st.date_input(
            "Date range",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    with col2:
        selected_sleep_cats = st.pills(
            "Sleep Category", sleep_categories, selection_mode="multi"
        )
    with col3:
        selected_readiness_cats = st.pills(
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

# Trend Charts with Period-over-Period Comparisons
st.subheader("Trends")

# Chart configuration controls
cfg_col1, cfg_col2, cfg_col3 = st.columns(3)
with cfg_col1:
    smoothing = st.selectbox(
        "Data Smoothing",
        ["Daily", "7-day MA", "30-day MA"],
        index=0,
        help="How to smooth the data points",
    )
with cfg_col2:
    tick_interval = st.selectbox(
        "Tick Interval",
        ["Day", "Week", "Month"],
        index=0,
        help="Granularity of data points on x-axis",
    )
with cfg_col3:
    comparison = st.selectbox(
        "Comparison Period",
        ["Week over Week", "Month over Month", "Year over Year"],
        index=0,
        help="What period the change bars compare against",
    )

# Prepare data based on settings
df_trends = df.sort_values("day").copy()

# Apply smoothing
smoothing_window = {"Daily": 1, "7-day MA": 7, "30-day MA": 30}[smoothing]
metrics_config = [
    {"col": "resting_heart_rate", "label": "Resting HR", "unit": "bpm", "color": "#ef4444", "higher_better": False},
    {"col": "sleep_score", "label": "Sleep Quality", "unit": "", "color": "#6366f1", "higher_better": True},
    {"col": "readiness_score", "label": "Readiness", "unit": "", "color": "#22c55e", "higher_better": True},
    {"col": "activity_score", "label": "Activity", "unit": "", "color": "#f59e0b", "higher_better": True},
]

for m in metrics_config:
    col = m["col"]
    if col in df_trends.columns:
        if smoothing_window > 1:
            df_trends[f"{col}_smooth"] = df_trends[col].rolling(window=smoothing_window, min_periods=1).mean()
        else:
            df_trends[f"{col}_smooth"] = df_trends[col]

# Aggregate by tick interval
if tick_interval == "Week":
    df_trends["tick"] = df_trends["day"].dt.to_period("W").dt.start_time
elif tick_interval == "Month":
    df_trends["tick"] = df_trends["day"].dt.to_period("M").dt.start_time
else:
    df_trends["tick"] = df_trends["day"]

# Aggregate to tick level
agg_cols = {f"{m['col']}_smooth": "mean" for m in metrics_config if f"{m['col']}_smooth" in df_trends.columns}
df_agg = df_trends.groupby("tick").agg(agg_cols).reset_index()

# Calculate period-over-period change
comparison_shift = {"Week over Week": 7, "Month over Month": 30, "Year over Year": 365}[comparison]
if tick_interval == "Week":
    shift_periods = comparison_shift // 7
elif tick_interval == "Month":
    shift_periods = comparison_shift // 30
else:
    shift_periods = comparison_shift

for m in metrics_config:
    col = m["col"]
    smooth_col = f"{col}_smooth"
    if smooth_col in df_agg.columns:
        df_agg[f"{col}_change"] = df_agg[smooth_col] - df_agg[smooth_col].shift(shift_periods)

# Limit to recent data for display
max_ticks = {"Day": 30, "Week": 12, "Month": 12}[tick_interval]
total_rows = len(df_agg)

# Check if we have enough data for the selected comparison
has_enough_data = total_rows > shift_periods

if comparison == "Year over Year" and not has_enough_data:
    st.warning(f"Not enough historical data for Year over Year comparison. Need more than {shift_periods} {tick_interval.lower()}s of data (have {total_rows}). Try switching to Week over Week or Month over Month.")
    # Fall back to showing recent data without change bars
    df_chart = df_agg.tail(max_ticks).copy()
else:
    df_chart = df_agg.tail(max_ticks).copy()

# Format tick labels
if tick_interval == "Month":
    df_chart["tick_label"] = df_chart["tick"].dt.strftime("%b %Y")
else:
    df_chart["tick_label"] = df_chart["tick"].dt.strftime("%b %d")

tick_order = df_chart["tick_label"].tolist()


def create_metric_chart(metric_config: dict, data: pd.DataFrame, tick_order: list) -> alt.Chart | None:
    """Create a combined line + bar chart for a single metric."""
    col = metric_config["col"]
    smooth_col = f"{col}_smooth"
    change_col = f"{col}_change"
    label = metric_config["label"]
    unit = metric_config["unit"]

    if smooth_col not in data.columns or data[smooth_col].isna().all():
        return None

    chart_data = data[["tick_label", smooth_col, change_col]].copy()
    chart_data = chart_data.dropna(subset=[smooth_col])
    if len(chart_data) == 0:
        return None

    # Rename columns for cleaner display
    chart_data = chart_data.rename(columns={smooth_col: "value", change_col: "change"})

    # Line for the metric value
    line = alt.Chart(chart_data).mark_line(
        color=metric_config["color"],
        strokeWidth=2,
    ).encode(
        x=alt.X("tick_label:N", sort=tick_order, axis=alt.Axis(labelAngle=-45, title=None)),
        y=alt.Y("value:Q", title=f"{label} ({unit})" if unit else label, scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("tick_label:N", title="Date"),
            alt.Tooltip("value:Q", title=label, format=".1f"),
        ],
    )

    # Points on the line
    points = alt.Chart(chart_data).mark_point(
        color=metric_config["color"],
        size=30,
    ).encode(
        x=alt.X("tick_label:N", sort=tick_order),
        y=alt.Y("value:Q"),
    )

    # Bars for period change (only where we have change data)
    bar_data = chart_data.dropna(subset=["change"]).copy()
    if len(bar_data) > 0:
        # Determine bar color based on whether higher is better
        if metric_config["higher_better"]:
            bar_data["bar_color"] = bar_data["change"].apply(lambda x: "#22c55e" if x > 0 else "#ef4444")
        else:
            bar_data["bar_color"] = bar_data["change"].apply(lambda x: "#ef4444" if x > 0 else "#22c55e")

        # Calculate bar width based on number of data points to avoid overlap
        num_points = len(bar_data)
        bar_width = max(2, min(12, 400 // num_points))  # Scale width: more points = thinner bars

        bars = alt.Chart(bar_data).mark_bar(opacity=0.4, width=bar_width).encode(
            x=alt.X("tick_label:N", sort=tick_order, axis=alt.Axis(labels=False, title=None)),
            y=alt.Y("change:Q", title="Change"),
            y2=alt.datum(0),
            color=alt.Color("bar_color:N", scale=None),
            tooltip=[
                alt.Tooltip("tick_label:N", title="Date"),
                alt.Tooltip("change:Q", title="Change", format="+.1f"),
            ],
        )

        # Stack vertically: line chart on top, bar chart below
        line_chart = alt.layer(line, points).properties(height=120)
        bar_chart = bars.properties(height=60)

        combined = alt.vconcat(line_chart, bar_chart, spacing=0).properties(
            title=f"{label} ({unit})" if unit else label,
        )
    else:
        combined = alt.layer(line, points).properties(
            height=180,
            title=f"{label} ({unit})" if unit else label,
        )

    return combined


# Render charts in rows of 2
available_metrics = [m for m in metrics_config if f"{m['col']}_smooth" in df_chart.columns and df_chart[f"{m['col']}_smooth"].notna().any()]

# Row 1: Resting HR, Sleep Quality
row1_metrics = [m for m in available_metrics if m["col"] in ["resting_heart_rate", "sleep_score"]]
if row1_metrics:
    cols = st.columns(2)
    for i, m in enumerate(row1_metrics):
        chart = create_metric_chart(m, df_chart, tick_order)
        if chart:
            cols[i].altair_chart(chart, use_container_width=True)

# Row 2: Readiness, Activity
row2_metrics = [m for m in available_metrics if m["col"] in ["readiness_score", "activity_score"]]
if row2_metrics:
    cols = st.columns(2)
    for i, m in enumerate(row2_metrics):
        chart = create_metric_chart(m, df_chart, tick_order)
        if chart:
            cols[i].altair_chart(chart, use_container_width=True)

# Show info if health metrics are missing
if "resting_heart_rate" not in [m["col"] for m in available_metrics]:
    st.info("Resting HR requires syncing sleep session data. Run `make sync-oura FULL=1` to populate.")

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
    x=alt.X("day:T", title="Date", axis=alt.Axis(format="%b %d", values=filtered["day"].tolist())),
    y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 100])),
    color=alt.Color("Metric:N", scale=alt.Scale(
        domain=["Sleep", "Readiness", "Activity"],
        range=["#6366f1", "#22c55e", "#f59e0b"]
    )),
    tooltip=[alt.Tooltip("day:T", format="%b %d, %Y"), "Metric:N", "Score:Q"],
).properties(height=300)

st.altair_chart(line_chart, use_container_width=True)

# Steps over time
st.subheader("Daily Steps")

# Add color category for steps
filtered["steps_color"] = filtered["steps"].apply(
    lambda x: "10k+" if x >= 10000 else ("7.5k+" if x >= 7500 else "<7.5k")
)

steps_chart = alt.Chart(filtered).mark_bar().encode(
    x=alt.X("day:T", title="Date", axis=alt.Axis(format="%b %d", values=filtered["day"].tolist())),
    y=alt.Y("steps:Q", title="Steps"),
    color=alt.Color("steps_color:N", scale=alt.Scale(
        domain=["10k+", "7.5k+", "<7.5k"],
        range=["#22c55e", "#f59e0b", "#ef4444"]
    ), legend=alt.Legend(title="Steps")),
    tooltip=[alt.Tooltip("day:T", format="%b %d, %Y"), "steps:Q", "activity_category:N"],
).properties(height=250)

# Add 10k goal line
goal_line = alt.Chart(pd.DataFrame({"y": [10000]})).mark_rule(
    strokeDash=[5, 5], color="gray"
).encode(y="y:Q")

st.altair_chart(steps_chart + goal_line, use_container_width=True)

# Body temperature deviation chart
st.subheader("Body Temperature Deviation")

temp_data = filtered[filtered["temperature_deviation"].notna()].copy()
if len(temp_data) > 0:
    # Add color category for temperature
    temp_data["temp_color"] = temp_data["temperature_deviation"].apply(
        lambda x: "elevated" if x > 0.5 else ("warm" if x > 0 else ("cool" if x > -0.5 else "low"))
    )

    temp_chart = alt.Chart(temp_data).mark_bar().encode(
        x=alt.X("day:T", title="Date", axis=alt.Axis(format="%b %d", values=temp_data["day"].tolist())),
        y=alt.Y("temperature_deviation:Q", title="Deviation from Baseline (C)"),
        color=alt.Color("temp_color:N", scale=alt.Scale(
            domain=["elevated", "warm", "cool", "low"],
            range=["#ef4444", "#f59e0b", "#3b82f6", "#6366f1"]
        ), legend=alt.Legend(title="Temp")),
        tooltip=[
            alt.Tooltip("day:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("temperature_deviation:Q", title="Deviation", format="+.2f"),
        ],
    ).properties(height=200)

    # Add baseline at 0
    baseline = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
        strokeDash=[5, 5], color="gray"
    ).encode(y="y:Q")

    st.altair_chart(temp_chart + baseline, use_container_width=True)
else:
    st.info("No temperature data available for selected period")

# Distribution charts
st.subheader("Score Distributions")
col1, col2, col3 = st.columns(3)

with col1:
    st.write("**Sleep Categories**")
    sleep_dist = filtered["sleep_category"].value_counts().reset_index()
    sleep_dist.columns = ["Category", "Count"]
    sleep_chart = alt.Chart(sleep_dist).mark_bar().encode(
        x=alt.X("Count:Q", title="Days"),
        y=alt.Y("Category:N", sort=["excellent", "good", "fair", "poor"], title=None),
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["excellent", "good", "fair", "poor"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        ), legend=None),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=150)
    st.altair_chart(sleep_chart, use_container_width=True)

with col2:
    st.write("**Readiness Categories**")
    readiness_dist = filtered["readiness_category"].value_counts().reset_index()
    readiness_dist.columns = ["Category", "Count"]
    readiness_chart = alt.Chart(readiness_dist).mark_bar().encode(
        x=alt.X("Count:Q", title="Days"),
        y=alt.Y("Category:N", sort=["optimal", "good", "fair", "poor"], title=None),
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["optimal", "good", "fair", "poor"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        ), legend=None),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=150)
    st.altair_chart(readiness_chart, use_container_width=True)

with col3:
    st.write("**Activity Categories**")
    activity_dist = filtered["activity_category"].value_counts().reset_index()
    activity_dist.columns = ["Category", "Count"]
    activity_chart = alt.Chart(activity_dist).mark_bar().encode(
        x=alt.X("Count:Q", title="Days"),
        y=alt.Y("Category:N", sort=["very_active", "active", "moderate", "sedentary"], title=None),
        color=alt.Color("Category:N", scale=alt.Scale(
            domain=["very_active", "active", "moderate", "sedentary"],
            range=["#22c55e", "#84cc16", "#f59e0b", "#ef4444"]
        ), legend=None),
        tooltip=["Category:N", "Count:Q"],
    ).properties(height=150)
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

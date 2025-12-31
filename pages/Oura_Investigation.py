"""
Oura Data Investigation - Time-based pattern analysis.

Explores 4+ years of Oura wellness data looking for genuine behavioral patterns:
- Day of week patterns
- Seasonal cycles
- Year-over-year trends
- Weekend vs weekday behavior
- Weekly rhythm (autocorrelation)
"""

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats

from data import load_oura_daily

st.set_page_config(layout="wide")

st.title("Oura Data Investigation")
st.markdown("**5 time-based patterns explored across 4+ years of personal wellness data.**")

# Load data
df = load_oura_daily()
df["day"] = pd.to_datetime(df["day"])
df["day_of_week"] = df["day"].dt.day_name()
df["dow_num"] = df["day"].dt.dayofweek
df["month"] = df["day"].dt.month
df["month_name"] = df["day"].dt.strftime("%b")
df["year"] = df["day"].dt.year
df["is_weekend"] = df["dow_num"] >= 5

# Filter to valid data
df = df[df["sleep_score"].notna()]

st.markdown(f"""
### Data Overview
- **Date Range:** {df['day'].min().date()} to {df['day'].max().date()}
- **Total Days:** {len(df):,}
- **Years of Data:** {df['year'].nunique()}
""")

# =============================================================================
# 1. Day of Week Patterns
# =============================================================================
st.markdown("---")
st.header("1. Day of Week Patterns")
st.markdown("**Do sleep and activity vary by day of the week?**")

dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

dow_stats = df.groupby("day_of_week").agg({
    "total_sleep_hours": ["mean", "std"],
    "steps": ["mean", "std"],
}).round(2)
dow_stats.columns = ["_".join(col) for col in dow_stats.columns]
dow_stats = dow_stats.reindex(dow_order).reset_index()

col1, col2 = st.columns(2)

with col1:
    chart_sleep_dow = (
        alt.Chart(dow_stats)
        .mark_bar(color="#6366f1")
        .encode(
            x=alt.X("day_of_week:N", sort=dow_order, title="Day of Week"),
            y=alt.Y("total_sleep_hours_mean:Q", title="Avg Sleep (hrs)", scale=alt.Scale(zero=False)),
            tooltip=["day_of_week:N", alt.Tooltip("total_sleep_hours_mean:Q", format=".2f", title="Avg Hours")],
        )
        .properties(height=300, title="Sleep Hours by Day")
    )
    st.altair_chart(chart_sleep_dow, use_container_width=True)

with col2:
    chart_steps_dow = (
        alt.Chart(dow_stats)
        .mark_bar(color="#f97316")
        .encode(
            x=alt.X("day_of_week:N", sort=dow_order, title="Day of Week"),
            y=alt.Y("steps_mean:Q", title="Average Steps"),
            tooltip=["day_of_week:N", alt.Tooltip("steps_mean:Q", format=",.0f", title="Avg Steps")],
        )
        .properties(height=300, title="Steps by Day")
    )
    st.altair_chart(chart_steps_dow, use_container_width=True)

# ANOVA tests
groups_sleep = [df[df["day_of_week"] == d]["total_sleep_hours"].dropna() for d in dow_order]
f_sleep, p_sleep = stats.f_oneway(*groups_sleep)
groups_steps = [df[df["day_of_week"] == d]["steps"].dropna() for d in dow_order]
f_steps, p_steps = stats.f_oneway(*groups_steps)

best_sleep_day = dow_stats.loc[dow_stats["total_sleep_hours_mean"].idxmax(), "day_of_week"]
worst_sleep_day = dow_stats.loc[dow_stats["total_sleep_hours_mean"].idxmin(), "day_of_week"]
best_steps_day = dow_stats.loc[dow_stats["steps_mean"].idxmax(), "day_of_week"]
worst_steps_day = dow_stats.loc[dow_stats["steps_mean"].idxmin(), "day_of_week"]

st.markdown(f"""
**Statistical Tests (ANOVA):**
- Sleep varies by day: F={f_sleep:.1f}, p={p_sleep:.4f} {'✓ Significant!' if p_sleep < 0.05 else ''}
- Steps vary by day: F={f_steps:.1f}, p={p_steps:.4f} {'✓ Significant!' if p_steps < 0.05 else ''}

**Findings:** Best sleep on **{best_sleep_day}**, worst on **{worst_sleep_day}**.
Most active on **{best_steps_day}**, least on **{worst_steps_day}**.
""")

# =============================================================================
# 2. Seasonal Cycles
# =============================================================================
st.markdown("---")
st.header("2. Seasonal Cycles")
st.markdown("**Do metrics follow predictable seasonal patterns?**")

month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

monthly = df.groupby("month").agg({
    "total_sleep_hours": "mean",
    "steps": "mean",
    "resting_heart_rate": "mean",
}).reset_index()
monthly["month_name"] = monthly["month"].apply(lambda x: month_order[x-1])

col1, col2 = st.columns(2)

with col1:
    chart_sleep_month = (
        alt.Chart(monthly)
        .mark_line(strokeWidth=3, color="#6366f1", point=True)
        .encode(
            x=alt.X("month_name:N", sort=month_order, title="Month"),
            y=alt.Y("total_sleep_hours:Q", title="Avg Sleep (hrs)", scale=alt.Scale(zero=False)),
            tooltip=["month_name:N", alt.Tooltip("total_sleep_hours:Q", format=".2f")],
        )
        .properties(height=300, title="Sleep by Month")
    )
    st.altair_chart(chart_sleep_month, use_container_width=True)

with col2:
    chart_steps_month = (
        alt.Chart(monthly)
        .mark_line(strokeWidth=3, color="#f97316", point=True)
        .encode(
            x=alt.X("month_name:N", sort=month_order, title="Month"),
            y=alt.Y("steps:Q", title="Avg Steps"),
            tooltip=["month_name:N", alt.Tooltip("steps:Q", format=",.0f")],
        )
        .properties(height=300, title="Steps by Month")
    )
    st.altair_chart(chart_steps_month, use_container_width=True)

# ANOVA for seasonal
groups_monthly = [df[df["month"] == m]["total_sleep_hours"].dropna() for m in range(1, 13)]
f_month, p_month = stats.f_oneway(*[g for g in groups_monthly if len(g) > 10])

best_sleep_month = monthly.loc[monthly["total_sleep_hours"].idxmax(), "month_name"]
worst_sleep_month = monthly.loc[monthly["total_sleep_hours"].idxmin(), "month_name"]
best_steps_month = monthly.loc[monthly["steps"].idxmax(), "month_name"]
worst_steps_month = monthly.loc[monthly["steps"].idxmin(), "month_name"]

st.markdown(f"""
**Seasonal Effect:** F={f_month:.1f}, p={p_month:.4f} {'✓ Significant!' if p_month < 0.05 else ''}

**Findings:** Best sleep in **{best_sleep_month}**, worst in **{worst_sleep_month}**.
Most active in **{best_steps_month}**, least in **{worst_steps_month}**.
""")

# =============================================================================
# 3. Year-over-Year Trends
# =============================================================================
st.markdown("---")
st.header("3. Year-over-Year Trends")
st.markdown("**Are things getting better or worse over time?**")

yearly = df.groupby("year").agg({
    "total_sleep_hours": ["mean", "count"],
    "steps": "mean",
    "resting_heart_rate": "mean",
}).round(2)
yearly.columns = ["_".join(col) if col[1] else col[0] for col in yearly.columns]
yearly = yearly.reset_index()

# Normalize to first year
first_year = yearly["year"].min()
baseline = yearly[yearly["year"] == first_year].iloc[0]
yearly["sleep_pct"] = (yearly["total_sleep_hours_mean"] / baseline["total_sleep_hours_mean"]) * 100
yearly["steps_pct"] = (yearly["steps_mean"] / baseline["steps_mean"]) * 100

yearly_long = pd.melt(
    yearly[["year", "sleep_pct", "steps_pct"]],
    id_vars=["year"],
    var_name="metric",
    value_name="pct"
)
yearly_long["metric"] = yearly_long["metric"].map({"sleep_pct": "Sleep Hours", "steps_pct": "Steps"})

chart_yoy = (
    alt.Chart(yearly_long)
    .mark_line(strokeWidth=3, point=True)
    .encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("pct:Q", title=f"% of {first_year} Baseline", scale=alt.Scale(domain=[60, 120])),
        color=alt.Color("metric:N", scale=alt.Scale(domain=["Sleep Hours", "Steps"], range=["#6366f1", "#f97316"])),
        tooltip=["year:O", "metric:N", alt.Tooltip("pct:Q", format=".1f")],
    )
    .properties(height=350)
)

rule = alt.Chart(pd.DataFrame({"y": [100]})).mark_rule(strokeDash=[5,5], color="gray").encode(y="y:Q")

st.altair_chart(chart_yoy + rule, use_container_width=True)

# Trend analysis
years = yearly["year"].values
sleep_vals = yearly["total_sleep_hours_mean"].values
steps_vals = yearly["steps_mean"].values

if len(years) > 2:
    sleep_slope, _, _, sleep_p, _ = stats.linregress(years, sleep_vals)
    steps_slope, _, _, steps_p, _ = stats.linregress(years, steps_vals)
else:
    sleep_slope = steps_slope = 0
    sleep_p = steps_p = 1

st.markdown(f"""
**Trend Analysis:**
- Sleep: {sleep_slope*10:+.2f} hours per decade (p={sleep_p:.3f})
- Steps: {steps_slope*10:+,.0f} steps per decade (p={steps_p:.3f})
""")

st.dataframe(
    yearly[["year", "total_sleep_hours_mean", "steps_mean", "total_sleep_hours_count"]].rename(
        columns={"year": "Year", "total_sleep_hours_mean": "Sleep (hrs)", "steps_mean": "Steps", "total_sleep_hours_count": "Days"}
    ),
    hide_index=True
)

# =============================================================================
# 4. Weekend vs Weekday
# =============================================================================
st.markdown("---")
st.header("4. Weekend vs Weekday")
st.markdown("**Do you behave differently on weekends?**")

weekend_stats = df.groupby("is_weekend").agg({
    "total_sleep_hours": "mean",
    "steps": "mean",
}).reset_index()
weekend_stats["type"] = weekend_stats["is_weekend"].map({False: "Weekday", True: "Weekend"})

# T-tests
weekday_sleep = df[~df["is_weekend"]]["total_sleep_hours"].dropna()
weekend_sleep = df[df["is_weekend"]]["total_sleep_hours"].dropna()
_, p_sleep_wk = stats.ttest_ind(weekday_sleep, weekend_sleep)

weekday_steps = df[~df["is_weekend"]]["steps"].dropna()
weekend_steps = df[df["is_weekend"]]["steps"].dropna()
_, p_steps_wk = stats.ttest_ind(weekday_steps, weekend_steps)

sleep_diff = weekend_stats[weekend_stats["type"]=="Weekend"]["total_sleep_hours"].values[0] - \
             weekend_stats[weekend_stats["type"]=="Weekday"]["total_sleep_hours"].values[0]
steps_diff = weekend_stats[weekend_stats["type"]=="Weekend"]["steps"].values[0] - \
             weekend_stats[weekend_stats["type"]=="Weekday"]["steps"].values[0]

col1, col2 = st.columns(2)

with col1:
    chart_wk_sleep = (
        alt.Chart(weekend_stats)
        .mark_bar()
        .encode(
            x=alt.X("type:N", title=""),
            y=alt.Y("total_sleep_hours:Q", title="Avg Sleep (hrs)", scale=alt.Scale(zero=False)),
            color=alt.Color("type:N", scale=alt.Scale(domain=["Weekday", "Weekend"], range=["#64748b", "#22c55e"])),
            tooltip=["type:N", alt.Tooltip("total_sleep_hours:Q", format=".2f")],
        )
        .properties(height=300, title="Sleep: Weekend vs Weekday")
    )
    st.altair_chart(chart_wk_sleep, use_container_width=True)

with col2:
    chart_wk_steps = (
        alt.Chart(weekend_stats)
        .mark_bar()
        .encode(
            x=alt.X("type:N", title=""),
            y=alt.Y("steps:Q", title="Avg Steps"),
            color=alt.Color("type:N", scale=alt.Scale(domain=["Weekday", "Weekend"], range=["#64748b", "#22c55e"])),
            tooltip=["type:N", alt.Tooltip("steps:Q", format=",.0f")],
        )
        .properties(height=300, title="Steps: Weekend vs Weekday")
    )
    st.altair_chart(chart_wk_steps, use_container_width=True)

st.markdown(f"""
**Differences:**
- Sleep: {sleep_diff:+.2f} hours on weekends ({'+' if sleep_diff > 0 else ''}{sleep_diff*60:.0f} min) - {'Significant!' if p_sleep_wk < 0.05 else 'Not significant'}
- Steps: {steps_diff:+,.0f} on weekends - {'Significant!' if p_steps_wk < 0.05 else 'Not significant'}
""")

# =============================================================================
# 5. Weekly Rhythm (Autocorrelation)
# =============================================================================
st.markdown("---")
st.header("5. Weekly Rhythm (Autocorrelation)")
st.markdown("**Is there a predictable 7-day cycle?**")

def calc_autocorr(series, max_lag=7):
    result = []
    for lag in range(1, max_lag + 1):
        shifted = series.shift(lag)
        valid = series.notna() & shifted.notna()
        if valid.sum() > 30:
            corr = series[valid].corr(shifted[valid])
            result.append({"lag": lag, "autocorrelation": corr})
    return pd.DataFrame(result)

sleep_acf = calc_autocorr(df["total_sleep_hours"])
sleep_acf["metric"] = "Sleep Hours"
steps_acf = calc_autocorr(df["steps"])
steps_acf["metric"] = "Steps"
acf_data = pd.concat([sleep_acf, steps_acf])

chart_acf = (
    alt.Chart(acf_data)
    .mark_bar()
    .encode(
        x=alt.X("lag:O", title="Lag (days)"),
        y=alt.Y("autocorrelation:Q", title="Autocorrelation", scale=alt.Scale(domain=[-0.1, 0.5])),
        color=alt.Color("metric:N", scale=alt.Scale(domain=["Sleep Hours", "Steps"], range=["#6366f1", "#f97316"])),
        xOffset="metric:N",
        tooltip=["lag:O", "metric:N", alt.Tooltip("autocorrelation:Q", format=".3f")],
    )
    .properties(height=350, title="Autocorrelation by Lag")
)

st.altair_chart(chart_acf, use_container_width=True)

sig_level = 1.96 / np.sqrt(len(df))
sleep_lag7 = sleep_acf[sleep_acf["lag"] == 7]["autocorrelation"].values[0] if len(sleep_acf[sleep_acf["lag"] == 7]) > 0 else 0
steps_lag7 = steps_acf[steps_acf["lag"] == 7]["autocorrelation"].values[0] if len(steps_acf[steps_acf["lag"] == 7]) > 0 else 0

st.markdown(f"""
**7-Day Cycle Detection** (significance threshold: {sig_level:.3f})
- Sleep lag-7 correlation: {sleep_lag7:.3f} {'✓ Weekly cycle!' if sleep_lag7 > sig_level else ''}
- Steps lag-7 correlation: {steps_lag7:.3f} {'✓ Weekly cycle!' if steps_lag7 > sig_level else ''}

*Higher bars at lag 7 indicate you tend to behave similarly on the same day each week.*
""")

# =============================================================================
# Summary
# =============================================================================
st.markdown("---")
st.header("Summary")

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"""
    **Day of Week**
    - Best sleep: **{best_sleep_day}**
    - Most active: **{best_steps_day}**

    **Seasonal**
    - Best sleep month: **{best_sleep_month}**
    - Most active month: **{best_steps_month}**
    """)

with col2:
    st.markdown(f"""
    **Weekend vs Weekday**
    - Sleep difference: **{sleep_diff:+.2f} hrs**
    - Steps difference: **{steps_diff:+,.0f}**

    **Weekly Rhythm**
    - Sleep: {"Has weekly cycle" if sleep_lag7 > sig_level else "No weekly cycle"}
    - Steps: {"Has weekly cycle" if steps_lag7 > sig_level else "No weekly cycle"}
    """)

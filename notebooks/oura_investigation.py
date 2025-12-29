# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "google-cloud-bigquery>=3.0.0",
#     "db-dtypes>=1.2.0",
#     "altair>=5.0.0",
#     "scipy>=1.14.0",
#     "python-dotenv>=1.0.0",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Oura Wellness Data Investigation

        **4+ years of personal health data, exploring time-based patterns.**

        This notebook analyzes sleep, recovery, and activity data collected from an Oura Ring,
        looking for genuine behavioral patterns rather than reverse-engineering Oura's algorithms.

        ## Questions We're Asking
        1. Are there day-of-week patterns? (Weekend vs weekday differences)
        2. Do metrics follow seasonal cycles?
        3. How have things changed year-over-year?
        4. Is there a predictable weekly rhythm?
        """
    )
    return


@app.cell
def _():
    import os
    import pandas as pd
    import altair as alt
    from scipy import stats
    import numpy as np
    from google.cloud import bigquery
    from dotenv import load_dotenv

    load_dotenv()

    alt.data_transformers.disable_max_rows()
    return alt, bigquery, load_dotenv, np, os, pd, stats


@app.cell
def _(bigquery, os, pd):
    # Load data from BigQuery
    credentials_path = os.getenv("GCP_SA_KEY_FILE", ".secrets/credentials.json")
    project_id = os.getenv("GCP_PROJECT_ID")

    client = bigquery.Client.from_service_account_json(
        credentials_path, project=project_id
    )

    query = """
    SELECT
        day,
        sleep_score,
        readiness_score,
        activity_score,
        total_sleep_hours,
        deep_sleep_hours,
        rem_sleep_hours,
        resting_heart_rate,
        average_hrv,
        temperature_deviation,
        steps,
        active_calories,
        high_activity_time_minutes,
        medium_activity_time_minutes,
        low_activity_time_minutes,
        sedentary_time_minutes
    FROM `oura.fct_oura_daily`
    WHERE sleep_score IS NOT NULL
    ORDER BY day
    """

    df = client.query(query).to_dataframe()
    df["day"] = pd.to_datetime(df["day"])
    df["day_of_week"] = df["day"].dt.day_name()
    df["dow_num"] = df["day"].dt.dayofweek  # 0=Monday, 6=Sunday
    df["month"] = df["day"].dt.month
    df["month_name"] = df["day"].dt.strftime("%b")
    df["year"] = df["day"].dt.year
    df["week_of_year"] = df["day"].dt.isocalendar().week
    df["is_weekend"] = df["dow_num"] >= 5

    print(f"Loaded {len(df)} days of data from {df['day'].min().date()} to {df['day'].max().date()}")
    return client, credentials_path, df, project_id, query


@app.cell
def _(df, mo):
    mo.md(
        f"""
        ## Data Overview

        | Metric | Value |
        |--------|-------|
        | **Date Range** | {df['day'].min().date()} to {df['day'].max().date()} |
        | **Total Days** | {len(df):,} |
        | **Years of Data** | {df['year'].nunique()} |
        | **Avg Sleep Hours** | {df['total_sleep_hours'].mean():.1f} |
        | **Avg Steps** | {df['steps'].mean():,.0f} |
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## 1. Day of Week Patterns

        **Question:** Do sleep, activity, and recovery vary by day of the week?
        Are there "Sunday Scaries" or "Monday Blues" in the data?
        """
    )
    return


@app.cell
def _(alt, df, mo, pd, stats):
    # Day of week analysis
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    dow_stats = df.groupby("day_of_week").agg({
        "sleep_score": ["mean", "std"],
        "total_sleep_hours": ["mean", "std"],
        "steps": ["mean", "std"],
        "readiness_score": ["mean", "std"],
    }).round(2)

    dow_stats.columns = ["_".join(col) for col in dow_stats.columns]
    dow_stats = dow_stats.reindex(dow_order).reset_index()

    # Sleep hours by day
    chart_sleep_dow = (
        alt.Chart(dow_stats)
        .mark_bar(color="#6366f1")
        .encode(
            x=alt.X("day_of_week:N", sort=dow_order, title="Day of Week"),
            y=alt.Y("total_sleep_hours_mean:Q", title="Average Sleep Hours", scale=alt.Scale(domain=[5.5, 7.5])),
            tooltip=[
                alt.Tooltip("day_of_week:N", title="Day"),
                alt.Tooltip("total_sleep_hours_mean:Q", title="Avg Hours", format=".2f"),
            ],
        )
        .properties(width=400, height=250, title="Sleep Hours by Day of Week")
    )

    # Steps by day
    chart_steps_dow = (
        alt.Chart(dow_stats)
        .mark_bar(color="#f97316")
        .encode(
            x=alt.X("day_of_week:N", sort=dow_order, title="Day of Week"),
            y=alt.Y("steps_mean:Q", title="Average Steps", scale=alt.Scale(domain=[10000, 22000])),
            tooltip=[
                alt.Tooltip("day_of_week:N", title="Day"),
                alt.Tooltip("steps_mean:Q", title="Avg Steps", format=",.0f"),
            ],
        )
        .properties(width=400, height=250, title="Steps by Day of Week")
    )

    # ANOVA test for day-of-week effect on sleep
    groups_sleep = [df[df["day_of_week"] == d]["total_sleep_hours"].dropna() for d in dow_order]
    f_sleep, p_sleep = stats.f_oneway(*groups_sleep)

    groups_steps = [df[df["day_of_week"] == d]["steps"].dropna() for d in dow_order]
    f_steps, p_steps = stats.f_oneway(*groups_steps)

    best_sleep_day = dow_stats.loc[dow_stats["total_sleep_hours_mean"].idxmax(), "day_of_week"]
    worst_sleep_day = dow_stats.loc[dow_stats["total_sleep_hours_mean"].idxmin(), "day_of_week"]
    best_steps_day = dow_stats.loc[dow_stats["steps_mean"].idxmax(), "day_of_week"]
    worst_steps_day = dow_stats.loc[dow_stats["steps_mean"].idxmin(), "day_of_week"]

    sleep_range = dow_stats["total_sleep_hours_mean"].max() - dow_stats["total_sleep_hours_mean"].min()
    steps_range = dow_stats["steps_mean"].max() - dow_stats["steps_mean"].min()

    mo.vstack([
        mo.hstack([chart_sleep_dow, chart_steps_dow]),
        mo.md(f"""
        **Statistical Tests:**
        - Sleep hours vary by day: F={f_sleep:.1f}, p={p_sleep:.4f} {"(significant!)" if p_sleep < 0.05 else "(not significant)"}
        - Steps vary by day: F={f_steps:.1f}, p={p_steps:.4f} {"(significant!)" if p_steps < 0.05 else "(not significant)"}

        **Key Findings:**
        - **Best sleep:** {best_sleep_day} ({dow_stats[dow_stats['day_of_week']==best_sleep_day]['total_sleep_hours_mean'].values[0]:.2f} hrs)
        - **Worst sleep:** {worst_sleep_day} ({dow_stats[dow_stats['day_of_week']==worst_sleep_day]['total_sleep_hours_mean'].values[0]:.2f} hrs)
        - **Sleep range:** {sleep_range:.2f} hours difference between best/worst days
        - **Most active:** {best_steps_day} ({dow_stats[dow_stats['day_of_week']==best_steps_day]['steps_mean'].values[0]:,.0f} steps)
        - **Least active:** {worst_steps_day} ({dow_stats[dow_stats['day_of_week']==worst_steps_day]['steps_mean'].values[0]:,.0f} steps)
        """),
    ])
    return (
        best_sleep_day,
        best_steps_day,
        chart_sleep_dow,
        chart_steps_dow,
        dow_order,
        dow_stats,
        f_sleep,
        f_steps,
        groups_sleep,
        groups_steps,
        p_sleep,
        p_steps,
        sleep_range,
        steps_range,
        worst_sleep_day,
        worst_steps_day,
    )


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## 2. Seasonal Cycles

        **Question:** Do metrics follow predictable seasonal patterns?
        Is sleep better in winter? Are you more active in summer?
        """
    )
    return


@app.cell
def _(alt, df, mo, stats):
    # Monthly analysis
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    monthly = df.groupby("month").agg({
        "total_sleep_hours": "mean",
        "sleep_score": "mean",
        "steps": "mean",
        "readiness_score": "mean",
        "resting_heart_rate": "mean",
        "average_hrv": "mean",
    }).reset_index()

    monthly["month_name"] = monthly["month"].apply(lambda x: month_order[x-1])

    # Sleep hours by month
    chart_sleep_month = (
        alt.Chart(monthly)
        .mark_line(strokeWidth=3, color="#6366f1", point=True)
        .encode(
            x=alt.X("month_name:N", sort=month_order, title="Month"),
            y=alt.Y("total_sleep_hours:Q", title="Average Sleep Hours", scale=alt.Scale(domain=[5.5, 7.5])),
            tooltip=[
                alt.Tooltip("month_name:N", title="Month"),
                alt.Tooltip("total_sleep_hours:Q", title="Avg Hours", format=".2f"),
            ],
        )
        .properties(width=500, height=250, title="Sleep Hours by Month")
    )

    # Steps by month
    chart_steps_month = (
        alt.Chart(monthly)
        .mark_line(strokeWidth=3, color="#f97316", point=True)
        .encode(
            x=alt.X("month_name:N", sort=month_order, title="Month"),
            y=alt.Y("steps:Q", title="Average Steps"),
            tooltip=[
                alt.Tooltip("month_name:N", title="Month"),
                alt.Tooltip("steps:Q", title="Avg Steps", format=",.0f"),
            ],
        )
        .properties(width=500, height=250, title="Steps by Month")
    )

    # Resting HR by month (fitness indicator)
    chart_hr_month = (
        alt.Chart(monthly)
        .mark_line(strokeWidth=3, color="#ef4444", point=True)
        .encode(
            x=alt.X("month_name:N", sort=month_order, title="Month"),
            y=alt.Y("resting_heart_rate:Q", title="Resting Heart Rate", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("month_name:N", title="Month"),
                alt.Tooltip("resting_heart_rate:Q", title="Resting HR", format=".1f"),
            ],
        )
        .properties(width=500, height=250, title="Resting Heart Rate by Month")
    )

    # ANOVA for seasonal effects
    groups_monthly = [df[df["month"] == m]["total_sleep_hours"].dropna() for m in range(1, 13)]
    f_month, p_month = stats.f_oneway(*[g for g in groups_monthly if len(g) > 10])

    best_sleep_month = monthly.loc[monthly["total_sleep_hours"].idxmax(), "month_name"]
    worst_sleep_month = monthly.loc[monthly["total_sleep_hours"].idxmin(), "month_name"]
    best_steps_month = monthly.loc[monthly["steps"].idxmax(), "month_name"]
    worst_steps_month = monthly.loc[monthly["steps"].idxmin(), "month_name"]

    mo.vstack([
        chart_sleep_month,
        chart_steps_month,
        chart_hr_month,
        mo.md(f"""
        **Seasonal Effect Test:** F={f_month:.1f}, p={p_month:.4f} {"- Seasonal patterns are statistically significant!" if p_month < 0.05 else "- No significant seasonal pattern"}

        **Sleep Patterns:**
        - Best month: **{best_sleep_month}** ({monthly[monthly['month_name']==best_sleep_month]['total_sleep_hours'].values[0]:.2f} hrs)
        - Worst month: **{worst_sleep_month}** ({monthly[monthly['month_name']==worst_sleep_month]['total_sleep_hours'].values[0]:.2f} hrs)

        **Activity Patterns:**
        - Most active: **{best_steps_month}** ({monthly[monthly['month_name']==best_steps_month]['steps'].values[0]:,.0f} steps)
        - Least active: **{worst_steps_month}** ({monthly[monthly['month_name']==worst_steps_month]['steps'].values[0]:,.0f} steps)

        **Interpretation:**
        Look for patterns - do you sleep more in winter months? Are you more active in summer?
        Does resting heart rate track with fitness/activity levels?
        """),
    ])
    return (
        best_sleep_month,
        best_steps_month,
        chart_hr_month,
        chart_sleep_month,
        chart_steps_month,
        f_month,
        groups_monthly,
        month_order,
        monthly,
        p_month,
        worst_sleep_month,
        worst_steps_month,
    )


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## 3. Year-over-Year Trends

        **Question:** Are things getting better or worse over the years?
        Is there a clear trend in sleep, activity, or recovery?
        """
    )
    return


@app.cell
def _(alt, df, mo, np, pd, stats):
    # Year-over-year analysis
    yearly = df.groupby("year").agg({
        "total_sleep_hours": ["mean", "std", "count"],
        "steps": ["mean", "std"],
        "sleep_score": "mean",
        "readiness_score": "mean",
        "resting_heart_rate": "mean",
        "average_hrv": "mean",
    }).round(2)

    yearly.columns = ["_".join(col) if col[1] else col[0] for col in yearly.columns]
    yearly = yearly.reset_index()

    # Trend lines
    yearly_long = pd.melt(
        yearly[["year", "total_sleep_hours_mean", "steps_mean", "resting_heart_rate_mean"]],
        id_vars=["year"],
        var_name="metric",
        value_name="value"
    )

    # Normalize to first year for comparison
    first_year = yearly["year"].min()
    yearly_norm = yearly.copy()
    baseline = yearly[yearly["year"] == first_year].iloc[0]

    yearly_norm["sleep_pct"] = (yearly["total_sleep_hours_mean"] / baseline["total_sleep_hours_mean"]) * 100
    yearly_norm["steps_pct"] = (yearly["steps_mean"] / baseline["steps_mean"]) * 100
    yearly_norm["rhr_pct"] = (yearly["resting_heart_rate_mean"] / baseline["resting_heart_rate_mean"]) * 100

    yearly_norm_long = pd.melt(
        yearly_norm[["year", "sleep_pct", "steps_pct", "rhr_pct"]],
        id_vars=["year"],
        var_name="metric",
        value_name="pct_of_baseline"
    )

    metric_labels = {"sleep_pct": "Sleep Hours", "steps_pct": "Steps", "rhr_pct": "Resting HR"}
    yearly_norm_long["metric_label"] = yearly_norm_long["metric"].map(metric_labels)

    chart_yoy = (
        alt.Chart(yearly_norm_long)
        .mark_line(strokeWidth=3, point=True)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("pct_of_baseline:Q", title=f"% of {first_year} Baseline", scale=alt.Scale(domain=[60, 120])),
            color=alt.Color("metric_label:N", title="Metric", scale=alt.Scale(
                domain=["Sleep Hours", "Steps", "Resting HR"],
                range=["#6366f1", "#f97316", "#ef4444"]
            )),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("metric_label:N", title="Metric"),
                alt.Tooltip("pct_of_baseline:Q", title="% of Baseline", format=".1f"),
            ],
        )
        .properties(width=500, height=300, title=f"Year-over-Year Trends (% of {first_year})")
    )

    # Add reference line at 100%
    rule = alt.Chart(pd.DataFrame({"y": [100]})).mark_rule(strokeDash=[5,5], color="gray").encode(y="y:Q")

    chart_yoy_combined = chart_yoy + rule

    # Calculate overall trends
    years = yearly["year"].values
    sleep_vals = yearly["total_sleep_hours_mean"].values
    steps_vals = yearly["steps_mean"].values

    if len(years) > 2:
        sleep_slope, _, sleep_r, sleep_p, _ = stats.linregress(years, sleep_vals)
        steps_slope, _, steps_r, steps_p, _ = stats.linregress(years, steps_vals)
    else:
        sleep_slope = steps_slope = 0
        sleep_p = steps_p = 1

    # Year-over-year table
    table_data = yearly[["year", "total_sleep_hours_mean", "steps_mean", "resting_heart_rate_mean", "total_sleep_hours_count"]].copy()
    table_data.columns = ["Year", "Sleep (hrs)", "Steps", "Resting HR", "Days"]

    mo.vstack([
        chart_yoy_combined,
        mo.md(f"""
        **Trend Analysis:**
        - Sleep trend: {sleep_slope*10:+.2f} hours per decade (p={sleep_p:.3f}) {"- significant decline!" if sleep_p < 0.05 and sleep_slope < 0 else "- significant improvement!" if sleep_p < 0.05 and sleep_slope > 0 else ""}
        - Steps trend: {steps_slope*10:+,.0f} steps per decade (p={steps_p:.3f})

        **Year-by-Year Breakdown:**
        """),
        mo.ui.table(table_data, selection=None),
    ])
    return (
        baseline,
        chart_yoy,
        chart_yoy_combined,
        first_year,
        metric_labels,
        rule,
        sleep_slope,
        sleep_p,
        sleep_r,
        sleep_vals,
        steps_slope,
        steps_p,
        steps_r,
        steps_vals,
        table_data,
        yearly,
        yearly_long,
        yearly_norm,
        yearly_norm_long,
        years,
    )


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## 4. Weekend vs Weekday Behavior

        **Question:** Do you actually behave differently on weekends?
        Do you sleep in? Are you more or less active?
        """
    )
    return


@app.cell
def _(alt, df, mo, np, pd, stats):
    # Weekend vs weekday comparison
    weekend_comp = df.groupby("is_weekend").agg({
        "total_sleep_hours": ["mean", "std", "count"],
        "steps": ["mean", "std"],
        "sleep_score": "mean",
        "readiness_score": "mean",
        "high_activity_time_minutes": "mean",
        "sedentary_time_minutes": "mean",
    }).round(2)

    weekend_comp.columns = ["_".join(col) if col[1] else col[0] for col in weekend_comp.columns]
    weekend_comp = weekend_comp.reset_index()
    weekend_comp["day_type"] = weekend_comp["is_weekend"].map({False: "Weekday", True: "Weekend"})

    # Statistical tests
    weekday_sleep = df[~df["is_weekend"]]["total_sleep_hours"].dropna()
    weekend_sleep = df[df["is_weekend"]]["total_sleep_hours"].dropna()
    t_sleep, p_sleep_wk = stats.ttest_ind(weekday_sleep, weekend_sleep)

    weekday_steps = df[~df["is_weekend"]]["steps"].dropna()
    weekend_steps = df[df["is_weekend"]]["steps"].dropna()
    t_steps, p_steps_wk = stats.ttest_ind(weekday_steps, weekend_steps)

    # Chart data
    comp_data = pd.DataFrame({
        "Metric": ["Sleep Hours", "Sleep Hours", "Steps (thousands)", "Steps (thousands)"],
        "Day Type": ["Weekday", "Weekend", "Weekday", "Weekend"],
        "Value": [
            weekend_comp[weekend_comp["day_type"]=="Weekday"]["total_sleep_hours_mean"].values[0],
            weekend_comp[weekend_comp["day_type"]=="Weekend"]["total_sleep_hours_mean"].values[0],
            weekend_comp[weekend_comp["day_type"]=="Weekday"]["steps_mean"].values[0] / 1000,
            weekend_comp[weekend_comp["day_type"]=="Weekend"]["steps_mean"].values[0] / 1000,
        ]
    })

    chart_weekend = (
        alt.Chart(comp_data)
        .mark_bar()
        .encode(
            x=alt.X("Day Type:N", title=""),
            y=alt.Y("Value:Q", title="Value"),
            color=alt.Color("Day Type:N", scale=alt.Scale(domain=["Weekday", "Weekend"], range=["#64748b", "#22c55e"])),
            column=alt.Column("Metric:N", title=""),
            tooltip=[
                alt.Tooltip("Day Type:N"),
                alt.Tooltip("Value:Q", format=".2f"),
            ],
        )
        .properties(width=150, height=250)
    )

    sleep_diff = weekend_comp[weekend_comp["day_type"]=="Weekend"]["total_sleep_hours_mean"].values[0] - \
                 weekend_comp[weekend_comp["day_type"]=="Weekday"]["total_sleep_hours_mean"].values[0]
    steps_diff = weekend_comp[weekend_comp["day_type"]=="Weekend"]["steps_mean"].values[0] - \
                 weekend_comp[weekend_comp["day_type"]=="Weekday"]["steps_mean"].values[0]

    mo.vstack([
        chart_weekend,
        mo.md(f"""
        **Weekend vs Weekday Differences:**

        | Metric | Weekday | Weekend | Difference | Significant? |
        |--------|---------|---------|------------|--------------|
        | Sleep Hours | {weekend_comp[weekend_comp['day_type']=='Weekday']['total_sleep_hours_mean'].values[0]:.2f} | {weekend_comp[weekend_comp['day_type']=='Weekend']['total_sleep_hours_mean'].values[0]:.2f} | {sleep_diff:+.2f} hrs | {"Yes (p={:.3f})".format(p_sleep_wk) if p_sleep_wk < 0.05 else "No"} |
        | Steps | {weekend_comp[weekend_comp['day_type']=='Weekday']['steps_mean'].values[0]:,.0f} | {weekend_comp[weekend_comp['day_type']=='Weekend']['steps_mean'].values[0]:,.0f} | {steps_diff:+,.0f} | {"Yes (p={:.3f})".format(p_steps_wk) if p_steps_wk < 0.05 else "No"} |

        **Interpretation:**
        - {"You sleep MORE on weekends (+{:.0f} min)".format(sleep_diff * 60) if sleep_diff > 0 else "You sleep LESS on weekends ({:.0f} min)".format(sleep_diff * 60)}
        - {"You're MORE active on weekends" if steps_diff > 0 else "You're LESS active on weekends"}
        """),
    ])
    return (
        chart_weekend,
        comp_data,
        p_sleep_wk,
        p_steps_wk,
        sleep_diff,
        steps_diff,
        t_sleep,
        t_steps,
        weekday_sleep,
        weekday_steps,
        weekend_comp,
        weekend_sleep,
        weekend_steps,
    )


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## 5. Weekly Rhythm (Autocorrelation)

        **Question:** Is there a predictable weekly cycle?
        Does how you feel today predict how you'll feel in 7 days?
        """
    )
    return


@app.cell
def _(alt, df, mo, np, pd):
    # Autocorrelation analysis for weekly patterns
    sleep_series = df["total_sleep_hours"].dropna()
    steps_series = df["steps"].dropna()

    # Calculate autocorrelation for lags 1-14
    def calc_autocorr(series, max_lag=14):
        result = []
        for lag in range(1, max_lag + 1):
            shifted = series.shift(lag)
            valid = series.notna() & shifted.notna()
            if valid.sum() > 30:
                corr = series[valid].corr(shifted[valid])
                result.append({"lag": lag, "autocorrelation": corr})
        return pd.DataFrame(result)

    sleep_acf = calc_autocorr(sleep_series)
    sleep_acf["metric"] = "Sleep Hours"

    steps_acf = calc_autocorr(steps_series)
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
            tooltip=[
                alt.Tooltip("lag:O", title="Lag"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("autocorrelation:Q", title="Correlation", format=".3f"),
            ],
        )
        .properties(width=600, height=300, title="Autocorrelation by Lag (Days)")
    )

    # Add reference line for significance (~0.05 for n>1000)
    sig_level = 1.96 / np.sqrt(len(sleep_series))

    # Check for 7-day cycle
    sleep_lag7 = sleep_acf[sleep_acf["lag"] == 7]["autocorrelation"].values[0] if len(sleep_acf[sleep_acf["lag"] == 7]) > 0 else 0
    steps_lag7 = steps_acf[steps_acf["lag"] == 7]["autocorrelation"].values[0] if len(steps_acf[steps_acf["lag"] == 7]) > 0 else 0
    sleep_lag1 = sleep_acf[sleep_acf["lag"] == 1]["autocorrelation"].values[0] if len(sleep_acf[sleep_acf["lag"] == 1]) > 0 else 0
    steps_lag1 = steps_acf[steps_acf["lag"] == 1]["autocorrelation"].values[0] if len(steps_acf[steps_acf["lag"] == 1]) > 0 else 0

    mo.vstack([
        chart_acf,
        mo.md(f"""
        **What This Shows:**
        - Bars show how correlated today's value is with values from 1-14 days ago
        - Higher bars = more predictable patterns
        - Lag 7 specifically tests for weekly cycles

        **Key Findings:**

        | Metric | Lag-1 (yesterday) | Lag-7 (weekly) | Weekly Cycle? |
        |--------|-------------------|----------------|---------------|
        | Sleep | {sleep_lag1:.3f} | {sleep_lag7:.3f} | {"Yes!" if sleep_lag7 > sig_level else "No"} |
        | Steps | {steps_lag1:.3f} | {steps_lag7:.3f} | {"Yes!" if steps_lag7 > sig_level else "No"} |

        **Significance threshold:** ~{sig_level:.3f} (values above this indicate real patterns)

        **Interpretation:**
        - {"Strong weekly rhythm in sleep - you tend to sleep similarly on the same day each week" if sleep_lag7 > sig_level else "No strong weekly sleep pattern"}
        - {"Strong weekly rhythm in activity - you're active on the same days each week" if steps_lag7 > sig_level else "No strong weekly activity pattern"}
        - Lag-1 correlation shows day-to-day consistency (how "sticky" your behavior is)
        """),
    ])
    return (
        acf_data,
        calc_autocorr,
        chart_acf,
        sig_level,
        sleep_acf,
        sleep_lag1,
        sleep_lag7,
        sleep_series,
        steps_acf,
        steps_lag1,
        steps_lag7,
        steps_series,
    )


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## Summary

        Key patterns discovered in the data:
        """
    )
    return


@app.cell
def _(best_sleep_day, best_sleep_month, best_steps_day, best_steps_month, mo, p_month, p_sleep, p_steps, sig_level, sleep_diff, sleep_lag7, sleep_slope, steps_diff, steps_lag7, steps_slope, worst_sleep_day, worst_sleep_month, worst_steps_day, worst_steps_month):
    mo.md(f"""
    ### Day of Week
    - Best sleep day: **{best_sleep_day}** / Worst: **{worst_sleep_day}**
    - Most active day: **{best_steps_day}** / Least: **{worst_steps_day}**
    - Day-of-week effect on sleep: {"Significant" if p_sleep < 0.05 else "Not significant"}
    - Day-of-week effect on steps: {"Significant" if p_steps < 0.05 else "Not significant"}

    ### Seasonal
    - Best sleep month: **{best_sleep_month}** / Worst: **{worst_sleep_month}**
    - Most active month: **{best_steps_month}** / Least: **{worst_steps_month}**
    - Seasonal patterns: {"Statistically significant" if p_month < 0.05 else "Not significant"}

    ### Year-over-Year
    - Sleep trend: {sleep_slope*10:+.2f} hours per decade
    - Steps trend: {steps_slope*10:+,.0f} steps per decade

    ### Weekend vs Weekday
    - Weekend sleep difference: {sleep_diff:+.2f} hours
    - Weekend activity difference: {steps_diff:+,.0f} steps

    ### Weekly Rhythm
    - Sleep has {"a weekly cycle" if sleep_lag7 > sig_level else "no weekly cycle"}
    - Steps have {"a weekly cycle" if steps_lag7 > sig_level else "no weekly cycle"}
    """)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        *Analysis generated with [marimo](https://marimo.io)*
        """
    )
    return


if __name__ == "__main__":
    app.run()

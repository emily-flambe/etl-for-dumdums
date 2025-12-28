"""
Tests for Streamlit pages.

These tests verify that Streamlit pages render without errors by:
1. Loading data from BigQuery (or using mock data)
2. Executing the chart/visualization code that only fails at runtime
3. Catching Altair encoding errors, type mismatches, etc.

Run with: make test
"""

import altair as alt
import pandas as pd
import pytest


# Mock data that matches the structure returned by BigQuery
@pytest.fixture
def mock_issues_data():
    return pd.DataFrame({
        "identifier": ["DDX-1", "DDX-2"],
        "title": ["Issue 1", "Issue 2"],
        "state": ["In Progress", "Done"],
        "estimate": [3.0, 5.0],
        "assignee_name": ["Alice", "Bob"],
        "cycle_name": ["Sprint 1", "Sprint 1"],
        "cycle_starts_at": pd.to_datetime(["2025-01-01", "2025-01-01"]),
        "labels": [["bug"], ["feature"]],
        "project_name": ["Project A", "Project A"],
        "days_since_created": [10, 5],
        "created_at": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "updated_at": pd.to_datetime(["2025-01-10", "2025-01-07"]),
        "parent_identifier": [None, None],
        "parent_title": [None, None],
        "is_parent": [False, False],
        "is_child": [False, False],
        "child_count": [0, 0],
    })


@pytest.fixture
def mock_prs_data():
    return pd.DataFrame({
        "pull_request_id": [1, 2],
        "title": ["PR 1", "PR 2"],
        "state": ["open", "closed"],
        "author_login": ["alice", "bob"],
        "created_at": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "updated_at": pd.to_datetime(["2025-01-10", "2025-01-07"]),
        "merged_at": [None, pd.Timestamp("2025-01-07")],
    })


@pytest.fixture
def mock_oura_data():
    """Mock Oura data matching BigQuery fct_oura_daily structure."""
    return pd.DataFrame({
        "day": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
        "sleep_id": ["s1", "s2", "s3"],
        "sleep_score": [75.0, 82.0, 68.0],
        "readiness_score": [80.0, 85.0, 72.0],
        "activity_score": [90.0, 88.0, 95.0],
        "combined_wellness_score": [82.0, 85.0, 78.0],
        "steps": [8500.0, 12000.0, 6000.0],
        "active_calories": [350.0, 450.0, 280.0],
        "total_calories": [2200.0, 2400.0, 2100.0],
        "walking_distance_meters": [6500.0, 9000.0, 4500.0],
        "sleep_category": ["good", "excellent", "fair"],
        "readiness_category": ["good", "optimal", "fair"],
        "activity_category": ["active", "very_active", "moderate"],
        "temperature_deviation": [0.1, -0.2, 0.0],
        "sleep_contributor_deep_sleep": [80, 85, 70],
        "sleep_contributor_efficiency": [75, 80, 72],
        "sleep_contributor_latency": [90, 88, 85],
        "sleep_contributor_rem_sleep": [78, 82, 75],
        "sleep_contributor_restfulness": [72, 78, 68],
        "sleep_contributor_timing": [85, 90, 80],
        "sleep_contributor_total_sleep": [76, 84, 70],
        "readiness_id": ["r1", "r2", "r3"],
        "readiness_hrv_balance": [80, 85, 75],
        "readiness_resting_hr": [78, 82, 74],
        "readiness_recovery_index": [82, 88, 76],
        "activity_id": ["a1", "a2", "a3"],
        "high_activity_time_minutes": [30, 45, 20],
        "medium_activity_time_minutes": [60, 75, 45],
        "low_activity_time_minutes": [120, 100, 90],
        "sedentary_time_minutes": [480, 420, 540],
    })


class TestOuraPage:
    """Tests for the Oura Wellness page."""

    def test_oura_data_type_conversions(self, mock_oura_data):
        """Test that data type conversions work correctly."""
        df = mock_oura_data.copy()

        # These are the conversions done in the actual page
        df["day"] = pd.to_datetime(df["day"])

        int_cols = ["sleep_score", "readiness_score", "activity_score", "steps",
                    "active_calories", "total_calories", "walking_distance_meters"]
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)

        assert df["day"].dtype == "datetime64[ns]"
        assert df["sleep_score"].dtype == "float64"
        assert df["steps"].dtype == "float64"

    def test_oura_date_filtering(self, mock_oura_data):
        """Test that date filtering works with converted types."""
        from datetime import timedelta

        df = mock_oura_data.copy()
        df["day"] = pd.to_datetime(df["day"])

        min_date = df["day"].min()
        max_date = df["day"].max()
        default_start = max(min_date, max_date - timedelta(days=30))

        # This should not raise an error
        filtered = df[
            (df["day"] >= pd.Timestamp(default_start))
            & (df["day"] <= pd.Timestamp(max_date))
        ]
        assert len(filtered) > 0

    def test_oura_scores_line_chart(self, mock_oura_data):
        """Test that the scores line chart renders without errors."""
        df = mock_oura_data.copy()
        df["day"] = pd.to_datetime(df["day"])

        # Melt for multi-line chart (same as page does)
        chart_df = df[["day", "sleep_score", "readiness_score", "activity_score"]].copy()
        chart_df = chart_df.melt(id_vars=["day"], var_name="Metric", value_name="Score")
        chart_df["Metric"] = chart_df["Metric"].map({
            "sleep_score": "Sleep",
            "readiness_score": "Readiness",
            "activity_score": "Activity",
        })

        # This should not raise an Altair error
        line_chart = alt.Chart(chart_df).mark_line(point=True).encode(
            x=alt.X("day:T", title="Date"),
            y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("Metric:N", scale=alt.Scale(
                domain=["Sleep", "Readiness", "Activity"],
                range=["#6366f1", "#22c55e", "#f59e0b"]
            )),
            tooltip=["day:T", "Metric:N", "Score:Q"],
        ).properties(height=300)

        # Convert to dict to force validation
        spec = line_chart.to_dict()
        assert "encoding" in spec
        assert spec["mark"]["type"] == "line"

    def test_oura_steps_bar_chart(self, mock_oura_data):
        """Test that the steps bar chart renders without errors.

        This specifically tests the fix for nested alt.condition() which
        is not supported in Altair v6.
        """
        df = mock_oura_data.copy()
        df["day"] = pd.to_datetime(df["day"])
        df["steps"] = df["steps"].astype(float)

        # Add color category (the fix for nested alt.condition)
        df["steps_color"] = df["steps"].apply(
            lambda x: "10k+" if x >= 10000 else ("7.5k+" if x >= 7500 else "<7.5k")
        )

        steps_chart = alt.Chart(df).mark_bar().encode(
            x=alt.X("day:T", title="Date"),
            y=alt.Y("steps:Q", title="Steps"),
            color=alt.Color("steps_color:N", scale=alt.Scale(
                domain=["10k+", "7.5k+", "<7.5k"],
                range=["#22c55e", "#f59e0b", "#ef4444"]
            ), legend=alt.Legend(title="Steps")),
            tooltip=["day:T", "steps:Q"],
        ).properties(height=250)

        # Convert to dict to force validation
        spec = steps_chart.to_dict()
        assert "encoding" in spec
        assert spec["mark"]["type"] == "bar"

    def test_oura_distribution_bar_charts(self, mock_oura_data):
        """Test that the distribution bar charts render without errors."""
        df = mock_oura_data.copy()

        # Sleep categories bar chart
        sleep_dist = df["sleep_category"].value_counts().reset_index()
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

        spec = sleep_chart.to_dict()
        assert spec["mark"]["type"] == "bar"

    def test_oura_temperature_chart(self, mock_oura_data):
        """Test that the temperature deviation bar chart renders without errors."""
        df = mock_oura_data.copy()
        df["day"] = pd.to_datetime(df["day"])

        # Filter for non-null temperature data
        temp_data = df[df["temperature_deviation"].notna()].copy()

        # Add color category (same as page does)
        temp_data["temp_color"] = temp_data["temperature_deviation"].apply(
            lambda x: "elevated" if x > 0.5 else ("warm" if x > 0 else ("cool" if x > -0.5 else "low"))
        )

        temp_chart = alt.Chart(temp_data).mark_bar().encode(
            x=alt.X("day:T", title="Date"),
            y=alt.Y("temperature_deviation:Q", title="Deviation from Baseline (C)"),
            color=alt.Color("temp_color:N", scale=alt.Scale(
                domain=["elevated", "warm", "cool", "low"],
                range=["#ef4444", "#f59e0b", "#3b82f6", "#6366f1"]
            ), legend=alt.Legend(title="Temp")),
            tooltip=[
                alt.Tooltip("day:T", title="Date"),
                alt.Tooltip("temperature_deviation:Q", title="Deviation", format="+.2f"),
            ],
        ).properties(height=200)

        spec = temp_chart.to_dict()
        assert "encoding" in spec
        assert spec["mark"]["type"] == "bar"


class TestHomePage:
    """Tests for the Home page."""

    def test_home_page_metrics(self, mock_issues_data, mock_prs_data, mock_oura_data):
        """Test that home page metric calculations work."""
        # Linear metrics
        issues = mock_issues_data
        done_states = ["Done", "Done Pending Deployment"]
        open_count = len(issues[~issues["state"].isin(done_states)])
        assert open_count == 1

        # GitHub metrics
        prs = mock_prs_data
        open_prs = len(prs[prs["state"] == "open"])
        assert open_prs == 1

        # Oura metrics
        oura = mock_oura_data
        avg_wellness = oura["combined_wellness_score"].mean()
        assert avg_wellness > 0


class TestAltairV6Compatibility:
    """Tests specifically for Altair v6 compatibility issues."""

    def test_no_nested_condition(self):
        """Verify that nested alt.condition() raises an error in Altair v6.

        This test documents the issue so we don't accidentally reintroduce it.
        """
        df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        # This pattern was used before and fails in Altair v6
        with pytest.raises(TypeError):
            chart = alt.Chart(df).mark_bar().encode(
                x="x:Q",
                y="y:Q",
                color=alt.condition(
                    alt.datum.y >= 25,
                    alt.value("green"),
                    alt.condition(
                        alt.datum.y >= 15,
                        alt.value("yellow"),
                        alt.value("red")
                    )
                ),
            )
            # Force validation
            chart.to_dict()

    def test_categorical_color_alternative(self):
        """Test the correct pattern for multi-level color encoding."""
        df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        # Add categorical column instead of nested conditions
        df["color_cat"] = df["y"].apply(
            lambda v: "high" if v >= 25 else ("medium" if v >= 15 else "low")
        )

        chart = alt.Chart(df).mark_bar().encode(
            x="x:Q",
            y="y:Q",
            color=alt.Color("color_cat:N", scale=alt.Scale(
                domain=["high", "medium", "low"],
                range=["green", "yellow", "red"]
            )),
        )

        # This should work
        spec = chart.to_dict()
        assert "encoding" in spec

import pandas as pd

from services.dqi_service import (
    get_all_tests,
    get_available_charts,
    get_chart_data,
    get_chart_filter_values,
    get_data_availability,
    get_data_from_test_results,
    get_data_quality_grade,
    get_last_test_run_time,
    get_mart_statuses,
    get_mart_test_summary,
    get_mart_tests,
    get_outstanding_errors,
    get_test_category_summary,
    get_tests_completed_count,
)


class TestGetAvailableCharts:
    def test_returns_dataframe(self, mock_get_db_connection, sample_chart_data):
        """Test that get_available_charts returns a DataFrame."""
        result = get_available_charts()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_returns_correct_columns(self, mock_get_db_connection, sample_chart_data):
        """Test that get_available_charts returns the expected columns."""
        result = get_available_charts()
        expected_columns = [
            "DATA_QUALITY_CATEGORY",
            "GRAPH_NAME",
            "Y_AXIS_DESCRIPTION",
            "X_AXIS_DESCRIPTION",
            "FILTER_DESCRIPTION",
            "SUM_DESCRIPTION",
            "LEVEL_OF_DETAIL",
        ]
        assert all(col in result.columns for col in expected_columns)

    def test_returns_unique_charts(self, mock_get_db_connection, sample_chart_data):
        """Test that get_available_charts returns unique chart names."""
        result = get_available_charts()
        # Check that we have the expected number of unique charts
        assert len(result["GRAPH_NAME"].unique()) == 2
        assert "medical_paid_amount_vs_end_date_matrix" in result["GRAPH_NAME"].values
        assert "medical_claim_count_vs_end_date_matrix" in result["GRAPH_NAME"].values


class TestGetChartData:
    def test_returns_dataframe(self, mock_get_db_connection, sample_chart_data):
        """Test that get_chart_data returns a DataFrame."""
        result = get_chart_data("medical_paid_amount_vs_end_date_matrix")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_filters_by_chart_name(self, mock_get_db_connection, sample_chart_data):
        """Test that get_chart_data filters by chart name."""
        result = get_chart_data("medical_paid_amount_vs_end_date_matrix")
        assert all(
            row["GRAPH_NAME"] == "medical_paid_amount_vs_end_date_matrix"
            for _, row in result.iterrows()
        )

    def test_filters_by_chart_filter(self, mock_get_db_connection, sample_chart_data):
        """Test that get_chart_data filters by chart filter."""
        result = get_chart_data("medical_paid_amount_vs_end_date_matrix", "2017-01-01")
        assert all(row["CHART_FILTER"] == "2017-01-01" for _, row in result.iterrows())

    def test_returns_empty_for_nonexistent_chart(
        self, mock_get_db_connection, sample_chart_data
    ):
        """Test that get_chart_data returns empty DataFrame for nonexistent chart."""
        result = get_chart_data("nonexistent_chart")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestGetChartFilterValues:
    def test_returns_list(self, mock_get_db_connection, sample_chart_data):
        """Test that get_chart_filter_values returns a list."""
        result = get_chart_filter_values("medical_paid_amount_vs_end_date_matrix")
        assert isinstance(result, list)

    def test_returns_unique_filter_values(
        self, mock_get_db_connection, sample_chart_data
    ):
        """Test that get_chart_filter_values returns unique filter values."""
        result = get_chart_filter_values("medical_paid_amount_vs_end_date_matrix")
        assert len(result) == 1
        assert "2017-01-01" in result

    def test_returns_empty_for_nonexistent_chart(
        self, mock_get_db_connection, sample_chart_data
    ):
        """Test that get_chart_filter_values returns empty list for nonexistent chart."""
        result = get_chart_filter_values("nonexistent_chart")
        assert isinstance(result, list)
        assert len(result) == 0


class TestGetDataFromTestResults:
    def test_returns_dataframe(self, mock_get_db_connection, sample_test_results):
        """Test that get_data_from_test_results returns a DataFrame."""
        result = get_data_from_test_results()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_respects_limit(self, mock_get_db_connection, sample_test_results):
        """Test that get_data_from_test_results respects the limit parameter."""
        result = get_data_from_test_results(limit=2)
        assert len(result) == 2

    def test_returns_all_columns(self, mock_get_db_connection, sample_test_results):
        """Test that get_data_from_test_results returns all columns."""
        result = get_data_from_test_results()
        # Check that we have the expected number of columns
        assert len(result.columns) > 30


class TestGetDataQualityGrade:
    def test_returns_grade_f_for_sev1_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_data_quality_grade returns F for severity 1 failures."""
        # Update a test to fail with severity 1
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 1"
        )
        test_db_connection.commit()

        result = get_data_quality_grade()
        assert result == "F"

    def test_returns_grade_d_for_sev2_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_data_quality_grade returns D for severity 2 failures."""
        # Make sure no severity 1 failures
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'pass' WHERE SEVERITY_LEVEL = 1"
        )
        # Update a test to fail with severity 2
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 2"
        )
        test_db_connection.commit()

        result = get_data_quality_grade()
        assert result == "D"

    def test_returns_grade_c_for_sev3_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_data_quality_grade returns C for severity 3 failures."""
        # Make sure no severity 1 or 2 failures
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'pass' WHERE SEVERITY_LEVEL IN (1, 2)"
        )
        # Update a test to fail with severity 3
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 3"
        )
        test_db_connection.commit()

        result = get_data_quality_grade()
        assert result == "C"

    def test_returns_grade_b_for_sev4_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_data_quality_grade returns B for severity 4 failures."""
        # Make sure no severity 1, 2, or 3 failures
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'pass' WHERE SEVERITY_LEVEL IN (1, 2, 3)"
        )
        # Add a test with severity 4 that fails
        test_db_connection.execute(
            """
            INSERT INTO test_results (UNIQUE_ID, STATUS, SEVERITY_LEVEL)
            VALUES ('test.sev4.fail', 'fail', 4)
            """
        )
        test_db_connection.commit()

        result = get_data_quality_grade()
        assert result == "B"

    def test_returns_grade_a_for_no_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_data_quality_grade returns A for no failures."""
        # Make sure all tests pass
        test_db_connection.execute("UPDATE test_results SET STATUS = 'pass'")
        test_db_connection.commit()

        result = get_data_quality_grade()
        assert result == "A"


class TestGetTestsCompletedCount:
    def test_returns_integer(self, mock_get_db_connection, sample_test_results):
        """Test that get_tests_completed_count returns an integer."""
        result = get_tests_completed_count()
        assert isinstance(result, int)

    def test_counts_all_tests(self, mock_get_db_connection, sample_test_results):
        """Test that get_tests_completed_count counts all tests."""
        result = get_tests_completed_count()
        assert result == 3  # We have 3 sample test results


class TestGetLastTestRunTime:
    def test_returns_string(self, mock_get_db_connection, sample_test_results):
        """Test that get_last_test_run_time returns a string."""
        result = get_last_test_run_time()
        assert isinstance(result, str)

    def test_returns_latest_time(self, mock_get_db_connection, sample_test_results):
        """Test that get_last_test_run_time returns the latest time."""
        result = get_last_test_run_time()
        assert result == "2025-03-05 20:24:04"  # Latest time in our sample data


class TestGetMartStatuses:
    def test_returns_dict(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_statuses returns a dictionary."""
        result = get_mart_statuses()
        assert isinstance(result, dict)

    def test_includes_all_marts(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_statuses includes all data marts."""
        result = get_mart_statuses()
        expected_marts = [
            "SERVICE_CATEGORIES",
            "CCSR",
            "CMS_CHRONIC_CONDITIONS",
            "TUVA_CHRONIC_CONDITIONS",
            "CMS_HCCS",
            "ED_CLASSIFICATION",
            "FINANCIAL_PMPM",
            "QUALITY_MEASURES",
            "READMISSION",
        ]
        assert all(mart in result for mart in expected_marts)

    def test_all_marts_pass_by_default(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that all marts pass by default when all tests pass."""
        # Make sure all tests pass
        test_db_connection.execute("UPDATE test_results SET STATUS = 'pass'")
        test_db_connection.commit()

        result = get_mart_statuses()
        assert all(status == "pass" for status in result.values())

    def test_all_marts_fail_with_sev1_failure(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that all marts fail when there's a severity 1 failure."""
        # Update a test to fail with severity 1
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 1"
        )
        test_db_connection.commit()

        result = get_mart_statuses()
        assert all(status == "fail" for status in result.values())

    def test_specific_mart_fails_with_sev2_failure(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that specific marts fail when they have severity 2 failures."""
        # Make sure no severity 1 failures
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'pass' WHERE SEVERITY_LEVEL = 1"
        )
        # Update a test to fail with severity 2 for CMS_CHRONIC_CONDITIONS
        test_db_connection.execute(
            """
            UPDATE test_results 
            SET STATUS = 'fail' 
            WHERE SEVERITY_LEVEL = 2 AND FLAG_CMS_CHRONIC_CONDITIONS = 1
            """
        )
        test_db_connection.commit()

        result = get_mart_statuses()
        assert result["CMS_CHRONIC_CONDITIONS"] == "fail"
        # Other marts should still pass
        assert result["SERVICE_CATEGORIES"] == "pass"

    def test_specific_mart_warns_with_sev3_failure(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that specific marts warn when they have severity 3 failures."""
        # Make sure no severity 1 or 2 failures
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'pass' WHERE SEVERITY_LEVEL IN (1, 2)"
        )
        # Update a test to fail with severity 3 for CMS_HCCS
        test_db_connection.execute(
            """
            UPDATE test_results 
            SET STATUS = 'fail' 
            WHERE SEVERITY_LEVEL = 3 AND FLAG_CMS_HCCS = 1
            """
        )
        test_db_connection.commit()

        result = get_mart_statuses()
        assert result["CMS_HCCS"] == "warn"
        # Other marts should still pass
        assert result["SERVICE_CATEGORIES"] == "pass"


class TestGetOutstandingErrors:
    def test_returns_dataframe(self, mock_get_db_connection, sample_test_results):
        """Test that get_outstanding_errors returns a DataFrame."""
        result = get_outstanding_errors()
        assert isinstance(result, pd.DataFrame)

    def test_returns_only_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_outstanding_errors returns only failures."""
        # Update a test to fail
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 1"
        )
        test_db_connection.commit()

        result = get_outstanding_errors()
        assert not result.empty
        assert all(row["STATUS"] != "pass" for _, row in result.iterrows())

    def test_returns_empty_for_no_failures(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_outstanding_errors returns empty DataFrame for no failures."""
        # Make sure all tests pass
        test_db_connection.execute("UPDATE test_results SET STATUS = 'pass'")
        test_db_connection.commit()

        result = get_outstanding_errors()
        assert result.empty


class TestGetDataAvailability:
    def test_returns_dict(
        self, mock_get_db_connection, sample_test_results, sample_chart_data
    ):
        """Test that get_data_availability returns a dictionary."""
        result = get_data_availability()
        assert isinstance(result, dict)

    def test_includes_expected_keys(
        self, mock_get_db_connection, sample_test_results, sample_chart_data
    ):
        """Test that get_data_availability includes expected keys."""
        result = get_data_availability()
        expected_keys = ["test_results", "chart_data", "chart_categories"]
        assert all(key in result for key in expected_keys)

    def test_counts_test_results(self, mock_get_db_connection, sample_test_results):
        """Test that get_data_availability counts test results."""
        result = get_data_availability()
        assert result["test_results"] == 3  # We have 3 sample test results

    def test_counts_chart_data(self, mock_get_db_connection, sample_chart_data):
        """Test that get_data_availability counts chart data."""
        result = get_data_availability()
        assert result["chart_data"] == 3  # We have 3 sample chart data points


class TestGetAllTests:
    def test_returns_dataframe(self, mock_get_db_connection, sample_test_results):
        """Test that get_all_tests returns a DataFrame."""
        result = get_all_tests()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_returns_all_tests(self, mock_get_db_connection, sample_test_results):
        """Test that get_all_tests returns all tests."""
        result = get_all_tests()
        assert len(result) == 3  # We have 3 sample test results

    def test_sorts_by_severity_and_status(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_all_tests sorts by severity and status."""
        # Update tests to have different statuses
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 1"
        )
        test_db_connection.commit()

        result = get_all_tests()
        # First row should be severity 1 and failing
        assert result.iloc[0]["SEVERITY_LEVEL"] == 1
        assert result.iloc[0]["STATUS"] == "fail"


class TestGetMartTestSummary:
    def test_returns_list(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_test_summary returns a list."""
        result = get_mart_test_summary()
        assert isinstance(result, list)

    def test_includes_all_marts(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_test_summary includes all data marts."""
        result = get_mart_test_summary()
        expected_marts = [
            "SERVICE_CATEGORIES",
            "CCSR",
            "CMS_CHRONIC_CONDITIONS",
            "TUVA_CHRONIC_CONDITIONS",
            "CMS_HCCS",
            "ED_CLASSIFICATION",
            "FINANCIAL_PMPM",
            "QUALITY_MEASURES",
            "READMISSION",
        ]
        assert all(
            any(mart["mart"] == expected_mart for mart in result)
            for expected_mart in expected_marts
        )

    def test_includes_expected_fields(
        self, mock_get_db_connection, sample_test_results
    ):
        """Test that get_mart_test_summary includes expected fields."""
        result = get_mart_test_summary()
        expected_fields = [
            "mart",
            "display_name",
            "total_tests",
            "passing_tests",
            "passing_percentage",
            "sev1_fails",
            "sev2_fails",
            "sev3_fails",
            "sev4_fails",
            "sev5_fails",
            "status",
            "status_color",
        ]
        assert all(all(field in mart for field in expected_fields) for mart in result)


class TestGetTestCategorySummary:
    def test_returns_dataframe(self, mock_get_db_connection, sample_test_results):
        """Test that get_quality_dimension_summary returns a DataFrame."""
        result = get_test_category_summary()
        assert isinstance(result, pd.DataFrame)

    def test_includes_expected_columns(
        self, mock_get_db_connection, sample_test_results
    ):
        """Test that get_quality_dimension_summary includes expected columns."""
        result = get_test_category_summary()
        expected_columns = [
            "TEST_CATEGORY",
            "total_tests",
            "passing_tests",
            "failing_tests",
            "passing_percentage",
        ]
        assert all(col in result.columns for col in expected_columns)

    def test_calculates_passing_percentage(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_quality_dimension_summary calculates passing percentage correctly."""
        # Update a test to fail
        test_db_connection.execute(
            "UPDATE test_results SET STATUS = 'fail' WHERE SEVERITY_LEVEL = 1"
        )
        test_db_connection.commit()

        result = get_test_category_summary()
        # We should have 2/3 passing tests for validity
        validity_row = result[result["TEST_CATEGORY"] == "validity"]
        assert validity_row["passing_tests"].iloc[0] == 2
        assert validity_row["failing_tests"].iloc[0] == 1
        assert validity_row["passing_percentage"].iloc[0] == 66.7  # 2/3 = 66.7%


class TestGetMartTests:
    def test_returns_dataframe(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_tests returns a DataFrame."""
        result = get_mart_tests("CMS_CHRONIC_CONDITIONS")
        assert isinstance(result, pd.DataFrame)

    def test_filters_by_mart(
        self, mock_get_db_connection, sample_test_results, test_db_connection
    ):
        """Test that get_mart_tests filters by mart."""
        # First, update a test to fail so we have a non-passing test
        test_db_connection.execute("""
            UPDATE test_results 
            SET STATUS = 'fail' 
            WHERE UNIQUE_ID IN (
                SELECT UNIQUE_ID FROM test_results 
                WHERE FLAG_CMS_CHRONIC_CONDITIONS = 1 
                LIMIT 1
            )
        """)
        test_db_connection.commit()

        # Now get the tests
        result = get_mart_tests("CMS_CHRONIC_CONDITIONS")
        # We should have at least one failing test
        assert len(result) >= 1

    def test_filters_by_status(self, mock_get_db_connection, sample_test_results):
        """Test that get_mart_tests filters by status."""
        result = get_mart_tests("CMS_CHRONIC_CONDITIONS", status="pass")
        assert all(row["STATUS"] == "pass" for _, row in result.iterrows())

    def test_returns_empty_for_nonexistent_mart(
        self, mock_get_db_connection, sample_test_results
    ):
        """Test that get_mart_tests returns empty DataFrame for nonexistent mart."""
        result = get_mart_tests("NONEXISTENT_MART")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

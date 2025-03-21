import os
import sqlite3

import pytest

from db import init_db


@pytest.fixture
def test_db_path():
    """Path to a temporary test database."""
    return "test_app_data.db"


@pytest.fixture
def test_db_connection(test_db_path):
    """Create a test database connection."""
    # Create a new database for testing
    # Use the init_db function with the test database path
    init_db(test_db_path)

    # Create a connection to the test database
    conn = sqlite3.connect(test_db_path)
    conn.row_factory = sqlite3.Row

    yield conn

    # Close and remove the test database
    conn.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)


@pytest.fixture
def sample_test_results(test_db_connection):
    """Insert sample test results into the test database."""
    # Common fields with defaults for all columns in the schema
    common_row = {
        "UNIQUE_ID": "",
        "DATABASE_NAME": "dev_chase",
        "SCHEMA_NAME": "",
        "TABLE_NAME": "",
        "TEST_NAME": "",
        "TEST_SHORT_NAME": "accepted_values",
        "TEST_COLUMN_NAME": "",
        "SEVERITY": "ERROR",
        "WARN_IF": "!= 0",
        "ERROR_IF": "!= 0",
        "TEST_PARAMS": "",
        "TEST_ORIGINAL_NAME": "accepted_values",
        "TEST_TAGS": "[]",
        "TEST_DESCRIPTION": "This test validates that all of the values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the test will fail.",
        "TEST_PACKAGE_NAME": "the_tuva_project",
        "TEST_TYPE": "generic",
        "GENERATED_AT": "2025-03-05 20:24:04",
        "METADATA_HASH": "",
        "QUALITY_DIMENSION": "validity",
        "DETECTED_AT": "2025-03-12 17:10:48.000000000",
        "CREATED_AT": "2025-03-12 10:11:24.525000000",
        "COLUMN_NAME": "",
        "TEST_SUB_TYPE": "generic",
        "TEST_RESULTS_DESCRIPTION": "",
        "TEST_RESULTS_QUERY": "",
        "STATUS": "pass",
        "FAILURES": 0,
        "FAILED_ROW_COUNT": "",
        "TEST_CATEGORY": "validity",
        "SEVERITY_LEVEL": 1,
        "FLAG_SERVICE_CATEGORIES": 0,
        "FLAG_CCSR": 0,
        "FLAG_CMS_CHRONIC_CONDITIONS": 0,
        "FLAG_TUVA_CHRONIC_CONDITIONS": 0,
        "FLAG_CMS_HCCS": 0,
        "FLAG_ED_CLASSIFICATION": 0,
        "FLAG_FINANCIAL_PMPM": 0,
        "FLAG_QUALITY_MEASURES": 0,
        "FLAG_READMISSION": 0,
    }

    # Define specific data for each test record, only overriding what's different
    data = [
        # HIV/AIDS test
        {
            **common_row,
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_chronic_conditions__cms_chronic_conditions_hiv_aids_condition__Human_Immunodeficiency_Virus_and_or_Acquired_Immunodeficiency_Syndrome_HIV_AIDS_.c54d45a2c6",
            "SCHEMA_NAME": "chronic_conditions",
            "TABLE_NAME": "_int_cms_chronic_condition_hiv_aids",
            "TEST_NAME": "accepted_values_chronic_conditions__cms_chronic_conditions_hiv_aids_condition__Human_Immunodeficiency_Virus_and_or_Acquired_Immunodeficiency_Syndrome_HIV_AIDS_",
            "TEST_COLUMN_NAME": "condition",
            "TEST_PARAMS": '{"values": ["Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)"], "column_name": "condition", "model": "{{ get_where_subquery(ref(\'chronic_conditions__cms_chronic_conditions_hiv_aids\')) }}"}',
            "METADATA_HASH": "c0f9cdd5a5d6bd6ab723b45fcb163969",
            "COLUMN_NAME": "condition",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        condition as value_field,\n        count(*) as n_records\n\n    from dev_chase.chronic_conditions._int_cms_chronic_condition_hiv_aids\n    group by condition\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)'\n)\n\n\n",
            "FLAG_CMS_CHRONIC_CONDITIONS": 1,
        },
        # Dual status code test
        {
            **common_row,
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_input_layer__eligibility_dual_status_code__00__01__02__03__04__05__06__08__09__10.5f70cd2ab3",
            "SCHEMA_NAME": "input_layer",
            "TABLE_NAME": "input_layer__eligibility",
            "TEST_NAME": "accepted_values_input_layer__eligibility_dual_status_code__00__01__02__03__04__05__06__08__09__10",
            "TEST_COLUMN_NAME": "dual_status_code",
            "SEVERITY": "warn",
            "TEST_PARAMS": '{"values": ["00", "01", "02", "03", "04", "05", "06", "08", "09", "10"], "column_name": "dual_status_code", "model": "{{ get_where_subquery(ref(\'input_layer__eligibility\')) }}"}',
            "TEST_TAGS": '["dqi", "dqi_cms_hccs", "tuva_dqi_sev_3"]',
            "TEST_DESCRIPTION": "https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january",
            "METADATA_HASH": "d8715c693c9d660963a4cafdcba490d9",
            "COLUMN_NAME": "dual_status_code",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        dual_status_code as value_field,\n        count(*) as n_records\n\n    from dev_chase.input_layer.input_layer__eligibility\n    group by dual_status_code\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    '00','01','02','03','04','05','06','08','09','10'\n)\n\n\n",
            "SEVERITY_LEVEL": 3,
            "FLAG_CMS_HCCS": 1,
            "GENERATED_AT": "2025-03-05 20:24:03",
        },
        # OUD test
        {
            **common_row,
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_chronic_conditions__cms_chronic_conditions_oud_condition__Opioid_Use_Disorder_OUD_.f44489eec5",
            "SCHEMA_NAME": "chronic_conditions",
            "TABLE_NAME": "_int_cms_chronic_condition_oud",
            "TEST_NAME": "accepted_values_chronic_conditions__cms_chronic_conditions_oud_condition__Opioid_Use_Disorder_OUD_",
            "TEST_COLUMN_NAME": "condition",
            "TEST_PARAMS": '{"values": ["Opioid Use Disorder (OUD)"], "column_name": "condition", "model": "{{ get_where_subquery(ref(\'chronic_conditions__cms_chronic_conditions_oud\')) }}"}',
            "METADATA_HASH": "cf17746d0ec95ac179e084d5e62c6347",
            "COLUMN_NAME": "condition",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        condition as value_field,\n        count(*) as n_records\n\n    from dev_chase.chronic_conditions._int_cms_chronic_condition_oud\n    group by condition\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'Opioid Use Disorder (OUD)'\n)\n\n\n",
            "SEVERITY_LEVEL": 2,
            "FLAG_CMS_CHRONIC_CONDITIONS": 1,
        },
    ]

    # Insert the data
    for row in data:
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(row.keys())
        query = f"INSERT INTO test_results ({columns}) VALUES ({placeholders})"
        test_db_connection.execute(query, list(row.values()))

    test_db_connection.commit()
    return data


@pytest.fixture
def sample_chart_data(test_db_connection):
    """Insert sample chart data into the test database."""
    # Common fields with defaults for all columns in the schema
    common_row = {
        "GRAPH_NAME": "",
        "DATA_QUALITY_CATEGORY": "timeliness",
        "LEVEL_OF_DETAIL": "month",
        "SUM_DESCRIPTION": "",
        "Y_AXIS_DESCRIPTION": "claim_end_date",
        "X_AXIS_DESCRIPTION": "paid_date",
        "FILTER_DESCRIPTION": "paid_year",
        "Y_AXIS": "",
        "X_AXIS": "",
        "CHART_FILTER": "2017-01-01",
        "VALUE": 0,
    }

    data = [
        # Paid amount chart 1
        {
            **common_row,
            "GRAPH_NAME": "medical_paid_amount_vs_end_date_matrix",
            "SUM_DESCRIPTION": "total_paid_amount",
            "Y_AXIS": "2017-12-01",
            "VALUE": 532607.03,
        },
        # Paid amount chart 2
        {
            **common_row,
            "GRAPH_NAME": "medical_paid_amount_vs_end_date_matrix",
            "SUM_DESCRIPTION": "total_paid_amount",
            "Y_AXIS": "2017-09-01",
            "VALUE": 386331.17,
        },
        # Claim count chart
        {
            **common_row,
            "GRAPH_NAME": "medical_claim_count_vs_end_date_matrix",
            "SUM_DESCRIPTION": "unique_number_of_claims",
            "Y_AXIS": "2017-12-01",
            "VALUE": 2852,
        },
    ]

    # Insert the data
    for row in data:
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(row.keys())
        query = f"INSERT INTO chart_data ({columns}) VALUES ({placeholders})"
        test_db_connection.execute(query, list(row.values()))

    test_db_connection.commit()
    return data


@pytest.fixture
def mock_get_db_connection(monkeypatch, test_db_connection):
    """Mock the get_db_connection function to return our test connection."""

    def mock_connection():
        return test_db_connection

    # Import the module that contains get_db_connection

    # Patch the function
    monkeypatch.setattr("services.dqi_service.get_db_connection", mock_connection)

    return mock_connection

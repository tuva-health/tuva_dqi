import os
import sqlite3

import pytest


@pytest.fixture
def test_db_path():
    """Path to a temporary test database."""
    return "test_app_data.db"


@pytest.fixture
def test_db_connection(test_db_path):
    """Create a test database connection."""
    # Create a new database for testing
    conn = sqlite3.connect(test_db_path)
    conn.row_factory = sqlite3.Row

    # Create the necessary tables
    conn.execute("""
    CREATE TABLE IF NOT EXISTS test_results (
        UNIQUE_ID TEXT PRIMARY KEY,
        DATABASE_NAME TEXT,
        SCHEMA_NAME TEXT,
        TABLE_NAME TEXT,
        TEST_NAME TEXT,
        TEST_SHORT_NAME TEXT,
        TEST_COLUMN_NAME TEXT,
        SEVERITY TEXT,
        WARN_IF TEXT,
        ERROR_IF TEXT,
        TEST_PARAMS TEXT,
        TEST_ORIGINAL_NAME TEXT,
        TEST_TAGS TEXT,
        TEST_DESCRIPTION TEXT,
        TEST_PACKAGE_NAME TEXT,
        TEST_TYPE TEXT,
        GENERATED_AT TEXT,
        METADATA_HASH TEXT,
        QUALITY_DIMENSION TEXT,
        DETECTED_AT TEXT,
        CREATED_AT TEXT,
        COLUMN_NAME TEXT,
        TEST_SUB_TYPE TEXT,
        TEST_RESULTS_DESCRIPTION TEXT,
        TEST_RESULTS_QUERY TEXT,
        STATUS TEXT,
        FAILURES INTEGER,
        FAILED_ROW_COUNT TEXT,
        TEST_CATEGORY TEXT,
        SEVERITY_LEVEL INTEGER,
        FLAG_SERVICE_CATEGORIES INTEGER,
        FLAG_CCSR INTEGER,
        FLAG_CMS_CHRONIC_CONDITIONS INTEGER,
        FLAG_TUVA_CHRONIC_CONDITIONS INTEGER,
        FLAG_CMS_HCCS INTEGER,
        FLAG_ED_CLASSIFICATION INTEGER,
        FLAG_FINANCIAL_PMPM INTEGER,
        FLAG_QUALITY_MEASURES INTEGER,
        FLAG_READMISSION INTEGER
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS chart_data (
        DATA_QUALITY_CATEGORY TEXT,
        GRAPH_NAME TEXT,
        LEVEL_OF_DETAIL TEXT,
        Y_AXIS_DESCRIPTION TEXT,
        X_AXIS_DESCRIPTION TEXT,
        FILTER_DESCRIPTION TEXT,
        SUM_DESCRIPTION TEXT,
        Y_AXIS TEXT,
        X_AXIS TEXT,
        CHART_FILTER TEXT,
        VALUE REAL
    )
    """)

    yield conn

    # Close and remove the test database
    conn.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)


@pytest.fixture
def sample_test_results(test_db_connection):
    """Insert sample test results into the test database."""
    # Sample data from your CSV
    data = [
        {
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_chronic_conditions__cms_chronic_conditions_hiv_aids_condition__Human_Immunodeficiency_Virus_and_or_Acquired_Immunodeficiency_Syndrome_HIV_AIDS_.c54d45a2c6",
            "DATABASE_NAME": "dev_chase",
            "SCHEMA_NAME": "chronic_conditions",
            "TABLE_NAME": "_int_cms_chronic_condition_hiv_aids",
            "TEST_NAME": "accepted_values_chronic_conditions__cms_chronic_conditions_hiv_aids_condition__Human_Immunodeficiency_Virus_and_or_Acquired_Immunodeficiency_Syndrome_HIV_AIDS_",
            "TEST_SHORT_NAME": "accepted_values",
            "TEST_COLUMN_NAME": "condition",
            "SEVERITY": "ERROR",
            "WARN_IF": "!= 0",
            "ERROR_IF": "!= 0",
            "TEST_PARAMS": '{"values": ["Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)"], "column_name": "condition", "model": "{{ get_where_subquery(ref(\'chronic_conditions__cms_chronic_conditions_hiv_aids\')) }}"}',
            "TEST_ORIGINAL_NAME": "accepted_values",
            "TEST_TAGS": "[]",
            "TEST_DESCRIPTION": "This test validates that all of the values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the test will fail.",
            "TEST_PACKAGE_NAME": "the_tuva_project",
            "TEST_TYPE": "generic",
            "GENERATED_AT": "2025-03-05 20:24:04",
            "METADATA_HASH": "c0f9cdd5a5d6bd6ab723b45fcb163969",
            "QUALITY_DIMENSION": "validity",
            "DETECTED_AT": "2025-03-12 17:10:48.000000000",
            "CREATED_AT": "2025-03-12 10:11:24.525000000",
            "COLUMN_NAME": "condition",
            "TEST_SUB_TYPE": "generic",
            "TEST_RESULTS_DESCRIPTION": "",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        condition as value_field,\n        count(*) as n_records\n\n    from dev_chase.chronic_conditions._int_cms_chronic_condition_hiv_aids\n    group by condition\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)'\n)\n\n\n",
            "STATUS": "pass",
            "FAILURES": 0,
            "FAILED_ROW_COUNT": "",
            "TEST_CATEGORY": "validity",
            "SEVERITY_LEVEL": 1,
            "FLAG_SERVICE_CATEGORIES": 0,
            "FLAG_CCSR": 0,
            "FLAG_CMS_CHRONIC_CONDITIONS": 1,
            "FLAG_TUVA_CHRONIC_CONDITIONS": 0,
            "FLAG_CMS_HCCS": 0,
            "FLAG_ED_CLASSIFICATION": 0,
            "FLAG_FINANCIAL_PMPM": 0,
            "FLAG_QUALITY_MEASURES": 0,
            "FLAG_READMISSION": 0,
        },
        {
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_input_layer__eligibility_dual_status_code__00__01__02__03__04__05__06__08__09__10.5f70cd2ab3",
            "DATABASE_NAME": "dev_chase",
            "SCHEMA_NAME": "input_layer",
            "TABLE_NAME": "input_layer__eligibility",
            "TEST_NAME": "accepted_values_input_layer__eligibility_dual_status_code__00__01__02__03__04__05__06__08__09__10",
            "TEST_SHORT_NAME": "accepted_values",
            "TEST_COLUMN_NAME": "dual_status_code",
            "SEVERITY": "warn",
            "WARN_IF": "!= 0",
            "ERROR_IF": "!= 0",
            "TEST_PARAMS": '{"values": ["00", "01", "02", "03", "04", "05", "06", "08", "09", "10"], "column_name": "dual_status_code", "model": "{{ get_where_subquery(ref(\'input_layer__eligibility\')) }}"}',
            "TEST_ORIGINAL_NAME": "accepted_values",
            "TEST_TAGS": '["dqi", "dqi_cms_hccs", "tuva_dqi_sev_3"]',
            "TEST_DESCRIPTION": "https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january",
            "TEST_PACKAGE_NAME": "the_tuva_project",
            "TEST_TYPE": "generic",
            "GENERATED_AT": "2025-03-05 20:24:03",
            "METADATA_HASH": "d8715c693c9d660963a4cafdcba490d9",
            "QUALITY_DIMENSION": "validity",
            "DETECTED_AT": "2025-03-12 17:10:48.000000000",
            "CREATED_AT": "2025-03-12 10:11:24.525000000",
            "COLUMN_NAME": "dual_status_code",
            "TEST_SUB_TYPE": "generic",
            "TEST_RESULTS_DESCRIPTION": "",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        dual_status_code as value_field,\n        count(*) as n_records\n\n    from dev_chase.input_layer.input_layer__eligibility\n    group by dual_status_code\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    '00','01','02','03','04','05','06','08','09','10'\n)\n\n\n",
            "STATUS": "pass",
            "FAILURES": 0,
            "FAILED_ROW_COUNT": "",
            "TEST_CATEGORY": "validity",
            "SEVERITY_LEVEL": 3,
            "FLAG_SERVICE_CATEGORIES": 0,
            "FLAG_CCSR": 0,
            "FLAG_CMS_CHRONIC_CONDITIONS": 0,
            "FLAG_TUVA_CHRONIC_CONDITIONS": 0,
            "FLAG_CMS_HCCS": 1,
            "FLAG_ED_CLASSIFICATION": 0,
            "FLAG_FINANCIAL_PMPM": 0,
            "FLAG_QUALITY_MEASURES": 0,
            "FLAG_READMISSION": 0,
        },
        {
            "UNIQUE_ID": "test.the_tuva_project.accepted_values_chronic_conditions__cms_chronic_conditions_oud_condition__Opioid_Use_Disorder_OUD_.f44489eec5",
            "DATABASE_NAME": "dev_chase",
            "SCHEMA_NAME": "chronic_conditions",
            "TABLE_NAME": "_int_cms_chronic_condition_oud",
            "TEST_NAME": "accepted_values_chronic_conditions__cms_chronic_conditions_oud_condition__Opioid_Use_Disorder_OUD_",
            "TEST_SHORT_NAME": "accepted_values",
            "TEST_COLUMN_NAME": "condition",
            "SEVERITY": "ERROR",
            "WARN_IF": "!= 0",
            "ERROR_IF": "!= 0",
            "TEST_PARAMS": '{"values": ["Opioid Use Disorder (OUD)"], "column_name": "condition", "model": "{{ get_where_subquery(ref(\'chronic_conditions__cms_chronic_conditions_oud\')) }}"}',
            "TEST_ORIGINAL_NAME": "accepted_values",
            "TEST_TAGS": "[]",
            "TEST_DESCRIPTION": "This test validates that all of the values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the test will fail.",
            "TEST_PACKAGE_NAME": "the_tuva_project",
            "TEST_TYPE": "generic",
            "GENERATED_AT": "2025-03-05 20:24:04",
            "METADATA_HASH": "cf17746d0ec95ac179e084d5e62c6347",
            "QUALITY_DIMENSION": "validity",
            "DETECTED_AT": "2025-03-12 17:10:48.000000000",
            "CREATED_AT": "2025-03-12 10:11:24.525000000",
            "COLUMN_NAME": "condition",
            "TEST_SUB_TYPE": "generic",
            "TEST_RESULTS_DESCRIPTION": "",
            "TEST_RESULTS_QUERY": "with all_values as (\n\n    select\n        condition as value_field,\n        count(*) as n_records\n\n    from dev_chase.chronic_conditions._int_cms_chronic_condition_oud\n    group by condition\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'Opioid Use Disorder (OUD)'\n)\n\n\n",
            "STATUS": "pass",
            "FAILURES": 0,
            "FAILED_ROW_COUNT": "",
            "TEST_CATEGORY": "validity",
            "SEVERITY_LEVEL": 2,
            "FLAG_SERVICE_CATEGORIES": 0,
            "FLAG_CCSR": 0,
            "FLAG_CMS_CHRONIC_CONDITIONS": 1,
            "FLAG_TUVA_CHRONIC_CONDITIONS": 0,
            "FLAG_CMS_HCCS": 0,
            "FLAG_ED_CLASSIFICATION": 0,
            "FLAG_FINANCIAL_PMPM": 0,
            "FLAG_QUALITY_MEASURES": 0,
            "FLAG_READMISSION": 0,
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
    # Sample data from your CSV
    data = [
        {
            "DATA_QUALITY_CATEGORY": "timeliness",
            "GRAPH_NAME": "medical_paid_amount_vs_end_date_matrix",
            "LEVEL_OF_DETAIL": "month",
            "Y_AXIS_DESCRIPTION": "claim_end_date",
            "X_AXIS_DESCRIPTION": "paid_date",
            "FILTER_DESCRIPTION": "paid_year",
            "SUM_DESCRIPTION": "total_paid_amount",
            "Y_AXIS": "2017-12-01",
            "X_AXIS": "",
            "CHART_FILTER": "2017-01-01",
            "VALUE": 532607.03,
        },
        {
            "DATA_QUALITY_CATEGORY": "timeliness",
            "GRAPH_NAME": "medical_paid_amount_vs_end_date_matrix",
            "LEVEL_OF_DETAIL": "month",
            "Y_AXIS_DESCRIPTION": "claim_end_date",
            "X_AXIS_DESCRIPTION": "paid_date",
            "FILTER_DESCRIPTION": "paid_year",
            "SUM_DESCRIPTION": "total_paid_amount",
            "Y_AXIS": "2017-09-01",
            "X_AXIS": "",
            "CHART_FILTER": "2017-01-01",
            "VALUE": 386331.17,
        },
        {
            "DATA_QUALITY_CATEGORY": "timeliness",
            "GRAPH_NAME": "medical_claim_count_vs_end_date_matrix",
            "LEVEL_OF_DETAIL": "month",
            "Y_AXIS_DESCRIPTION": "claim_end_date",
            "X_AXIS_DESCRIPTION": "paid_date",
            "FILTER_DESCRIPTION": "paid_year",
            "SUM_DESCRIPTION": "unique_number_of_claims",
            "Y_AXIS": "2017-12-01",
            "X_AXIS": "",
            "CHART_FILTER": "2017-01-01",
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

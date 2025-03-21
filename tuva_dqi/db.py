import sqlite3


def get_db_connection(db_file_name="app_data.db") -> sqlite3.Connection:
    """Create a connection to the SQLite database."""
    conn = sqlite3.connect(db_file_name)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_file_name="app_data.db") -> None:
    """Initialize the database with required tables."""
    conn = get_db_connection(db_file_name)
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
    conn.commit()
    conn.close()

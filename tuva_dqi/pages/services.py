import pandas as pd
from pandas import DataFrame

from db import get_db_connection


def get_available_charts() -> DataFrame:
    """Get a list of available charts from the database."""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(
            """
            SELECT DISTINCT 
                DATA_QUALITY_CATEGORY, 
                GRAPH_NAME,
                Y_AXIS_DESCRIPTION,
                X_AXIS_DESCRIPTION,
                FILTER_DESCRIPTION,
                SUM_DESCRIPTION,
                LEVEL_OF_DETAIL
            FROM chart_data
            ORDER BY DATA_QUALITY_CATEGORY, GRAPH_NAME
        """,
            conn,
        )
        conn.close()
        return df
    except Exception as e:
        print(f"Error getting available charts: {str(e)}")
        return pd.DataFrame()


def get_chart_data(graph_name, chart_filter=None) -> DataFrame:
    """Get data for a specific chart."""
    try:
        conn = get_db_connection()
        query = f"""
            SELECT * FROM chart_data
            WHERE GRAPH_NAME = '{graph_name}'
        """

        if chart_filter:
            query += f" AND CHART_FILTER = '{chart_filter}'"

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error getting chart data: {str(e)}")
        return pd.DataFrame()


def get_chart_filter_values(graph_name) -> list:
    """Get unique filter values for a chart."""
    try:
        conn = get_db_connection()
        query = f"""
            SELECT DISTINCT CHART_FILTER 
            FROM chart_data
            WHERE GRAPH_NAME = '{graph_name}'
            AND CHART_FILTER IS NOT NULL
            AND CHART_FILTER != ''
            ORDER BY CHART_FILTER
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df["CHART_FILTER"].tolist()
    except Exception as e:
        print(f"Error getting chart filter values: {str(e)}")
        return []


def get_data_from_test_results(limit=100) -> DataFrame:
    conn = get_db_connection()
    df = pd.read_sql_query(
        f"SELECT * FROM test_results LIMIT {limit}",
        conn,
    )
    conn.close()
    return df


def get_data_quality_grade() -> str:
    conn = get_db_connection()

    # Check for Sev 1 issues (status is not 'pass' and SEVERITY_LEVEL = 1)
    sev1_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 1
    """,
        conn,
    ).iloc[0]["count"]

    # Check for Sev 2 issues
    sev2_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 2
    """,
        conn,
    ).iloc[0]["count"]

    # Check for Sev 3 issues
    sev3_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 3
    """,
        conn,
    ).iloc[0]["count"]

    # Check for Sev 4 issues
    sev4_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 4
    """,
        conn,
    ).iloc[0]["count"]

    conn.close()

    # Determine grade based on severity counts
    if sev1_count > 0:
        return "F"
    elif sev2_count > 0:
        return "D"
    elif sev3_count > 0:
        return "C"
    elif sev4_count > 0:
        return "B"
    else:
        return "A"


# services.py
def get_tests_completed_count() -> int:
    conn = get_db_connection()
    count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results
    """,
        conn,
    ).iloc[0]["count"]
    conn.close()
    return int(count)


# services.py
def get_last_test_run_time():
    conn = get_db_connection()
    last_time = pd.read_sql_query(
        """
        SELECT MAX(GENERATED_AT) as last_run FROM test_results
    """,
        conn,
    ).iloc[0]["last_run"]
    conn.close()
    return last_time if last_time else "No data available"


def get_mart_statuses() -> dict[str, str]:
    conn = get_db_connection()

    # Get all failed tests with severity level 1
    sev1_failures = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 1
    """,
        conn,
    ).iloc[0]["count"]

    # Initialize results dictionary
    mart_statuses = {
        "SERVICE_CATEGORIES": "pass",
        "CCSR": "pass",
        "CMS_CHRONIC_CONDITIONS": "pass",
        "TUVA_CHRONIC_CONDITIONS": "pass",
        "CMS_HCCS": "pass",
        "ED_CLASSIFICATION": "pass",
        "FINANCIAL_PMPM": "pass",
        "QUALITY_MEASURES": "pass",
        "READMISSION": "pass",
    }

    # If any severity level 1 issues exist, all marts are not usable
    if sev1_failures > 0:
        for mart in mart_statuses:
            mart_statuses[mart] = "fail"
        conn.close()
        return mart_statuses

    # Check severity level 2 issues for each mart
    for mart in mart_statuses:
        flag_column = f"FLAG_{mart}"

        # Check severity level 2 issues for this mart
        sev2_failures = pd.read_sql_query(
            f"""
            SELECT COUNT(*) as count FROM test_results 
            WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 2 AND {flag_column} = 1
        """,
            conn,
        ).iloc[0]["count"]

        if sev2_failures > 0:
            mart_statuses[mart] = "fail"
            continue  # No need to check level 3 if already failed

        # Check severity level 3 issues for this mart
        sev3_failures = pd.read_sql_query(
            f"""
            SELECT COUNT(*) as count FROM test_results 
            WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 3 AND {flag_column} = 1
        """,
            conn,
        ).iloc[0]["count"]

        if sev3_failures > 0:
            mart_statuses[mart] = "warn"

    conn.close()
    return mart_statuses


def get_outstanding_errors() -> DataFrame:
    conn = get_db_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            UNIQUE_ID, SEVERITY_LEVEL, DATABASE_NAME, TABLE_NAME, TEST_COLUMN_NAME, 
            TEST_ORIGINAL_NAME, TEST_TYPE, TEST_SUB_TYPE, TEST_DESCRIPTION,
            TEST_RESULTS_QUERY, STATUS,
            FLAG_SERVICE_CATEGORIES, FLAG_CCSR, FLAG_CMS_CHRONIC_CONDITIONS,
            FLAG_TUVA_CHRONIC_CONDITIONS, FLAG_CMS_HCCS, FLAG_ED_CLASSIFICATION,
            FLAG_FINANCIAL_PMPM, FLAG_QUALITY_MEASURES, FLAG_READMISSION
        FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL IS NOT NULL
        ORDER BY SEVERITY_LEVEL ASC
    """,
        conn,
    )
    conn.close()
    return df


def get_data_availability() -> dict:
    """Check what data is available in the database."""
    conn = get_db_connection()

    # Check for test results
    test_results_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results
    """,
        conn,
    ).iloc[0]["count"]

    # Check for chart data
    chart_data_count = (
        pd.read_sql_query(
            """
        SELECT COUNT(*) as count FROM chart_data
    """,
            conn,
        ).iloc[0]["count"]
        if table_exists(conn, "chart_data")
        else 0
    )

    # Get chart categories if available
    chart_categories = (
        pd.read_sql_query(
            """
        SELECT DISTINCT DATA_QUALITY_CATEGORY, COUNT(*) as count
        FROM chart_data
        GROUP BY DATA_QUALITY_CATEGORY
    """,
            conn,
        )
        if table_exists(conn, "chart_data")
        else pd.DataFrame()
    )

    conn.close()

    return {
        "test_results": test_results_count,
        "chart_data": chart_data_count,
        "chart_categories": chart_categories.to_dict("records")
        if not chart_categories.empty
        else [],
    }


def table_exists(conn, table_name: str) -> bool:
    """Check if a table exists in the database."""
    query = f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}'
    """
    result = conn.execute(query).fetchone()
    return result is not None


def get_all_tests() -> DataFrame:
    """Get all tests from the database with their status."""
    conn = get_db_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            UNIQUE_ID, 
            CAST(SEVERITY_LEVEL AS INTEGER) AS SEVERITY_LEVEL, 
            DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, 
            TEST_COLUMN_NAME, TEST_ORIGINAL_NAME, TEST_TYPE, TEST_SUB_TYPE, 
            TEST_DESCRIPTION, STATUS, QUALITY_DIMENSION, TEST_CATEGORY,
            FLAG_SERVICE_CATEGORIES, FLAG_CCSR, FLAG_CMS_CHRONIC_CONDITIONS,
            FLAG_TUVA_CHRONIC_CONDITIONS, FLAG_CMS_HCCS, FLAG_ED_CLASSIFICATION,
            FLAG_FINANCIAL_PMPM, FLAG_QUALITY_MEASURES, FLAG_READMISSION
        FROM test_results 
       
        ORDER BY SEVERITY_LEVEL ASC, STATUS DESC, TABLE_NAME ASC
    """,
        conn,
    )
    conn.close()
    return df


def get_mart_test_summary() -> list:
    """Get a summary of tests by data mart."""
    conn = get_db_connection()

    # Create a list to store results for each mart
    mart_summaries = []

    # List of all data marts
    marts = [
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

    # For each mart, get counts of tests by severity and status
    for mart in marts:
        flag_column = f"FLAG_{mart}"

        # Get total tests, passing tests, and failing tests by severity
        query = f"""
            SELECT 
                COUNT(*) as total_tests,
                SUM(CASE WHEN STATUS = 'pass' THEN 1 ELSE 0 END) as passing_tests,
                SUM(CASE WHEN STATUS != 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) = 1 THEN 1 ELSE 0 END) as sev1_fails,
                SUM(CASE WHEN STATUS != 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) = 2 THEN 1 ELSE 0 END) as sev2_fails,
                SUM(CASE WHEN STATUS != 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) = 3 THEN 1 ELSE 0 END) as sev3_fails,
                SUM(CASE WHEN STATUS != 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) = 4 THEN 1 ELSE 0 END) as sev4_fails,
                SUM(CASE WHEN STATUS != 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) = 5 THEN 1 ELSE 0 END) as sev5_fails
            FROM test_results 
            WHERE {flag_column} = 1
        """

        result = pd.read_sql_query(query, conn).iloc[0]

        # Convert None values to 0 for numeric fields
        sev1_fails = 0 if result["sev1_fails"] is None else int(result["sev1_fails"])
        sev2_fails = 0 if result["sev2_fails"] is None else int(result["sev2_fails"])
        sev3_fails = 0 if result["sev3_fails"] is None else int(result["sev3_fails"])
        sev4_fails = 0 if result["sev4_fails"] is None else int(result["sev4_fails"])
        sev5_fails = 0 if result["sev5_fails"] is None else int(result["sev5_fails"])
        total_tests = 0 if result["total_tests"] is None else int(result["total_tests"])
        passing_tests = (
            0 if result["passing_tests"] is None else int(result["passing_tests"])
        )

        # Format the display name
        display_name = (
            mart.replace("_", " ")
            .title()
            .replace("Ccsr", "CCSR")
            .replace("Cms", "CMS")
            .replace("Ed", "ED")
            .replace("Pmpm", "PMPM")
        )

        # Calculate passing percentage
        passing_pct = 0
        if total_tests > 0:
            passing_pct = round(passing_tests / total_tests * 100, 1)

        # Determine status based on severity counts
        if sev1_fails > 0:
            status = "Not Usable"
            status_color = "danger"
        elif sev2_fails > 0:
            status = "Not Usable"
            status_color = "danger"
        elif sev3_fails > 0:
            status = "Use with Caution"
            status_color = "warning"
        else:
            status = "Usable"
            status_color = "success"

        # Add to summary list
        mart_summaries.append(
            {
                "mart": mart,
                "display_name": display_name,
                "total_tests": total_tests,
                "passing_tests": passing_tests,
                "passing_percentage": passing_pct,
                "sev1_fails": sev1_fails,
                "sev2_fails": sev2_fails,
                "sev3_fails": sev3_fails,
                "sev4_fails": sev4_fails,
                "sev5_fails": sev5_fails,
                "status": status,
                "status_color": status_color,
            }
        )

    conn.close()
    return mart_summaries


def get_test_category_summary() -> DataFrame:
    """Get a summary of tests by Test Category."""
    conn = get_db_connection()

    # Get counts by Test Category and status
    query = """
        SELECT 
            TEST_CATEGORY,
            COUNT(*) as total_tests,
            SUM(CASE WHEN STATUS = 'pass' THEN 1 ELSE 0 END) as passing_tests,
            SUM(CASE WHEN STATUS != 'pass' THEN 1 ELSE 0 END) as failing_tests
        FROM test_results 
        WHERE TEST_CATEGORY IS NOT NULL AND TEST_CATEGORY != ''
        GROUP BY TEST_CATEGORY
        ORDER BY TEST_CATEGORY
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Calculate passing percentages
    if not df.empty:
        df["passing_percentage"] = (
            df["passing_tests"] / df["total_tests"] * 100
        ).round(1)

    return df


def get_mart_tests(mart_name, status: str = None) -> DataFrame:
    """Get tests for a specific mart."""
    conn = get_db_connection()
    flag_column = f"FLAG_{mart_name}"

    # Check if the column exists
    column_exists = False
    try:
        # Try to get column info
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(test_results)")
        columns = cursor.fetchall()
        column_exists = any(col[1] == flag_column for col in columns)
    except Exception:
        pass

    # If column doesn't exist, return empty DataFrame
    if not column_exists:
        return pd.DataFrame(
            columns=[
                "UNIQUE_ID",
                "SEVERITY_LEVEL",
                "DATABASE_NAME",
                "TABLE_NAME",
                "TEST_COLUMN_NAME",
                "TEST_ORIGINAL_NAME",
                "TEST_TYPE",
                "TEST_SUB_TYPE",
                "TEST_DESCRIPTION",
                "TEST_RESULTS_QUERY",
                "STATUS",
            ]
        )

    query = f"""
        SELECT 
            UNIQUE_ID, SEVERITY_LEVEL, DATABASE_NAME, TABLE_NAME, TEST_COLUMN_NAME, 
            TEST_ORIGINAL_NAME, TEST_TYPE, TEST_SUB_TYPE, TEST_DESCRIPTION,
            TEST_RESULTS_QUERY, STATUS
        FROM test_results 
        WHERE {flag_column} = 1
    """

    if status:
        query += f" AND STATUS = '{status}'"
    else:
        query += " AND STATUS != 'pass'"

    query += " ORDER BY SEVERITY_LEVEL ASC"

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

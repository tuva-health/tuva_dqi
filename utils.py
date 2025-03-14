import base64
import io
import json
import sqlite3
import traceback

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dash_table, dcc, html


def get_mart_tests(mart_name, status=None):
    """Get tests for a specific mart."""
    conn = get_db_connection()
    flag_column = f"FLAG_{mart_name}"

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


def get_db_connection():
    conn = sqlite3.connect("app_data.db")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
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


def parse_chart_data_contents(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)

    try:
        if "csv" in filename:
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))

            # Check if this is a chart data file or test results file
            if "DATA_QUALITY_CATEGORY" in df.columns and "GRAPH_NAME" in df.columns:
                # This is a chart data file
                conn = get_db_connection()

                # Use pandas to_sql with 'replace' to overwrite existing data
                df.to_sql("chart_data", conn, if_exists="replace", index=False)
                conn.close()

                return html.Div(
                    [
                        html.H5(f"Uploaded Chart Data: {filename}"),
                        html.Hr(),
                        html.P(
                            f"{len(df)} data points imported successfully to database."
                        ),
                        html.P(f'Detected {df["GRAPH_NAME"].nunique()} unique charts.'),
                        dash_table.DataTable(
                            data=df.head(10).to_dict("records"),
                            columns=[{"name": i, "id": i} for i in df.columns],
                            page_size=10,
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                                "maxWidth": 0,
                            },
                        ),
                    ]
                )
            elif "UNIQUE_ID" in df.columns and "TEST_NAME" in df.columns:
                # This is a test results file
                conn = get_db_connection()
                # Use pandas to_sql with 'replace' to overwrite existing data
                df.to_sql("test_results", conn, if_exists="replace", index=False)
                conn.close()

                return html.Div(
                    [
                        html.H5(f"Uploaded Test Results: {filename}"),
                        html.Hr(),
                        html.P(
                            f"{len(df)} test results imported successfully to database."
                        ),
                        dash_table.DataTable(
                            data=df.head(10).to_dict("records"),
                            columns=[{"name": i, "id": i} for i in df.columns],
                            page_size=10,
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                                "maxWidth": 0,
                            },
                        ),
                    ]
                )
            else:
                return html.Div(
                    [
                        html.H5(f"Uploaded: {filename}"),
                        html.Hr(),
                        html.P(
                            "Unrecognized CSV format. Please upload either a test results file or chart data file."
                        ),
                    ]
                )
        else:
            return html.Div(
                [
                    html.H5(f"Uploaded: {filename}"),
                    html.Hr(),
                    html.P("Only CSV files are supported."),
                ]
            )
    except Exception as e:
        return html.Div(
            [
                html.H5(f"Error processing {filename}"),
                html.Hr(),
                html.P(f"Error: {str(e)}"),
                html.Pre(traceback.format_exc()),
            ]
        )


def get_available_charts():
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


def get_chart_data(graph_name, chart_filter=None):
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


def get_chart_filter_values(graph_name):
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


def create_chart(graph_name, chart_filter=None):
    """Create a plotly figure for the specified chart."""
    # Get data for this chart
    df = get_chart_data(graph_name, chart_filter)

    if df.empty:
        # Return a message if no data is available
        return html.Div("No data available for this chart")

    # Get chart metadata from the first row
    metadata = df.iloc[0]

    # Format the title
    title = f"{metadata['DATA_QUALITY_CATEGORY'].title()}: {graph_name.replace('_', ' ').title()}"
    if chart_filter and metadata["FILTER_DESCRIPTION"] != "N/A":
        title += f" ({metadata['FILTER_DESCRIPTION']}: {chart_filter})"

    # Check if this is a matrix chart (from the name)
    is_matrix_chart = "matrix" in graph_name.lower()

    # For matrix charts, if X-axis description is not N/A but X-axis values are null, show error
    if (
        is_matrix_chart
        and metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and all(pd.isna(df["X_AXIS"]))
    ):
        return html.Div(
            [
                html.H5(title, className="text-center"),
                html.Div(
                    "Chart cannot be rendered because X axis values are missing.",
                    className="alert alert-warning",
                ),
            ]
        )

    # Check if this is a time series chart (special case)
    is_time_series = False
    if (
        "date" in metadata["X_AXIS_DESCRIPTION"].lower()
        or "date" in metadata["Y_AXIS_DESCRIPTION"].lower()
    ):
        is_time_series = True
        # Convert date strings to datetime objects for proper sorting
        if not all(pd.isna(df["X_AXIS"])):
            df["X_AXIS"] = pd.to_datetime(df["X_AXIS"], errors="coerce")
            df = df.sort_values("X_AXIS")
        if not all(pd.isna(df["Y_AXIS"])):
            df["Y_AXIS"] = pd.to_datetime(df["Y_AXIS"], errors="coerce")
            df = df.sort_values("Y_AXIS")

    # Case 1: Both X and Y axes have values - create a matrix/table view
    if (
        metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and metadata["Y_AXIS_DESCRIPTION"] != "N/A"
        and not all(pd.isna(df["X_AXIS"]))
        and not all(pd.isna(df["Y_AXIS"]))
    ):
        try:
            # Create a pivot table
            pivot_df = df.pivot_table(
                values="VALUE", index="Y_AXIS", columns="X_AXIS", aggfunc="first"
            ).reset_index()

            # Create a table figure
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=[metadata["Y_AXIS_DESCRIPTION"]]
                            + pivot_df.columns.tolist()[1:],
                            fill_color="paleturquoise",
                            align="left",
                        ),
                        cells=dict(
                            values=[pivot_df["Y_AXIS"]]
                            + [pivot_df[col] for col in pivot_df.columns[1:]],
                            fill_color="lavender",
                            align="left",
                        ),
                    )
                ]
            )

            fig.update_layout(title=title)
            return dcc.Graph(figure=fig)
        except Exception:
            # If pivot fails, fall back to a standard bar chart
            pass

    # Case 2: X axis has values, Y axis is empty or N/A - create a bar chart with X axis
    elif metadata["X_AXIS_DESCRIPTION"] != "N/A" and not all(pd.isna(df["X_AXIS"])):
        fig = px.bar(
            df,
            x="X_AXIS",
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                "X_AXIS": metadata["X_AXIS_DESCRIPTION"],
            },
        )

    # Case 3: Y axis has values, X axis is empty or N/A - create a bar chart with Y axis as X
    elif (
        metadata["Y_AXIS_DESCRIPTION"] != "N/A"
        and not all(pd.isna(df["Y_AXIS"]))
        and not is_matrix_chart
    ):
        fig = px.bar(
            df,
            x="Y_AXIS",  # Use Y_AXIS for the X-axis of the chart
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                "Y_AXIS": metadata["Y_AXIS_DESCRIPTION"],
            },
        )

    # Case 4: For non-matrix charts, if X axis description is not N/A but values are null
    elif (
        not is_matrix_chart
        and metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and all(pd.isna(df["X_AXIS"]))
    ):
        # For time series charts, use the X_AXIS column in X_AXIS_DESCRIPTION
        if "over_time" in graph_name.lower():
            fig = px.bar(
                df,
                x="X_AXIS",  # This will be blank but we'll use the description
                y="VALUE",
                title=title,
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"]
                    if pd.notna(metadata["SUM_DESCRIPTION"])
                    else "Value",
                    "X_AXIS": metadata["X_AXIS_DESCRIPTION"],
                },
            )
        else:
            # For other charts, use Y_AXIS as X
            fig = px.bar(
                df,
                x="Y_AXIS",
                y="VALUE",
                title=title,
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"]
                    if pd.notna(metadata["SUM_DESCRIPTION"])
                    else "Value",
                    "Y_AXIS": metadata["Y_AXIS_DESCRIPTION"]
                    if metadata["Y_AXIS_DESCRIPTION"] != "N/A"
                    else metadata["X_AXIS_DESCRIPTION"],
                },
            )

    # Fallback case: Use whatever axis has values
    else:
        # Determine which column to use for the x-axis
        if not all(pd.isna(df["X_AXIS"])):
            x_col = "X_AXIS"
            x_label = (
                metadata["X_AXIS_DESCRIPTION"]
                if metadata["X_AXIS_DESCRIPTION"] != "N/A"
                else "X Axis"
            )
        elif not all(pd.isna(df["Y_AXIS"])) and not is_matrix_chart:
            x_col = "Y_AXIS"
            x_label = (
                metadata["Y_AXIS_DESCRIPTION"]
                if metadata["Y_AXIS_DESCRIPTION"] != "N/A"
                else "Y Axis"
            )
        else:
            # If both axes are empty or it's a matrix chart with missing X values, show error
            return html.Div(
                [
                    html.H5(title, className="text-center"),
                    html.Div(
                        "Chart cannot be rendered because necessary axis values are missing.",
                        className="alert alert-warning",
                    ),
                ]
            )

        # Create a simple bar chart
        fig = px.bar(
            df,
            x=x_col,
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                x_col: x_label,
            },
        )

    # Improve layout
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=40, t=60, b=40),
    )

    # For time series data, format the x-axis to show dates properly
    if is_time_series:
        fig.update_xaxes(tickformat="%b %Y", tickangle=45)

    return dcc.Graph(figure=fig)


def get_data_from_test_results(limit=100):
    conn = get_db_connection()
    df = pd.read_sql_query(
        f"SELECT * FROM test_results WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5 LIMIT {limit}",
        conn,
    )
    conn.close()
    return df


def get_data_quality_grade():
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


def get_tests_completed_count():
    conn = get_db_connection()
    count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
    """,
        conn,
    ).iloc[0]["count"]
    conn.close()
    return count


def get_last_test_run_time():
    conn = get_db_connection()
    last_time = pd.read_sql_query(
        """
        SELECT MAX(GENERATED_AT) as last_run FROM test_results WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
    """,
        conn,
    ).iloc[0]["last_run"]
    conn.close()
    return last_time if last_time else "No data available"


def get_mart_statuses():
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


def get_outstanding_errors():
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
        AND CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
        ORDER BY SEVERITY_LEVEL ASC
    """,
        conn,
    )
    conn.close()
    return df


def create_test_table(df, table_type):
    if df.empty:
        return html.P(f"No {table_type} tests found for this data mart.")

    # Store the full dataframe in a hidden div for later use
    hidden_data = html.Div(
        id=f"hidden-{table_type}-data",
        style={"display": "none"},
        children=json.dumps(df.to_dict("records")),
    )

    # Create a list of rows with buttons
    rows = []
    for i, row in df.iterrows():
        if i >= 10:  # Only create the first 10 rows initially
            break

        severity_level = (
            int(row["SEVERITY_LEVEL"]) if pd.notna(row["SEVERITY_LEVEL"]) else 0
        )

        # Different styling for passing vs failing tests
        if table_type == "passing":
            # For passing tests, use a left border with severity color
            row_style = {
                "className": f"mb-2 border-bottom pb-2 passing-test-row passing-severity-{severity_level}"
            }
        else:
            # For failing tests, use the background color approach
            row_style = {
                "className": "mb-2 border-bottom pb-2",
                "style": {
                    "backgroundColor": "#ffcccc"
                    if severity_level == 1
                    else "#ffe6cc"
                    if severity_level == 2
                    else "#ffffcc"
                    if severity_level == 3
                    else "#e6ffcc"
                    if severity_level == 4
                    else "#ccffcc"
                    if severity_level == 5
                    else "white"
                },
            }

        rows.append(
            dbc.Row(
                [
                    dbc.Col(
                        str(severity_level),
                        width=1,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        row["TABLE_NAME"],
                        width=2,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        row["TEST_COLUMN_NAME"],
                        width=2,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        row["TEST_ORIGINAL_NAME"],
                        width=2,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        row["TEST_TYPE"],
                        width=2,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        row["TEST_SUB_TYPE"] if pd.notna(row["TEST_SUB_TYPE"]) else "",
                        width=2,
                        className="align-self-center table-cell",
                    ),
                    dbc.Col(
                        dbc.Button(
                            "More Info",
                            id={"type": f"{table_type}-info-button", "index": i},
                            color="info",
                            size="sm",
                            className="my-1",
                        ),
                        width=1,
                        className="d-flex align-items-center",
                    ),
                ],
                **row_style,
            )
        )

    # Create a header row
    header = dbc.Row(
        [
            dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
            dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
        ],
        className="mb-2 border-bottom pb-2 font-weight-bold",
    )

    # Combine header and rows
    table_content = [header] + rows

    # Create a container for the table with pagination
    table_container = html.Div(
        [
            hidden_data,
            html.Div(
                table_content, id=f"{table_type}-tests-table-content"
            ),  # Add an ID for updating
            dbc.Pagination(
                id=f"{table_type}-pagination",
                max_value=max(
                    1, (len(df) + 9) // 10
                ),  # Ceiling division to get number of pages
                first_last=True,
                previous_next=True,
                active_page=1,
            )
            if len(df) > 10
            else html.Div(),
        ]
    )

    return table_container


def create_test_modal_content(row):
    """Helper function to create modal content for a test."""
    # Convert severity to integer if it exists
    severity_level = (
        int(row["SEVERITY_LEVEL"]) if pd.notna(row["SEVERITY_LEVEL"]) else None
    )

    modal_content = [
        html.H5(f"Test Details for {row['TABLE_NAME']}"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(
                    [
                        # Add word-break for long IDs
                        html.P(
                            [
                                html.Strong("Unique ID: "),
                                html.Span(
                                    row["UNIQUE_ID"], style={"word-break": "break-all"}
                                ),
                            ]
                        ),
                        html.P(
                            [
                                html.Strong("Severity Level: "),
                                str(severity_level)
                                if severity_level is not None
                                else "N/A",
                            ]
                        ),
                        # Add database name
                        html.P([html.Strong("Database: "), row["DATABASE_NAME"]]),
                        html.P([html.Strong("Table: "), row["TABLE_NAME"]]),
                        html.P([html.Strong("Column: "), row["TEST_COLUMN_NAME"]]),
                        html.P([html.Strong("Test Name: "), row["TEST_ORIGINAL_NAME"]]),
                        html.P([html.Strong("Test Type: "), row["TEST_TYPE"]]),
                        html.P(
                            [
                                html.Strong("Test Sub Type: "),
                                row["TEST_SUB_TYPE"]
                                if pd.notna(row["TEST_SUB_TYPE"])
                                else "",
                            ]
                        ),
                        # Only add Status if it exists in the row
                        html.P([html.Strong("Status: "), row["STATUS"]])
                        if "STATUS" in row
                        else None,
                    ],
                    width=12,
                ),
            ]
        ),
        html.Hr(),
        html.H6("Test Description:"),
        html.P(
            row["TEST_DESCRIPTION"]
            if pd.notna(row["TEST_DESCRIPTION"])
            else "No description available"
        ),
        html.Hr(),
        html.H6("Test Results Query:"),
        html.Div(
            [
                dbc.Textarea(
                    id="query-text",
                    value=row["TEST_RESULTS_QUERY"]
                    if pd.notna(row["TEST_RESULTS_QUERY"])
                    else "No query available",
                    readOnly=True,
                    style={"height": "200px", "fontFamily": "monospace"},
                ),
                dbc.Button(
                    "Copy to Clipboard",
                    id="copy-query-button",
                    color="secondary",
                    className="mt-2",
                    n_clicks=0,
                ),
                html.Div(id="copy-query-output"),
            ]
        ),
        html.Hr(),
    ]

    # Filter out any None values from the modal content
    modal_content = [item for item in modal_content if item is not None]

    return modal_content


def get_data_availability():
    """Check what data is available in the database."""
    conn = get_db_connection()

    # Check for test results
    test_results_count = pd.read_sql_query(
        """
        SELECT COUNT(*) as count FROM test_results WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
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


def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    query = f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}'
    """
    result = conn.execute(query).fetchone()
    return result is not None


def get_all_tests():
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
        WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
        ORDER BY SEVERITY_LEVEL ASC, STATUS DESC, TABLE_NAME ASC
    """,
        conn,
    )
    conn.close()
    return df


def get_mart_test_summary():
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
            AND CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
        """

        result = pd.read_sql_query(query, conn).iloc[0]

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
        if result["total_tests"] > 0:
            passing_pct = (result["passing_tests"] / result["total_tests"] * 100).round(
                1
            )

        # Determine status based on severity counts
        if result["sev1_fails"] > 0:
            status = "Not Usable"
            status_color = "danger"
        elif result["sev2_fails"] > 0:
            status = "Not Usable"
            status_color = "danger"
        elif result["sev3_fails"] > 0:
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
                "total_tests": result["total_tests"],
                "passing_tests": result["passing_tests"],
                "passing_percentage": passing_pct,
                "sev1_fails": result["sev1_fails"],
                "sev2_fails": result["sev2_fails"],
                "sev3_fails": result["sev3_fails"],
                "sev4_fails": result["sev4_fails"],
                "sev5_fails": result["sev5_fails"],
                "status": status,
                "status_color": status_color,
            }
        )

    conn.close()
    return mart_summaries


def get_quality_dimension_summary():
    """Get a summary of tests by quality dimension."""
    conn = get_db_connection()

    # Get counts by quality dimension and status
    query = """
        SELECT 
            QUALITY_DIMENSION,
            COUNT(*) as total_tests,
            SUM(CASE WHEN STATUS = 'pass' THEN 1 ELSE 0 END) as passing_tests,
            SUM(CASE WHEN STATUS != 'pass' THEN 1 ELSE 0 END) as failing_tests
        FROM test_results 
        WHERE QUALITY_DIMENSION IS NOT NULL AND QUALITY_DIMENSION != ''
        GROUP BY QUALITY_DIMENSION
        ORDER BY QUALITY_DIMENSION
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Calculate passing percentages
    if not df.empty:
        df["passing_percentage"] = (
            df["passing_tests"] / df["total_tests"] * 100
        ).round(1)

    return df

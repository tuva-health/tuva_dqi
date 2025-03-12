import dash
from dash import html, dcc, callback, Input, Output, State, dash_table, ctx
import dash_bootstrap_components as dbc
import json
import pandas as pd
import sqlite3
import os
import io
import base64
import traceback

# Register the page
dash.register_page(__name__, path='/analytics', name='DQI Dashboard')


# Function to get tests for a specific mart
def get_mart_tests(mart_name, status=None):
    conn = get_db_connection()
    flag_column = f'FLAG_{mart_name}'

    query = f"""
        SELECT 
            UNIQUE_ID, SEVERITY_LEVEL, DATABASE_NAME, TABLE_NAME, TEST_COLUMN_NAME, 
            TEST_ORIGINAL_NAME, TEST_TYPE, TEST_SUB_TYPE, TEST_DESCRIPTION,
            TEST_RESULTS_QUERY, RESULT_ROWS, STATUS
        FROM test_results 
        WHERE {flag_column} = 1
    """

    if status:
        query += f" AND STATUS = '{status}'"
    else:
        query += f" AND STATUS != 'pass'"

    query += " ORDER BY SEVERITY_LEVEL ASC"

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Create a SQLite database connection
def get_db_connection():
    conn = sqlite3.connect('app_data.db')
    conn.row_factory = sqlite3.Row
    return conn


# Initialize the database (create tables if they don't exist)
def init_db():
    conn = get_db_connection()
    conn.execute('''
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
        RESULT_ROWS TEXT,
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
    ''')
    conn.commit()
    conn.close()

# Initialize the database when the module is loaded
init_db()

def get_test_category_stats():
    conn = get_db_connection()

    # Get all test categories and their counts
    categories_df = pd.read_sql_query("""
        SELECT 
            TEST_CATEGORY, 
            COUNT(*) as total_tests,
            SUM(CASE WHEN STATUS = 'pass' THEN 1 ELSE 0 END) as passing_tests
        FROM test_results 
        WHERE TEST_CATEGORY IS NOT NULL AND TEST_CATEGORY != ''
        GROUP BY TEST_CATEGORY
        ORDER BY TEST_CATEGORY
    """, conn)

    conn.close()

    # Calculate passing percentages
    if not categories_df.empty:
        categories_df['passing_percentage'] = (categories_df['passing_tests'] /
                                               categories_df['total_tests'] * 100).round(1)

    return categories_df


# Function to parse the contents of an uploaded file
def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        if 'csv' in filename:
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

            # Insert data into SQLite database
            conn = get_db_connection()
            # Use pandas to_sql with 'replace' if you want to overwrite existing data
            # or 'append' to add to existing data
            df.to_sql('test_results', conn, if_exists='replace', index=False)
            conn.close()

            return html.Div([
                html.H5(f'Uploaded: {filename}'),
                html.Hr(),
                html.P(f'{len(df)} rows imported successfully to database.'),
                dash_table.DataTable(
                    data=df.head(10).to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    page_size=10,
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                        'maxWidth': 0,
                    }
                )
            ])
        else:
            return html.Div([
                html.H5(f'Uploaded: {filename}'),
                html.Hr(),
                html.P('Only CSV files are supported.')
            ])
    except Exception as e:
        return html.Div([
            html.H5(f'Error processing {filename}'),
            html.Hr(),
            html.P(f'Error: {str(e)}')
        ])


# Function to get data from the database
def get_data_from_db(limit=100):
    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM test_results LIMIT {limit}", conn)
    conn.close()
    return df

def get_data_quality_grade():
    conn = get_db_connection()

    # Check for Sev 1 issues (status is not 'pass' and SEVERITY_LEVEL = 1)
    sev1_count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 1
    """, conn).iloc[0]['count']

    # Check for Sev 2 issues
    sev2_count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 2
    """, conn).iloc[0]['count']

    # Check for Sev 3 issues
    sev3_count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 3
    """, conn).iloc[0]['count']

    # Check for Sev 4 issues
    sev4_count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 4
    """, conn).iloc[0]['count']

    # Check for Sev 5 issues
    sev5_count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 5
    """, conn).iloc[0]['count']

    conn.close()

    # Determine grade based on severity counts
    if sev1_count > 0:
        return 'F'
    elif sev2_count > 0:
        return 'D'
    elif sev3_count > 0:
        return 'C'
    elif sev4_count > 0:
        return 'B'
    else:  # sev5_count > 0 or all tests pass
        return 'A'


def get_tests_completed_count():
    conn = get_db_connection()
    count = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results
    """, conn).iloc[0]['count']
    conn.close()
    return count


def get_last_test_run_time():
    conn = get_db_connection()
    last_time = pd.read_sql_query("""
        SELECT MAX(GENERATED_AT) as last_run FROM test_results
    """, conn).iloc[0]['last_run']
    conn.close()
    return last_time if last_time else "No data available"

def get_mart_statuses():
    conn = get_db_connection()

    # Get all failed tests with severity level 1
    sev1_failures = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 1
    """, conn).iloc[0]['count']

    # Initialize results dictionary
    mart_statuses = {
        'SERVICE_CATEGORIES': 'pass',
        'CCSR': 'pass',
        'CMS_CHRONIC_CONDITIONS': 'pass',
        'TUVA_CHRONIC_CONDITIONS': 'pass',
        'CMS_HCCS': 'pass',
        'ED_CLASSIFICATION': 'pass',
        'FINANCIAL_PMPM': 'pass',
        'QUALITY_MEASURES': 'pass',
        'READMISSION': 'pass'
    }

    # If any severity level 1 issues exist, all marts are not usable
    if sev1_failures > 0:
        for mart in mart_statuses:
            mart_statuses[mart] = 'fail'
        conn.close()
        return mart_statuses

    # Check severity level 2 issues for each mart
    for mart in mart_statuses:
        flag_column = f'FLAG_{mart}'

        # Check severity level 2 issues for this mart
        sev2_failures = pd.read_sql_query(f"""
            SELECT COUNT(*) as count FROM test_results 
            WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 2 AND {flag_column} = 1
        """, conn).iloc[0]['count']

        if sev2_failures > 0:
            mart_statuses[mart] = 'fail'
            continue  # No need to check level 3 if already failed

        # Check severity level 3 issues for this mart
        sev3_failures = pd.read_sql_query(f"""
            SELECT COUNT(*) as count FROM test_results 
            WHERE STATUS != 'pass' AND SEVERITY_LEVEL = 3 AND {flag_column} = 1
        """, conn).iloc[0]['count']

        if sev3_failures > 0:
            mart_statuses[mart] = 'warn'

    conn.close()
    return mart_statuses

def get_outstanding_errors():
    conn = get_db_connection()
    df = pd.read_sql_query("""
        SELECT 
            UNIQUE_ID, SEVERITY_LEVEL, DATABASE_NAME, TABLE_NAME, TEST_COLUMN_NAME, 
            TEST_ORIGINAL_NAME, TEST_TYPE, TEST_SUB_TYPE, TEST_DESCRIPTION,
            TEST_RESULTS_QUERY, RESULT_ROWS, STATUS,
            FLAG_SERVICE_CATEGORIES, FLAG_CCSR, FLAG_CMS_CHRONIC_CONDITIONS,
            FLAG_TUVA_CHRONIC_CONDITIONS, FLAG_CMS_HCCS, FLAG_ED_CLASSIFICATION,
            FLAG_FINANCIAL_PMPM, FLAG_QUALITY_MEASURES, FLAG_READMISSION
        FROM test_results 
        WHERE STATUS != 'pass' AND SEVERITY_LEVEL IS NOT NULL
        ORDER BY SEVERITY_LEVEL ASC
    """, conn)
    conn.close()
    return df


def create_test_table(df, table_type):
    if df.empty:
        return html.P(f"No {table_type} tests found for this data mart.")

    # Store the full dataframe in a hidden div for later use
    hidden_data = html.Div(
        id=f'hidden-{table_type}-data',
        style={'display': 'none'},
        children=json.dumps(df.to_dict('records'))
    )

    # Create a list of rows with buttons
    rows = []
    for i, row in df.iterrows():
        if i >= 10:  # Only create the first 10 rows initially
            break

        severity_level = int(row['SEVERITY_LEVEL']) if pd.notna(row['SEVERITY_LEVEL']) else 0

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
                    "backgroundColor":
                        "#ffcccc" if severity_level == 1 else
                        "#ffe6cc" if severity_level == 2 else
                        "#ffffcc" if severity_level == 3 else
                        "#e6ffcc" if severity_level == 4 else
                        "#ccffcc" if severity_level == 5 else
                        "white"
                }
            }

        rows.append(
            dbc.Row([
                dbc.Col(str(severity_level), width=1, className="align-self-center table-cell"),
                dbc.Col(row['TABLE_NAME'], width=2, className="align-self-center table-cell"),
                dbc.Col(row['TEST_COLUMN_NAME'], width=2, className="align-self-center table-cell"),
                dbc.Col(row['TEST_ORIGINAL_NAME'], width=2, className="align-self-center table-cell"),
                dbc.Col(row['TEST_TYPE'], width=2, className="align-self-center table-cell"),
                dbc.Col(row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '', width=2,
                        className="align-self-center table-cell"),
                dbc.Col(
                    dbc.Button(
                        "More Info",
                        id={"type": f"{table_type}-info-button", "index": i},
                        color="info",
                        size="sm",
                        className="my-1"
                    ),
                    width=1,
                    className="d-flex align-items-center"
                ),
            ],
                **row_style
            )
        )

    # Create a header row
    header = dbc.Row([
        dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
        dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
        dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
        dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
        dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
        dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
        dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
    ], className="mb-2 border-bottom pb-2 font-weight-bold")

    # Combine header and rows
    table_content = [header] + rows

    # Create a container for the table with pagination
    table_container = html.Div([
        hidden_data,
        html.Div(table_content, id=f'{table_type}-tests-table-content'),  # Add an ID for updating
        dbc.Pagination(
            id=f"{table_type}-pagination",
            max_value=max(1, (len(df) + 9) // 10),  # Ceiling division to get number of pages
            first_last=True,
            previous_next=True,
            active_page=1
        ) if len(df) > 10 else html.Div()
    ])

    return table_container


# Helper function to create modal content for a test
def create_test_modal_content(row):
    # Convert severity to integer if it exists
    severity_level = int(row['SEVERITY_LEVEL']) if pd.notna(row['SEVERITY_LEVEL']) else None

    modal_content = [
        html.H5(f"Test Details for {row['TABLE_NAME']}"),
        html.Hr(),

        dbc.Row([
            dbc.Col([
                # Add word-break for long IDs
                html.P([html.Strong("Unique ID: "),
                        html.Span(row['UNIQUE_ID'], style={"word-break": "break-all"})]),
                html.P([html.Strong("Severity Level: "),
                        str(severity_level) if severity_level is not None else "N/A"]),
                # Add database name
                html.P([html.Strong("Database: "), row['DATABASE_NAME']]),
                html.P([html.Strong("Table: "), row['TABLE_NAME']]),
                html.P([html.Strong("Column: "), row['TEST_COLUMN_NAME']]),
                html.P([html.Strong("Test Name: "), row['TEST_ORIGINAL_NAME']]),
                html.P([html.Strong("Test Type: "), row['TEST_TYPE']]),
                html.P([html.Strong("Test Sub Type: "),
                        row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '']),
                # Only add Status if it exists in the row
                html.P([html.Strong("Status: "), row['STATUS']]) if 'STATUS' in row else None,
            ], width=12),
        ]),

        html.Hr(),
        html.H6("Test Description:"),
        html.P(row['TEST_DESCRIPTION'] if pd.notna(row['TEST_DESCRIPTION']) else "No description available"),

        html.Hr(),
        html.H6("Test Results Query:"),
        html.Div([
            dbc.Textarea(
                id="query-text",
                value=row['TEST_RESULTS_QUERY'] if pd.notna(row['TEST_RESULTS_QUERY']) else "No query available",
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
        ]),

        html.Hr(),
        html.H6("Result Rows:"),
        html.P(row['RESULT_ROWS'] if pd.notna(row['RESULT_ROWS']) else "No result rows available"),
    ]

    # Filter out any None values from the modal content
    modal_content = [item for item in modal_content if item is not None]

    return modal_content


# Layout with Bootstrap cards/tiles
layout = html.Div([
    html.H1('Data Quality Results Dashboard', className='mb-4'),

    # First row of tiles
    dbc.Row([
        # File Upload Tile
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Import Test Results", className="bg-primary text-white"),
                dbc.CardBody([
                    html.P("Upload a CSV file to import into the database:"),
                    dcc.Upload(
                        id='upload-data',
                        children=html.Div([
                            'Drag and Drop or ',
                            html.A('Select Files', className="upload-link")
                        ], className="upload-text"),
                        style={
                            'width': '100%',
                            'height': '60px',
                            'lineHeight': '60px',
                            'borderWidth': '1px',
                            'borderStyle': 'dashed',
                            'borderRadius': '5px',
                            'textAlign': 'center',
                            'margin': '10px'
                        },
                        className="upload-area",
                        multiple=False
                    ),
                    html.Div(id='output-data-upload')
                ])
            ], className="mb-4"),
            width=12
        ),
    ]),

    # First row summary tiles
    dbc.Row([
        # Data Quality Grade Tile
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Data Quality Grade", className="bg-primary text-white"),
                dbc.CardBody([
                    html.Div(id='data-quality-grade', className="text-center display-4")
                ])
            ], className="mb-4"),
            width=4
        ),

        # Tests Completed Tile
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Tests Completed", className="bg-primary text-white"),
                dbc.CardBody([
                    html.Div(id='tests-completed-count', className="text-center display-4")
                ])
            ], className="mb-4"),
            width=4
        ),

        # Last Test Run Tile
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Last Test Run", className="bg-primary text-white"),
                dbc.CardBody([
                    html.Div(id='last-test-run-time', className="text-center h5")
                ])
            ], className="mb-4"),
            width=4
        ),
    ]),

    # Mart Status Tiles
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Data Mart Usability Status", className="bg-primary text-white"),
                dbc.CardBody([
                    html.Div(id='mart-status-display')
                ])
            ], className="mb-4"),
            width=12
        ),
    ]),
    # Test type summary statistics
    # dbc.Row([
    #         dbc.Col(
    #             dbc.Card([
    #                 dbc.CardHeader("Test Categories - Passing Rates", className="bg-primary text-white"),
    #                 dbc.CardBody([
    #                     html.Div(id='test-category-tiles')
    #                 ])
    #             ], className="mb-4"),
    #             width=12
    #         ),
    #     ]),

    dbc.Card([
        dbc.CardHeader("Outstanding Errors", className="bg-danger text-white"),
        dbc.CardBody([
            html.P("Tests that have failed or have warnings:"),
            html.Button('Refresh Data', id='refresh-button', className='btn btn-primary mb-3'),
            html.Div(id='outstanding-errors-table')
        ])
    ], className="mb-4"),

    # Add modal for data mart details
    dbc.Modal([
        dbc.ModalHeader(id="mart-modal-header"),
        dbc.ModalBody([
            dbc.Tabs([
                dbc.Tab(html.Div(id="mart-failing-tests"), label="Failing Tests", tab_id="failing-tab"),
                dbc.Tab(html.Div(id="mart-passing-tests"), label="Passing Tests", tab_id="passing-tab"),
            ], id="mart-tabs", active_tab="failing-tab"),
        ]),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-mart-modal", className="ml-auto")
        ),
    ], id="mart-modal", size="xl"),

    dbc.Modal([
        dbc.ModalHeader("Error Details"),
        dbc.ModalBody(id="error-modal-body"),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-error-modal", className="ml-auto")
        ),
    ], id="error-modal", size="xl"),
], className="dashboard-container")


# Callback for the file upload
@callback(
    Output('output-data-upload', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def update_output(contents, filename):
    if contents is not None:
        return parse_contents(contents, filename)
    return html.Div()


# Callback for the database preview
@callback(
    Output('database-preview', 'children'),
    Input('refresh-button', 'n_clicks'),
    Input('output-data-upload', 'children')  # Refresh when new data is uploaded
)
def update_database_preview(n_clicks, upload_output):
    try:
        df = get_data_from_db(limit=10)
        if df.empty:
            return html.P("No data in the database yet. Please upload a CSV file.")

        # Select a subset of columns for better display
        display_columns = [
            'UNIQUE_ID', 'TABLE_NAME', 'TEST_NAME', 'TEST_COLUMN_NAME',
            'SEVERITY', 'QUALITY_DIMENSION', 'STATUS'
        ]

        display_df = df[display_columns] if all(col in df.columns for col in display_columns) else df

        return dash_table.DataTable(
            data=display_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in display_df.columns],
            page_size=10,
            style_table={'overflowX': 'auto'},
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
            },
            tooltip_data=[
                {
                    column: {'value': str(value), 'type': 'markdown'}
                    for column, value in row.items()
                } for row in display_df.to_dict('records')
            ],
            tooltip_duration=None
        )
    except Exception as e:
        return html.P(f"Error retrieving data: {str(e)}")


# Callback for the data quality grade
@callback(
    Output('data-quality-grade', 'children'),
    [Input('refresh-button', 'n_clicks'),
     Input('output-data-upload', 'children')]
)
def update_data_quality_grade(n_clicks, upload_output):
    try:
        grade = get_data_quality_grade()

        # Define classes for different grades
        grade_classes = {
            'A': 'grade-a',
            'B': 'grade-b',
            'C': 'grade-c',
            'D': 'grade-d',
            'F': 'grade-f'
        }

        class_name = grade_classes.get(grade, '')

        return html.Span(grade, className=class_name)
    except Exception as e:
        return html.P(f"Error: {str(e)}")


# Callback for the tests completed count
@callback(
    Output('tests-completed-count', 'children'),
    [Input('refresh-button', 'n_clicks'),
     Input('output-data-upload', 'children')]
)
def update_tests_completed(n_clicks, upload_output):
    try:
        count = get_tests_completed_count()
        return f"{count:,}"
    except Exception as e:
        return html.P(f"Error: {str(e)}")


# Callback for the last test run time
@callback(
    Output('last-test-run-time', 'children'),
    [Input('refresh-button', 'n_clicks'),
     Input('output-data-upload', 'children')]
)
def update_last_test_run(n_clicks, upload_output):
    try:
        last_run = get_last_test_run_time()
        if last_run and last_run != "No data available":
            # Format the timestamp for better readability
            from datetime import datetime, timezone
            import pytz  # You'll need to install this package if not already installed

            try:
                # Parse the timestamp (assuming it's in UTC)
                dt = datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)  # Explicitly mark as UTC

                # Convert to Eastern Time
                eastern = pytz.timezone('America/New_York')
                dt_eastern = dt.astimezone(eastern)

                # Get current time in UTC for relative time calculation
                now = datetime.now(timezone.utc)
                time_diff = now - dt

                # Calculate relative time
                seconds = time_diff.total_seconds()
                if seconds < 60:
                    relative_time = f"{int(seconds)} seconds ago"
                elif seconds < 3600:
                    minutes = int(seconds // 60)
                    relative_time = f"{minutes} minute{'s' if minutes != 1 else ''} ago"
                elif seconds < 86400:
                    hours = int(seconds // 3600)
                    relative_time = f"{hours} hour{'s' if hours != 1 else ''} ago"
                elif seconds < 2592000:  # 30 days
                    days = int(seconds // 86400)
                    relative_time = f"{days} day{'s' if days != 1 else ''} ago"
                elif seconds < 31536000:  # 365 days
                    months = int(seconds // 2592000)
                    relative_time = f"{months} month{'s' if months != 1 else ''} ago"
                else:
                    years = int(seconds // 31536000)
                    relative_time = f"{years} year{'s' if years != 1 else ''} ago"

                # Format the Eastern time
                # Add ET/EST/EDT label based on whether daylight saving is in effect
                et_label = "EDT" if dt_eastern.dst() else "EST"
                formatted_time = dt_eastern.strftime(f"%b %d, %Y at %I:%M %p {et_label}")

                # Return both the formatted time and the relative time
                return html.Div([
                    html.Span(formatted_time),
                    html.Span(f" ({relative_time})", style={'color': 'gray', 'font-style': 'italic'})
                ])
            except Exception as e:
                return f"Error formatting time: {str(e)}"
        return last_run
    except Exception as e:
        return html.P(f"Error: {str(e)}")

@callback(
    Output('mart-status-display', 'children'),
    [Input('refresh-button', 'n_clicks'),
     Input('output-data-upload', 'children')]
)
def update_mart_status(n_clicks, upload_output):
    try:
        mart_statuses = get_mart_statuses()

        # Create a grid of cards for mart statuses
        cards = []
        for mart, status in mart_statuses.items():
            # Format the mart name for display
            display_name = (mart.replace('_', ' ').title()
                            .replace('Ccsr', 'CCSR')
                            .replace('Cms', 'CMS')
                            .replace('Ed', 'ED')
                            .replace('Pmpm', 'PMPM')
                            )

            # Choose icon and color based on status
            if status == 'fail':
                icon = html.I(className="fas fa-times-circle mart-icon", style={'color': '#dc3545'})
                color = "danger"
                status_text = "Not Usable"
            elif status == 'warn':
                icon = html.I(className="fas fa-exclamation-triangle mart-icon", style={'color': '#ffc107'})
                color = "warning"
                status_text = "Use with Caution"
            else:  # 'pass'
                icon = html.I(className="fas fa-check-circle mart-icon", style={'color': '#28a745'})
                color = "success"
                status_text = "Usable"

            # Create card with clickable functionality - use a button for guaranteed click behavior
            card = dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div(icon),
                        html.H5(display_name, className="text-center"),
                        html.P(status_text, className=f"text-{color} text-center"),
                        # Add a button that looks like a link
                        dbc.Button(
                            "View Tests",
                            id={"type": "mart-button", "index": mart},
                            color="link",
                            className="mt-2 w-100"
                        )
                    ], className="mart-status-card")
                ]),
                md=4, className="mb-4"
            )
            cards.append(card)

        # Arrange cards in rows
        rows = []
        for i in range(0, len(cards), 3):
            rows.append(dbc.Row(cards[i:i + 3], className="mb-3"))

        return html.Div(rows)

    except Exception as e:
        return html.P(f"Error determining mart status: {str(e)}")


@callback(
    Output('outstanding-errors-table', 'children'),
    [Input('refresh-button', 'n_clicks'),
     Input('output-data-upload', 'children')]
)
def update_outstanding_errors(n_clicks, upload_output):
    try:
        df = get_outstanding_errors()
        if df.empty:
            return html.P("No outstanding errors found.")

        # Store the full dataframe in a hidden div for later use
        hidden_data = html.Div(
            id='hidden-error-data',
            style={'display': 'none'},
            children=json.dumps(df.to_dict('records'))
        )

        # Create a list of rows with buttons
        rows = []
        for i, row in df.iterrows():
            rows.append(
                dbc.Row([
                    dbc.Col(str(int(row['SEVERITY_LEVEL'])), width=1, className="align-self-center table-cell"),  # Convert to int
                    dbc.Col(row['TABLE_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_COLUMN_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_ORIGINAL_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_TYPE'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '', width=2,
                            className="align-self-center table-cell"),
                    dbc.Col(
                        dbc.Button(
                            "More Info",
                            id={"type": "error-info-button", "index": i},
                            color="info",
                            size="sm",
                            className="my-1"  # Add vertical margin to center the button
                        ),
                        width=1,
                        className="d-flex align-items-center"  # Better vertical centering
                    ),
                ],
                    className="mb-2 border-bottom pb-2",
                    style={
                        "backgroundColor":
                            "#ffcccc" if row['SEVERITY_LEVEL'] == 1 else
                            "#ffe6cc" if row['SEVERITY_LEVEL'] == 2 else
                            "#ffffcc" if row['SEVERITY_LEVEL'] == 3 else
                            "#e6ffcc" if row['SEVERITY_LEVEL'] == 4 else
                            "#ccffcc" if row['SEVERITY_LEVEL'] == 5 else
                            "white"
                    })
            )

        # Create a header row
        header = dbc.Row([
            dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
            dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
        ], className="mb-2 border-bottom pb-2 font-weight-bold")

        # Combine header and rows
        table_content = [header] + rows

        # Create a container for the table with pagination
        table_container = html.Div([
            hidden_data,
            html.Div(table_content[:10]),  # Show first 10 rows initially
            dbc.Pagination(
                id="error-pagination",
                max_value=max(1, (len(rows) + 9) // 10),  # Ceiling division to get number of pages
                first_last=True,
                previous_next=True,
                active_page=1
            ) if len(rows) > 10 else html.Div()
        ])

        return table_container

    except Exception as e:
        return html.P(f"Error retrieving data: {str(e)}")


# Add a callback for pagination
@callback(
    Output('outstanding-errors-table', 'children', allow_duplicate=True),
    [Input('error-pagination', 'active_page')],
    [State('hidden-error-data', 'children')],
    prevent_initial_call=True
)
def change_page(page, json_data):
    if not page or not json_data:
        return dash.no_update

    try:
        # Parse the data
        data = json.loads(json_data)
        df = pd.DataFrame(data)

        # Create rows for the current page
        rows = []
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(df))

        for i in range(start_idx, end_idx):
            row = df.iloc[i]
            rows.append(
                dbc.Row([
                    dbc.Col(str(row['SEVERITY_LEVEL']), width=1, className="align-self-center table-cell"),
                    dbc.Col(row['TABLE_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_COLUMN_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_ORIGINAL_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_TYPE'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '', width=2,
                            className="align-self-center table-cell"),
                    dbc.Col(
                        dbc.Button(
                            "More Info",
                            id={"type": "error-info-button", "index": i},
                            color="info",
                            size="sm",
                            className="my-1"
                        ),
                        width=1,
                        className="d-flex align-items-center"
                    ),
                ],
                    className="mb-2 border-bottom pb-2",
                    style={
                        "backgroundColor":
                            "#ffcccc" if row['SEVERITY_LEVEL'] == 1 else
                            "#ffe6cc" if row['SEVERITY_LEVEL'] == 2 else
                            "#ffffcc" if row['SEVERITY_LEVEL'] == 3 else
                            "#e6ffcc" if row['SEVERITY_LEVEL'] == 4 else
                            "#ccffcc" if row['SEVERITY_LEVEL'] == 5 else
                            "white"
                    })
            )

        # Create a header row
        header = dbc.Row([
            dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
            dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
        ], className="mb-2 border-bottom pb-2 font-weight-bold")

        # Combine header and rows
        table_content = [header] + rows

        # Create a container for the table with pagination
        table_container = html.Div([
            html.Div(id='hidden-error-data', style={'display': 'none'}, children=json_data),
            html.Div(table_content),
            dbc.Pagination(
                id="error-pagination",
                max_value=max(1, (len(df) + 9) // 10),  # Ceiling division to get number of pages
                first_last=True,
                previous_next=True,
                active_page=page
            ) if len(df) > 10 else html.Div()
        ])

        return table_container

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


# Callback for the "More Info" buttons
@callback(
    [Output("error-modal", "is_open", allow_duplicate=True),
     Output("error-modal-body", "children", allow_duplicate=True)],
    [Input({"type": "error-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-error-data", "children"),
     State("error-modal", "is_open")],
    prevent_initial_call=True
)
def toggle_error_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and 'index' in triggered_id:
        clicked_index = triggered_id['index']

        try:
            # Get the data for the clicked row
            data = json.loads(json_data)
            row = data[clicked_index]

            # Create the modal content using our helper function
            modal_content = create_test_modal_content(row)

            # Check if we have the data mart flags before adding that section
            mart_flags_exist = all(flag in row for flag in [
                'FLAG_SERVICE_CATEGORIES', 'FLAG_CCSR', 'FLAG_CMS_CHRONIC_CONDITIONS',
                'FLAG_TUVA_CHRONIC_CONDITIONS', 'FLAG_CMS_HCCS', 'FLAG_ED_CLASSIFICATION',
                'FLAG_FINANCIAL_PMPM', 'FLAG_QUALITY_MEASURES', 'FLAG_READMISSION'
            ])

            # Add the affected data marts section if flags exist
            if mart_flags_exist:
                modal_content.extend([
                    html.Hr(),
                    html.H6("Affected Data Marts:"),
                    dbc.Row([
                        # Create icons for each affected data mart
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_SERVICE_CATEGORIES'] == 1 else "fas fa-database text-muted mr-2"),
                            "Service Categories"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_CCSR'] == 1 else "fas fa-database text-muted mr-2"),
                            "CCSR"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_CMS_CHRONIC_CONDITIONS'] == 1 else "fas fa-database text-muted mr-2"),
                            "CMS Chronic Conditions"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_TUVA_CHRONIC_CONDITIONS'] == 1 else "fas fa-database text-muted mr-2"),
                            "TUVA Chronic Conditions"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_CMS_HCCS'] == 1 else "fas fa-database text-muted mr-2"),
                            "CMS HCCs"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_ED_CLASSIFICATION'] == 1 else "fas fa-database text-muted mr-2"),
                            "ED Classification"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_FINANCIAL_PMPM'] == 1 else "fas fa-database text-muted mr-2"),
                            "Financial PMPM"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_QUALITY_MEASURES'] == 1 else "fas fa-database text-muted mr-2"),
                            "Quality Measures"
                        ]), width=4),
                        dbc.Col(html.Div([
                            html.I(className="fas fa-database text-danger mr-2" if row['FLAG_READMISSION'] == 1 else "fas fa-database text-muted mr-2"),
                            "Readmission"
                        ]), width=4),
                    ]),
                ])

            return True, modal_content

        except Exception as e:
            # Return an error message in the modal if something goes wrong
            error_content = [
                html.H5("Error Loading Test Details"),
                html.Hr(),
                html.P(f"An error occurred: {str(e)}"),
                html.Pre(traceback.format_exc())
            ]
            return True, error_content

    return is_open, dash.no_update

@callback(
    [Output("error-modal", "is_open"),
     Output("error-modal-body", "children")],
    [Input({"type": "failing-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-failing-data", "children"),
     State("error-modal", "is_open")],
    prevent_initial_call=True
)
def toggle_failing_test_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and 'index' in triggered_id:
        clicked_index = triggered_id['index']

        # Get the data for the clicked row
        data = json.loads(json_data)
        row = data[clicked_index]

        # Create the modal content
        modal_content = create_test_modal_content(row)

        return True, modal_content

    return is_open, dash.no_update


# Callback for the "More Info" buttons in passing tests
@callback(
    [Output("error-modal", "is_open", allow_duplicate=True),
     Output("error-modal-body", "children", allow_duplicate=True)],
    [Input({"type": "passing-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-passing-data", "children"),
     State("error-modal", "is_open")],
    prevent_initial_call=True
)
def toggle_passing_test_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and 'index' in triggered_id:
        clicked_index = triggered_id['index']

        # Get the data for the clicked row
        data = json.loads(json_data)
        row = data[clicked_index]

        # Create the modal content
        modal_content = create_test_modal_content(row)

        return True, modal_content

    return is_open, dash.no_update

@callback(
    Output("error-modal", "is_open", allow_duplicate=True),
    Input("close-error-modal", "n_clicks"),
    State("error-modal", "is_open"),
    prevent_initial_call=True
)
def close_modal(close_clicks, is_open):
    if close_clicks:
        return False
    return is_open


# Copy button feedback
@callback(
    Output("copy-query-output", "children"),
    Input("copy-query-button", "n_clicks"),
    State("query-text", "value"),
    prevent_initial_call=True,
)
def copy_to_clipboard(n_clicks, query_text):
    if n_clicks > 0:
        # Return a confirmation message
        return html.Div([
            html.Span("Copied to clipboard!", style={"color": "green", "margin-top": "5px"}),
            # Add a hidden div with JavaScript to copy to clipboard
            html.Div(id="clipboard-js",
                     children=[],
                     style={"display": "none"},
                     # This will execute when the component is rendered
                     **{"data-clipboard": query_text})
        ])
    return dash.no_update

dash.clientside_callback(
    """
    function(divProps) {
        if(divProps && divProps['data-clipboard']) {
            navigator.clipboard.writeText(divProps['data-clipboard']);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("clipboard-js", "children"),
    Input("clipboard-js", "data-clipboard")
)


@callback(
    [Output("mart-modal", "is_open"),
     Output("mart-modal-header", "children"),
     Output("mart-failing-tests", "children"),
     Output("mart-passing-tests", "children")],
    [Input({"type": "mart-button", "index": dash.ALL}, "n_clicks")],
    prevent_initial_call=True
)
def toggle_mart_modal(n_clicks):
    # Check if callback was triggered by an actual click
    if not ctx.triggered_id or not any(n for n in n_clicks if n):
        return False, dash.no_update, dash.no_update, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and 'index' in triggered_id:
        clicked_mart = triggered_id['index']

        # Format the mart name for display
        display_name = (clicked_mart.replace('_', ' ').title()
                        .replace('Ccsr', 'CCSR')
                        .replace('Cms', 'CMS')
                        .replace('Ed', 'ED')
                        .replace('Pmpm', 'PMPM')
                        )

        # Get failing tests for this mart
        failing_df = get_mart_tests(clicked_mart)

        # Get passing tests for this mart
        passing_df = get_mart_tests(clicked_mart, status='pass')

        # Create tables for failing and passing tests
        failing_content = create_test_table(failing_df, "failing")
        passing_content = create_test_table(passing_df, "passing")

        return True, f"{display_name} Data Mart Tests", failing_content, passing_content

    return False, dash.no_update, dash.no_update, dash.no_update

# Close mart modal callback
@callback(
    Output("mart-modal", "is_open", allow_duplicate=True),
    Input("close-mart-modal", "n_clicks"),
    State("mart-modal", "is_open"),
    prevent_initial_call=True
)
def close_mart_modal(close_clicks, is_open):
    if close_clicks:
        return False
    return is_open


@callback(
    Output('failing-tests-table-content', 'children'),
    [Input('failing-pagination', 'active_page')],
    [State('hidden-failing-data', 'children')],
    prevent_initial_call=True
)
def update_failing_pagination(page, json_data):
    if not page or not json_data:
        return dash.no_update

    try:
        # Parse the data
        data = json.loads(json_data)
        df = pd.DataFrame(data)

        # Create rows for the current page
        rows = []
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(df))

        for i in range(start_idx, end_idx):
            row = df.iloc[i]
            severity_level = int(row['SEVERITY_LEVEL']) if pd.notna(row['SEVERITY_LEVEL']) else 0

            rows.append(
                dbc.Row([
                    dbc.Col(str(severity_level), width=1, className="align-self-center table-cell"),
                    dbc.Col(row['TABLE_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_COLUMN_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_ORIGINAL_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_TYPE'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '', width=2,
                            className="align-self-center table-cell"),
                    dbc.Col(
                        dbc.Button(
                            "More Info",
                            id={"type": "failing-info-button", "index": i},
                            color="info",
                            size="sm",
                            className="my-1"
                        ),
                        width=1,
                        className="d-flex align-items-center"
                    ),
                ],
                    className="mb-2 border-bottom pb-2",
                    style={
                        "backgroundColor":
                            "#ffcccc" if severity_level == 1 else
                            "#ffe6cc" if severity_level == 2 else
                            "#ffffcc" if severity_level == 3 else
                            "#e6ffcc" if severity_level == 4 else
                            "#ccffcc" if severity_level == 5 else
                            "white"
                    })
            )

        # Create a header row
        header = dbc.Row([
            dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
            dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
        ], className="mb-2 border-bottom pb-2 font-weight-bold")

        # Combine header and rows
        table_content = [header] + rows
        return table_content

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


@callback(
    Output('passing-tests-table-content', 'children'),
    [Input('passing-pagination', 'active_page')],
    [State('hidden-passing-data', 'children')],
    prevent_initial_call=True
)
def update_passing_pagination(page, json_data):
    if not page or not json_data:
        return dash.no_update

    try:
        # Parse the data
        data = json.loads(json_data)
        df = pd.DataFrame(data)

        # Create rows for the current page
        rows = []
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(df))

        for i in range(start_idx, end_idx):
            row = df.iloc[i]
            severity_level = int(row['SEVERITY_LEVEL']) if pd.notna(row['SEVERITY_LEVEL']) else 0

            rows.append(
                dbc.Row([
                    dbc.Col(str(severity_level), width=1, className="align-self-center table-cell"),
                    dbc.Col(row['TABLE_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_COLUMN_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_ORIGINAL_NAME'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_TYPE'], width=2, className="align-self-center table-cell"),
                    dbc.Col(row['TEST_SUB_TYPE'] if pd.notna(row['TEST_SUB_TYPE']) else '', width=2,
                            className="align-self-center table-cell"),
                    dbc.Col(
                        dbc.Button(
                            "More Info",
                            id={"type": "passing-info-button", "index": i},
                            color="info",
                            size="sm",
                            className="my-1"
                        ),
                        width=1,
                        className="d-flex align-items-center"
                    ),
                ],
                    className=f"mb-2 border-bottom pb-2 passing-test-row passing-severity-{severity_level}")
            )

        # Create a header row
        header = dbc.Row([
            dbc.Col(html.Strong("Severity"), width=1, className="table-cell"),
            dbc.Col(html.Strong("Table"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Column"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Name"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Test Sub Type"), width=2, className="table-cell"),
            dbc.Col(html.Strong("Actions"), width=1, className="table-cell"),
        ], className="mb-2 border-bottom pb-2 font-weight-bold")

        # Combine header and rows
        table_content = [header] + rows
        return table_content

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


# @callback(
#     Output('test-category-tiles', 'children'),
#     [Input('refresh-button', 'n_clicks'),
#      Input('output-data-upload', 'children')]
# )
# def update_test_category_tiles(n_clicks, upload_output):
#     try:
#         categories_df = get_test_category_stats()
#
#         if categories_df.empty:
#             return html.P("No test category data available.")
#
#         # Create tiles for each test category
#         category_tiles = []
#
#         for _, row in categories_df.iterrows():
#             category = row['TEST_CATEGORY']
#             passing_pct = row['passing_percentage']
#             total_tests = row['total_tests']
#             passing_tests = row['passing_tests']
#
#             # Determine color based on passing percentage
#             if passing_pct >= 90:
#                 bar_color = "success"
#             elif passing_pct >= 75:
#                 bar_color = "info"
#             elif passing_pct >= 50:
#                 bar_color = "warning"
#             else:
#                 bar_color = "danger"
#
#             # Create a card for each category
#             category_tile = dbc.Col(
#                 dbc.Card([
#                     dbc.CardBody([
#                         html.H5(category, className="card-title"),
#                         html.P(f"{passing_tests} of {total_tests} tests passing", className="card-text"),
#                         dbc.Progress(
#                             value=passing_pct,
#                             label=f"{passing_pct}%",
#                             color=bar_color,
#                             striped=True,
#                             className="mb-2"
#                         ),
#                     ])
#                 ], className="h-100"),
#                 md=4, className="mb-3"
#             )
#
#             category_tiles.append(category_tile)
#
#         # Arrange tiles in rows
#         rows = []
#         for i in range(0, len(category_tiles), 3):
#             rows.append(dbc.Row(category_tiles[i:i + 3], className="mb-2"))
#
#         return html.Div(rows)
#
#     except Exception as e:
#         return html.P(f"Error retrieving test category data: {str(e)}")

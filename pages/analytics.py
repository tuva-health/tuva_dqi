import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import sqlite3
import os
import io
import base64

# Register the page
dash.register_page(__name__)


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


# Layout with Bootstrap cards/tiles
layout = html.Div([
    html.H1('Test Results Dashboard', className='mb-4'),

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
                            html.A('Select Files')
                        ]),
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

    # Third row - Data Preview Tile
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Test Results Preview", className="bg-secondary text-white"),
                dbc.CardBody([
                    html.P("Preview of test results in the database:"),
                    html.Button('Refresh Data', id='refresh-button', className='btn btn-primary mb-3'),
                    html.Div(id='database-preview')
                ])
            ]),
            width=12
        ),
    ]),
])


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

        # Define colors for different grades
        grade_colors = {
            'A': 'text-success',
            'B': 'text-info',
            'C': 'text-warning',
            'D': 'text-warning',
            'F': 'text-danger'
        }

        color_class = grade_colors.get(grade, '')

        return html.Span(grade, className=color_class)
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
            # This assumes the timestamp is in a standard format
            from datetime import datetime
            try:
                dt = datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%b %d, %Y at %I:%M %p")
            except:
                return last_run
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
            display_name = mart.replace('_', ' ').title()

            # Choose icon and color based on status
            if status == 'fail':
                icon = html.I(className="fas fa-times-circle", style={'fontSize': '36px'})
                color = "danger"
                status_text = "Not Usable"
            elif status == 'warn':
                icon = html.I(className="fas fa-exclamation-triangle", style={'fontSize': '36px'})
                color = "warning"
                status_text = "Use with Caution"
            else:  # 'pass'
                icon = html.I(className="fas fa-check-circle", style={'fontSize': '36px'})
                color = "success"
                status_text = "Usable"

            # Create card
            card = dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div(icon, className=f"text-{color} text-center mb-2"),
                        html.H5(display_name, className="text-center"),
                        html.P(status_text, className=f"text-{color} text-center")
                    ])
                ], className="mb-4"),
                width=4  # 3 cards per row
            )
            cards.append(card)

        # Arrange cards in rows
        rows = []
        for i in range(0, len(cards), 3):
            rows.append(dbc.Row(cards[i:i + 3]))

        return html.Div(rows)

    except Exception as e:
        return html.P(f"Error determining mart status: {str(e)}")

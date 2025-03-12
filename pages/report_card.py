import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import sqlite3
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Import helper functions from analytics page
from utils import (
    get_db_connection, get_data_quality_grade,
    get_tests_completed_count, get_last_test_run_time,
    get_mart_statuses, get_outstanding_errors,
    get_available_charts, get_chart_data, create_chart,
    get_test_category_stats, get_all_tests, get_quality_dimension_summary, get_mart_test_summary
)

# Register the page
dash.register_page(__name__, path='/report-card', name='Report Card')

# Layout for the report card page
layout = html.Div([
    # Header section with print button
    dbc.Row([
        dbc.Col([
            html.H1("Data Quality Report Card", className="report-title"),
            html.P(id="report-generation-date", className="text-muted"),
        ], width=9),
        dbc.Col([
            dbc.Button([
                html.I(className="fas fa-print me-2"),
                "Print Report"
            ], id="report-print-button", color="primary", className="float-end")
        ], width=3),
    ], className="mb-4 d-print-none"),

    # Report header for print version
    html.Div([
        html.Div([
            html.Img(src="/assets/tuva_logo.png", height="60px", className="me-3"),
            html.Div([
                html.H1("Data Quality Report Card", className="report-title mb-0"),
                html.P(id="report-print-date", className="text-muted"),
            ])
        ], className="d-flex align-items-center"),
        html.Hr()
    ], className="d-none d-print-block mb-4"),

    # Summary section
    dbc.Card([
        dbc.CardHeader(html.H3("Executive Summary", className="m-0")),
        dbc.CardBody([
            dbc.Row([
                # Data Quality Grade
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Overall Data Quality Grade", className="text-center mb-3"),
                            html.Div(id="report-quality-grade", className="text-center display-1 mb-3"),
                            html.P(id="report-grade-description", className="text-center")
                        ])
                    ], className="h-100")
                ], md=4),

                # Tests Summary
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Test Summary", className="text-center mb-3"),
                            html.Div([
                                html.Div([
                                    html.Span(id="report-tests-completed", className="display-6"),
                                    html.Span(" Total Tests", className="ms-2")
                                ], className="d-flex align-items-baseline justify-content-center mb-2"),
                                html.Div([
                                    html.Span(id="report-tests-passing", className="display-6"),
                                    html.Span(" Passing Tests", className="ms-2")
                                ], className="d-flex align-items-baseline justify-content-center mb-2"),
                                html.Div([
                                    html.Span(id="report-tests-failing", className="display-6"),
                                    html.Span(" Failing Tests", className="ms-2")
                                ], className="d-flex align-items-baseline justify-content-center")
                            ])
                        ])
                    ], className="h-100")
                ], md=4),

                # Last Run Time
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Test Run Information", className="text-center mb-3"),
                            html.P([
                                html.Strong("Last Test Run: "),
                                html.Span(id="report-last-run")
                            ], className="mb-2"),
                            html.P([
                                html.Strong("Report Generated: "),
                                html.Span(id="report-generated")
                            ], className="mb-2"),
                            html.P([
                                html.Strong("Database: "),
                                html.Span(id="report-database")
                            ], className="mb-2")
                        ])
                    ], className="h-100")
                ], md=4)
            ])
        ])
    ], className="mb-4"),

    # Data Mart Status section
    dbc.Card([
        dbc.CardHeader(html.H3("Data Mart Status", className="m-0")),
        dbc.CardBody([
            html.Div(id="report-mart-status")
        ])
    ], className="mb-4"),

    # Quality Dimension Summary
    dbc.Card([
        dbc.CardHeader(html.H3("Quality Dimension Summary", className="m-0")),
        dbc.CardBody([
            html.Div(id="report-quality-dimensions")
        ])
    ], className="mb-4 page-break-before"),

    # Critical Issues section
    dbc.Card([
        dbc.CardHeader(html.H3("Critical Issues", className="m-0")),
        dbc.CardBody([
            html.Div(id="report-critical-issues")
        ])
    ], className="mb-4 page-break-before"),

    # All Tests section
    dbc.Card([
        dbc.CardHeader(html.H3("All Tests", className="m-0")),
        dbc.CardBody([
            html.Div(id="report-all-tests")
        ])
    ], className="mb-4 page-break-before"),

    # Data Visualizations section
    dbc.Card([
        dbc.CardHeader(html.H3("Data Visualizations", className="m-0")),
        dbc.CardBody([
            html.Div(id="report-visualizations")
        ])
    ], className="mb-4 page-break-before"),

    # Print JavaScript
    html.Div([
        html.Script('''
            document.getElementById('report-print-button').addEventListener('click', function() {
                window.print();
            });
        ''')
    ], id='report-print-script'),

    # Hidden div to store data
    html.Div(id="report-hidden-data", style={"display": "none"})
], id="report-card-container")


# Callback to populate the report data
@callback(
    [
        Output("report-generation-date", "children"),
        Output("report-print-date", "children"),
        Output("report-quality-grade", "children"),
        Output("report-grade-description", "children"),
        Output("report-tests-completed", "children"),
        Output("report-tests-passing", "children"),
        Output("report-tests-failing", "children"),
        Output("report-last-run", "children"),
        Output("report-generated", "children"),
        Output("report-database", "children"),
        Output("report-mart-status", "children"),
        Output("report-quality-dimensions", "children"),
        Output("report-critical-issues", "children"),
        Output("report-all-tests", "children"),
        Output("report-visualizations", "children"),
        Output("report-hidden-data", "children")
    ],
    [Input("report-print-button", "n_clicks"),
     Input("report-card-container", "id")]  # Add this to trigger on page load
)
def generate_report(n_clicks, container_id):
    # Current date and time
    now = datetime.now()
    current_date = now.strftime("%B %d, %Y at %I:%M %p")

    # Get the data quality grade
    grade = get_data_quality_grade()

    # Get grade description
    grade_descriptions = {
        'A': "Excellent - No severity level 1-4 issues detected. All data marts are usable.",
        'B': "Good - Has severity level 4 issues. All data marts are usable.",
        'C': "Fair - Has severity level 3 issues. Some data marts may have warnings.",
        'D': "Poor - Has severity level 2 issues. Some data marts may not be usable.",
        'F': "Critical - Has severity level 1 issues. Most data marts are not usable."
    }
    grade_description = grade_descriptions.get(grade, "Unknown grade")

    # Get test counts
    total_tests = get_tests_completed_count()

    # Get passing and failing test counts
    conn = get_db_connection()
    passing_tests = pd.read_sql_query("""
        SELECT COUNT(*) as count FROM test_results WHERE STATUS = 'pass' AND CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5
    """, conn).iloc[0]['count']
    failing_tests = total_tests - passing_tests

    # Get database name
    database_name = pd.read_sql_query("""
        SELECT DISTINCT DATABASE_NAME FROM test_results WHERE CAST(SEVERITY_LEVEL AS INTEGER) BETWEEN 1 AND 5 LIMIT 1
    """, conn).iloc[0]['DATABASE_NAME'] if total_tests > 0 else "N/A"

    conn.close()

    # Get last test run time
    last_run = get_last_test_run_time()
    if last_run and last_run != "No data available":
        try:
            # Format the timestamp for better readability
            dt = datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S")
            formatted_last_run = dt.strftime("%B %d, %Y at %I:%M %p")
        except:
            formatted_last_run = last_run
    else:
        formatted_last_run = last_run

    # Generate Data Mart Status section
    mart_summaries = get_mart_test_summary()

    mart_status_content = []

    # Create a table for mart status
    mart_table_header = html.Thead(html.Tr([
        html.Th("Data Mart", className="text-start"),
        html.Th("Status", className="text-center"),
        html.Th("Tests", className="text-center"),
        html.Th("Passing", className="text-center"),
        html.Th("Sev 1 Fails", className="text-center"),
        html.Th("Sev 2 Fails", className="text-center"),
        html.Th("Sev 3 Fails", className="text-center"),
        html.Th("Sev 4-5 Fails", className="text-center")
    ]))

    mart_table_rows = []
    for mart in mart_summaries:
        mart_table_rows.append(html.Tr([
            html.Td(mart['display_name'], className="text-start"),
            html.Td(
                html.Span(mart['status'], className=f"badge bg-{mart['status_color']}"),
                className="text-center"
            ),
            html.Td(f"{mart['total_tests']:,}", className="text-center"),
            html.Td(f"{mart['passing_tests']:,} ({mart['passing_percentage']}%)", className="text-center"),
            html.Td(
                f"{mart['sev1_fails']:,}",
                className="text-center",
                style={"background-color": "#ffcccc" if mart['sev1_fails'] > 0 else "transparent"}
            ),
            html.Td(
                f"{mart['sev2_fails']:,}",
                className="text-center",
                style={"background-color": "#ffe6cc" if mart['sev2_fails'] > 0 else "transparent"}
            ),
            html.Td(
                f"{mart['sev3_fails']:,}",
                className="text-center",
                style={"background-color": "#ffffcc" if mart['sev3_fails'] > 0 else "transparent"}
            ),
            html.Td(
                f"{mart['sev4_fails'] + mart['sev5_fails']:,}",
                className="text-center"
            )
        ]))

    mart_table = dbc.Table(
        [mart_table_header, html.Tbody(mart_table_rows)],
        bordered=True,
        hover=True,
        responsive=True,
        className="mart-status-table"
    )

    mart_status_content.append(mart_table)

    # Generate Quality Dimension Summary section
    quality_dimensions = get_quality_dimension_summary()

    if not quality_dimensions.empty:
        # Create a bar chart for quality dimensions
        fig = px.bar(
            quality_dimensions,
            x="QUALITY_DIMENSION",
            y=["passing_tests", "failing_tests"],
            title="Tests by Quality Dimension",
            labels={
                "QUALITY_DIMENSION": "Quality Dimension",
                "value": "Number of Tests",
                "variable": "Status"
            },
            color_discrete_map={
                "passing_tests": "#28a745",
                "failing_tests": "#dc3545"
            },
            barmode="stack"
        )

        fig.update_layout(
            legend_title_text="Test Status",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        # Create a table for quality dimensions
        qd_table_header = html.Thead(html.Tr([
            html.Th("Quality Dimension", className="text-start"),
            html.Th("Total Tests", className="text-center"),
            html.Th("Passing Tests", className="text-center"),
            html.Th("Failing Tests", className="text-center"),
            html.Th("Passing Rate", className="text-center")
        ]))

        qd_table_rows = []
        for _, row in quality_dimensions.iterrows():
            qd_table_rows.append(html.Tr([
                html.Td(row['QUALITY_DIMENSION'].title(), className="text-start"),
                html.Td(f"{row['total_tests']:,}", className="text-center"),
                html.Td(f"{row['passing_tests']:,}", className="text-center"),
                html.Td(f"{row['failing_tests']:,}", className="text-center"),
                html.Td(
                    f"{row['passing_percentage']}%",
                    className="text-center",
                    style={
                        "background-color":
                            "#ccffcc" if row['passing_percentage'] >= 90 else
                            "#e6ffcc" if row['passing_percentage'] >= 75 else
                            "#ffffcc" if row['passing_percentage'] >= 50 else
                            "#ffe6cc" if row['passing_percentage'] >= 25 else
                            "#ffcccc"
                    }
                )
            ]))

        qd_table = dbc.Table(
            [qd_table_header, html.Tbody(qd_table_rows)],
            bordered=True,
            hover=True,
            responsive=True,
            className="quality-dimension-table mb-4"
        )

        quality_dimensions_content = html.Div([
            dcc.Graph(figure=fig, config={'displayModeBar': False}),
            qd_table
        ])
    else:
        quality_dimensions_content = html.P("No quality dimension data available.")

    # Generate Critical Issues section
    critical_issues = get_outstanding_errors()

    if not critical_issues.empty:
        # Filter for severity 1 and 2 issues
        critical_issues = critical_issues[critical_issues['SEVERITY_LEVEL'].isin([1, 2])]

        if not critical_issues.empty:
            # Create a table for critical issues
            ci_table_header = html.Thead(html.Tr([
                html.Th("Severity", className="text-center"),
                html.Th("Table", className="text-start"),
                html.Th("Column", className="text-start"),
                html.Th("Test", className="text-start"),
                html.Th("Type", className="text-start"),
                html.Th("Description", className="text-start")
            ]))

            ci_table_rows = []
            for _, row in critical_issues.iterrows():
                ci_table_rows.append(html.Tr([
                    html.Td(
                        str(int(row['SEVERITY_LEVEL'])),
                        className="text-center",
                        style={
                            "background-color":
                                "#ffcccc" if row['SEVERITY_LEVEL'] == 1 else
                                "#ffe6cc" if row['SEVERITY_LEVEL'] == 2 else
                                "transparent"
                        }
                    ),
                    html.Td(row['TABLE_NAME'], className="text-start"),
                    html.Td(row['TEST_COLUMN_NAME'], className="text-start"),
                    html.Td(row['TEST_ORIGINAL_NAME'], className="text-start"),
                    html.Td(row['TEST_TYPE'], className="text-start"),
                    html.Td(
                        row['TEST_DESCRIPTION'] if pd.notna(row['TEST_DESCRIPTION']) else "",
                        className="text-start"
                    )
                ]))

            ci_table = dbc.Table(
                [ci_table_header, html.Tbody(ci_table_rows)],
                bordered=True,
                hover=True,
                responsive=True,
                className="critical-issues-table"
            )

            critical_issues_content = ci_table
        else:
            critical_issues_content = html.P("No critical issues (Severity 1-2) found.")
    else:
        critical_issues_content = html.P("No critical issues found.")

    # Generate All Tests section
    all_tests = get_all_tests()

    if not all_tests.empty:
        # Create a table for all tests
        all_tests_table_header = html.Thead(html.Tr([
            html.Th("Status", className="text-center"),
            html.Th("Severity", className="text-center"),
            html.Th("Table", className="text-start"),
            html.Th("Column", className="text-start"),
            html.Th("Test", className="text-start"),
            html.Th("Type", className="text-start"),
            html.Th("Dimension", className="text-start")
        ]))

        all_tests_table_rows = []
        for _, row in all_tests.iterrows():

            try:
                if pd.isna(row['SEVERITY_LEVEL']):
                    severity_level = 0
                else:
                    # Try to convert to float first, then to int to handle both string and numeric formats
                    severity_level = int(float(row['SEVERITY_LEVEL']))
            except (ValueError, TypeError):
                # If conversion fails, set to 0
                severity_level = 0

            # Combine test name and description
            test_name = row['TEST_ORIGINAL_NAME']
            test_description = row['TEST_DESCRIPTION'] if pd.notna(row['TEST_DESCRIPTION']) else ""

            # Create a combined display with the test name in bold and description below it
            combined_test_info = html.Div([
                html.Strong(test_name),
                html.Br() if test_description else None,
                html.Span(test_description,
                          style={"font-size": "0.85em", "color": "#666"}) if test_description else None
            ])

            all_tests_table_rows.append(html.Tr([
                html.Td(
                    html.Span(
                        row['STATUS'],
                        className=f"badge bg-{'success' if row['STATUS'] == 'pass' else 'danger'}"
                    ),
                    className="text-center"
                ),
                html.Td(
                    str(severity_level),
                    className="text-center",
                    style={
                        "background-color":
                            "#ffcccc" if severity_level == 1 else
                            "#ffe6cc" if severity_level == 2 else
                            "#ffffcc" if severity_level == 3 else
                            "#e6ffcc" if severity_level == 4 else
                            "#ccffcc" if severity_level == 5 else
                            "transparent"
                    }
                ),
                html.Td(row['TABLE_NAME'], className="text-start"),
                html.Td(row['TEST_COLUMN_NAME'], className="text-start"),
                html.Td(combined_test_info, className="text-start"),  # Use the combined test info here
                html.Td(row['TEST_TYPE'], className="text-start"),
                html.Td(
                    row['QUALITY_DIMENSION'].title() if pd.notna(row['QUALITY_DIMENSION']) else "",
                    className="text-start"
                )
            ]))

        all_tests_table = dbc.Table(
            [all_tests_table_header, html.Tbody(all_tests_table_rows)],
            bordered=True,
            hover=True,
            responsive=True,
            className="all-tests-table"
        )

        all_tests_content = all_tests_table
    else:
        all_tests_content = html.P("No test data available.")

    # Generate Data Visualizations section
    charts_df = get_available_charts()

    if not charts_df.empty:
        # Group charts by category
        chart_categories = charts_df['DATA_QUALITY_CATEGORY'].unique()

        visualizations_content = []

        for i, category in enumerate(chart_categories):
            category_charts = charts_df[charts_df['DATA_QUALITY_CATEGORY'] == category]

            # Create a section for each category
            category_content = []

            # Add page break before each category (except the first one)
            if i > 0:
                category_content.append(html.Div(className="page-break-before"))

            # Add the category header
            category_content.append(html.H4(category.title(), className="mt-4 mb-3"))

            # Add each chart in this category
            for _, chart_info in category_charts.iterrows():
                chart_name = chart_info['GRAPH_NAME']

                # Create the chart
                try:
                    chart = create_chart(chart_name)

                    # Add chart to the category content
                    category_content.append(html.Div([
                        html.H5(chart_name.replace('_', ' ').title(), className="mt-3 mb-2"),
                        chart
                    ], className="mb-4"))
                except Exception as e:
                    # If chart creation fails, add an error message
                    category_content.append(html.Div([
                        html.H5(chart_name.replace('_', ' ').title(), className="mt-3 mb-2"),
                        html.P(f"Error creating chart: {str(e)}")
                    ], className="mb-4"))

            visualizations_content.append(html.Div(category_content))

        visualizations_content = html.Div(visualizations_content)
    else:
        visualizations_content = html.P("No visualization data available.")

    # Store data for potential use in other callbacks
    hidden_data = json.dumps({
        'grade': grade,
        'total_tests': int(total_tests),
        'passing_tests': int(passing_tests),
        'failing_tests': int(failing_tests)
    })

    # Define grade class for styling
    grade_classes = {
        'A': 'grade-a',
        'B': 'grade-b',
        'C': 'grade-c',
        'D': 'grade-d',
        'F': 'grade-f'
    }
    grade_element = html.Span(grade, className=grade_classes.get(grade, ''))

    return (
        f"Generated on {current_date}",  # report-generation-date
        f"Generated on {current_date}",  # report-print-date
        grade_element,  # report-quality-grade
        grade_description,  # report-grade-description
        f"{total_tests:,}",  # report-tests-completed
        f"{passing_tests:,}",  # report-tests-passing
        f"{failing_tests:,}",  # report-tests-failing
        formatted_last_run,  # report-last-run
        current_date,  # report-generated
        database_name,  # report-database
        html.Div(mart_status_content),  # report-mart-status
        quality_dimensions_content,  # report-quality-dimensions
        critical_issues_content,  # report-critical-issues
        all_tests_content,  # report-all-tests
        visualizations_content,  # report-visualizations
        hidden_data  # report-hidden-data
    )

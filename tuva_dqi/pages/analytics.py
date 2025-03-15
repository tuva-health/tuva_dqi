import json
import traceback

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import ALL, Input, Output, State, callback, ctx, dash_table, dcc, html

from tuva_dqi.utils import (
    create_chart,
    create_test_modal_content,
    create_test_table,
    get_available_charts,
    get_chart_filter_values,
    get_data_availability,
    get_data_from_test_results,
    get_data_quality_grade,
    get_last_test_run_time,
    get_mart_statuses,
    get_mart_tests,
    get_outstanding_errors,
    get_tests_completed_count,
    parse_chart_data_contents,
)

# Register the page
dash.register_page(__name__, path="/analytics", name="DQI Dashboard")

# Layout with Bootstrap cards/tiles
layout = html.Div(
    [
        html.H1("Data Quality Results Dashboard", className="mb-4"),
        # First row of tiles
        dbc.Row(
            [
                # File Upload Tile
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Import Test Results", className="bg-primary text-white"
                            ),
                            dbc.CardBody(
                                [
                                    html.P(
                                        "Upload a CSV file to import into the database:"
                                    ),
                                    dcc.Upload(
                                        id="upload-data",
                                        children=html.Div(
                                            [
                                                "Drag and Drop or ",
                                                html.A(
                                                    "Select Files",
                                                    className="upload-link",
                                                ),
                                            ],
                                            className="upload-text",
                                        ),
                                        style={
                                            "width": "100%",
                                            "height": "60px",
                                            "lineHeight": "60px",
                                            "borderWidth": "1px",
                                            "borderStyle": "dashed",
                                            "borderRadius": "5px",
                                            "textAlign": "center",
                                            "margin": "10px",
                                        },
                                        className="upload-area",
                                        multiple=False,
                                    ),
                                    html.Div(id="output-data-upload"),
                                ]
                            ),
                        ],
                        className="mb-4",
                    ),
                    width=12,
                ),
            ]
        ),
        # Data availibility
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Data Availability", className="bg-primary text-white"
                            ),
                            dbc.CardBody([html.Div(id="data-availability-display")]),
                        ],
                        className="mb-4",
                    ),
                    width=12,
                ),
            ]
        ),
        # First row summary tiles
        dbc.Row(
            [
                # Data Quality Grade Tile
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Data Quality Grade", className="bg-primary text-white"
                            ),
                            dbc.CardBody(
                                [
                                    html.Div(
                                        id="data-quality-grade",
                                        className="text-center display-4",
                                    )
                                ]
                            ),
                        ],
                        className="mb-4",
                    ),
                    width=4,
                ),
                # Tests Completed Tile
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Tests Completed", className="bg-primary text-white"
                            ),
                            dbc.CardBody(
                                [
                                    html.Div(
                                        id="tests-completed-count",
                                        className="text-center display-4",
                                    )
                                ]
                            ),
                        ],
                        className="mb-4",
                    ),
                    width=4,
                ),
                # Last Test Run Tile
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Last Test Run", className="bg-primary text-white"
                            ),
                            dbc.CardBody(
                                [
                                    html.Div(
                                        id="last-test-run-time",
                                        className="text-center h5",
                                    )
                                ]
                            ),
                        ],
                        className="mb-4",
                    ),
                    width=4,
                ),
            ]
        ),
        # Mart Status Tiles
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Data Mart Usability Status",
                                className="bg-primary text-white",
                            ),
                            dbc.CardBody([html.Div(id="mart-status-display")]),
                        ],
                        className="mb-4",
                    ),
                    width=12,
                ),
            ]
        ),
        dbc.Card(
            [
                dbc.CardHeader("Outstanding Errors", className="bg-danger text-white"),
                dbc.CardBody(
                    [
                        html.P("Tests that have failed or have warnings:"),
                        html.Button(
                            "Refresh Data",
                            id="refresh-button",
                            className="btn btn-primary mb-3",
                        ),
                        html.Div(id="outstanding-errors-table"),
                    ]
                ),
            ],
            className="mb-4",
        ),
        # Visualizations Exploratory
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Data Visualizations", className="bg-primary text-white"
                            ),
                            dbc.CardBody(
                                [
                                    html.P("Select a chart to display:"),
                                    dcc.Dropdown(
                                        id="chart-selector",
                                        options=[],
                                        placeholder="Select a chart",
                                    ),
                                    html.Div(
                                        id="chart-filter-container", className="mt-3"
                                    ),
                                    html.Div(id="chart-display", className="mt-3"),
                                ]
                            ),
                        ],
                        className="mb-4",
                    ),
                    width=12,
                ),
            ]
        ),
        # Add modal for data mart details
        dbc.Modal(
            [
                dbc.ModalHeader(id="mart-modal-header"),
                dbc.ModalBody(
                    [
                        dbc.Tabs(
                            [
                                dbc.Tab(
                                    html.Div(id="mart-failing-tests"),
                                    label="Failing Tests",
                                    tab_id="failing-tab",
                                ),
                                dbc.Tab(
                                    html.Div(id="mart-passing-tests"),
                                    label="Passing Tests",
                                    tab_id="passing-tab",
                                ),
                            ],
                            id="mart-tabs",
                            active_tab="failing-tab",
                        ),
                    ]
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-mart-modal", className="ml-auto")
                ),
            ],
            id="mart-modal",
            size="xl",
        ),
        dbc.Modal(
            [
                dbc.ModalHeader("Error Details"),
                dbc.ModalBody(id="error-modal-body"),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-error-modal", className="ml-auto")
                ),
            ],
            id="error-modal",
            size="xl",
        ),
    ],
    className="dashboard-container",
)


# Callback for the file upload
@callback(
    Output("output-data-upload", "children"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
)
def update_output(contents, filename):
    if contents is not None:
        return parse_chart_data_contents(contents, filename)
    return html.Div()


# Callback for the database preview
@callback(
    Output("database-preview", "children"),
    Input("refresh-button", "n_clicks"),
    Input("output-data-upload", "children"),  # Refresh when new data is uploaded
)
def update_database_preview(n_clicks, upload_output):
    try:
        df = get_data_from_test_results(limit=10)
        if df.empty:
            return html.P("No data in the database yet. Please upload a CSV file.")

        # Select a subset of columns for better display
        display_columns = [
            "UNIQUE_ID",
            "TABLE_NAME",
            "TEST_NAME",
            "TEST_COLUMN_NAME",
            "SEVERITY",
            "QUALITY_DIMENSION",
            "STATUS",
        ]

        display_df = (
            df[display_columns]
            if all(col in df.columns for col in display_columns)
            else df
        )

        return dash_table.DataTable(
            data=display_df.to_dict("records"),
            columns=[{"name": i, "id": i} for i in display_df.columns],
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={
                "overflow": "hidden",
                "textOverflow": "ellipsis",
                "maxWidth": 0,
            },
            tooltip_data=[
                {
                    column: {"value": str(value), "type": "markdown"}
                    for column, value in row.items()
                }
                for row in display_df.to_dict("records")
            ],
            tooltip_duration=None,
        )
    except Exception as e:
        return html.P(f"Error retrieving data: {str(e)}")


# Callback for the data quality grade
@callback(
    Output("data-quality-grade", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_data_quality_grade(n_clicks, upload_output):
    try:
        grade = get_data_quality_grade()

        # Define classes for different grades
        grade_classes = {
            "A": "grade-a",
            "B": "grade-b",
            "C": "grade-c",
            "D": "grade-d",
            "F": "grade-f",
        }

        class_name = grade_classes.get(grade, "")

        return html.Span(grade, className=class_name)
    except Exception as e:
        return html.P(f"Error: {str(e)}")


# Callback for the tests completed count
@callback(
    Output("tests-completed-count", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_tests_completed(n_clicks, upload_output):
    try:
        count = get_tests_completed_count()
        return f"{count:,}"
    except Exception as e:
        return html.P(f"Error: {str(e)}")


# Callback for the last test run time
@callback(
    Output("last-test-run-time", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
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
                eastern = pytz.timezone("America/New_York")
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
                formatted_time = dt_eastern.strftime(
                    f"%b %d, %Y at %I:%M %p {et_label}"
                )

                # Return both the formatted time and the relative time
                return html.Div(
                    [
                        html.Span(formatted_time),
                        html.Span(
                            f" ({relative_time})",
                            style={"color": "gray", "font-style": "italic"},
                        ),
                    ]
                )
            except Exception as e:
                return f"Error formatting time: {str(e)}"
        return last_run
    except Exception as e:
        return html.P(f"Error: {str(e)}")


@callback(
    Output("mart-status-display", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_mart_status(n_clicks, upload_output):
    try:
        mart_statuses = get_mart_statuses()

        # Create a grid of cards for mart statuses
        cards = []
        for mart, status in mart_statuses.items():
            # Format the mart name for display
            display_name = (
                mart.replace("_", " ")
                .title()
                .replace("Ccsr", "CCSR")
                .replace("Cms", "CMS")
                .replace("Ed", "ED")
                .replace("Pmpm", "PMPM")
            )

            # Choose icon and color based on status
            if status == "fail":
                icon = html.I(
                    className="fas fa-times-circle mart-icon",
                    style={"color": "#dc3545"},
                )
                color = "danger"
                status_text = "Not Usable"
            elif status == "warn":
                icon = html.I(
                    className="fas fa-exclamation-triangle mart-icon",
                    style={"color": "#ffc107"},
                )
                color = "warning"
                status_text = "Use with Caution"
            else:  # 'pass'
                icon = html.I(
                    className="fas fa-check-circle mart-icon",
                    style={"color": "#28a745"},
                )
                color = "success"
                status_text = "Usable"

            # Create card with clickable functionality - use a button for guaranteed click behavior
            card = dbc.Col(
                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                html.Div(icon),
                                html.H5(display_name, className="text-center"),
                                html.P(
                                    status_text, className=f"text-{color} text-center"
                                ),
                                # Add a button that looks like a link
                                dbc.Button(
                                    "View Tests",
                                    id={"type": "mart-button", "index": mart},
                                    color="link",
                                    className="mt-2 w-100",
                                ),
                            ],
                            className="mart-status-card",
                        )
                    ]
                ),
                md=4,
                className="mb-4",
            )
            cards.append(card)

        # Arrange cards in rows
        rows = []
        for i in range(0, len(cards), 3):
            rows.append(dbc.Row(cards[i : i + 3], className="mb-3"))

        return html.Div(rows)

    except Exception as e:
        return html.P(f"Error determining mart status: {str(e)}")


@callback(
    Output("outstanding-errors-table", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_outstanding_errors(n_clicks, upload_output):
    try:
        df = get_outstanding_errors()
        if df.empty:
            return html.P("No outstanding errors found.")

        # Store the full dataframe in a hidden div for later use
        hidden_data = html.Div(
            id="hidden-error-data",
            style={"display": "none"},
            children=json.dumps(df.to_dict("records")),
        )

        # Create a list of rows with buttons
        rows = []
        for i, row in df.iterrows():
            rows.append(
                dbc.Row(
                    [
                        dbc.Col(
                            str(int(row["SEVERITY_LEVEL"])),
                            width=1,
                            className="align-self-center table-cell",
                        ),  # Convert to int
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
                            row["TEST_SUB_TYPE"]
                            if pd.notna(row["TEST_SUB_TYPE"])
                            else "",
                            width=2,
                            className="align-self-center table-cell",
                        ),
                        dbc.Col(
                            dbc.Button(
                                "More Info",
                                id={"type": "error-info-button", "index": i},
                                color="info",
                                size="sm",
                                className="my-1",  # Add vertical margin to center the button
                            ),
                            width=1,
                            className="d-flex align-items-center",  # Better vertical centering
                        ),
                    ],
                    className="mb-2 border-bottom pb-2",
                    style={
                        "backgroundColor": "#ffcccc"
                        if row["SEVERITY_LEVEL"] == 1
                        else "#ffe6cc"
                        if row["SEVERITY_LEVEL"] == 2
                        else "#ffffcc"
                        if row["SEVERITY_LEVEL"] == 3
                        else "#e6ffcc"
                        if row["SEVERITY_LEVEL"] == 4
                        else "#ccffcc"
                        if row["SEVERITY_LEVEL"] == 5
                        else "white"
                    },
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
                html.Div(table_content[:10]),  # Show first 10 rows initially
                dbc.Pagination(
                    id="error-pagination",
                    max_value=max(
                        1, (len(rows) + 9) // 10
                    ),  # Ceiling division to get number of pages
                    first_last=True,
                    previous_next=True,
                    active_page=1,
                )
                if len(rows) > 10
                else html.Div(),
            ]
        )

        return table_container

    except Exception as e:
        return html.P(f"Error retrieving data: {str(e)}")


# Add a callback for pagination
@callback(
    Output("outstanding-errors-table", "children", allow_duplicate=True),
    [Input("error-pagination", "active_page")],
    [State("hidden-error-data", "children")],
    prevent_initial_call=True,
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
                dbc.Row(
                    [
                        dbc.Col(
                            str(row["SEVERITY_LEVEL"]),
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
                            row["TEST_SUB_TYPE"]
                            if pd.notna(row["TEST_SUB_TYPE"])
                            else "",
                            width=2,
                            className="align-self-center table-cell",
                        ),
                        dbc.Col(
                            dbc.Button(
                                "More Info",
                                id={"type": "error-info-button", "index": i},
                                color="info",
                                size="sm",
                                className="my-1",
                            ),
                            width=1,
                            className="d-flex align-items-center",
                        ),
                    ],
                    className="mb-2 border-bottom pb-2",
                    style={
                        "backgroundColor": "#ffcccc"
                        if row["SEVERITY_LEVEL"] == 1
                        else "#ffe6cc"
                        if row["SEVERITY_LEVEL"] == 2
                        else "#ffffcc"
                        if row["SEVERITY_LEVEL"] == 3
                        else "#e6ffcc"
                        if row["SEVERITY_LEVEL"] == 4
                        else "#ccffcc"
                        if row["SEVERITY_LEVEL"] == 5
                        else "white"
                    },
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
                html.Div(
                    id="hidden-error-data",
                    style={"display": "none"},
                    children=json_data,
                ),
                html.Div(table_content),
                dbc.Pagination(
                    id="error-pagination",
                    max_value=max(
                        1, (len(df) + 9) // 10
                    ),  # Ceiling division to get number of pages
                    first_last=True,
                    previous_next=True,
                    active_page=page,
                )
                if len(df) > 10
                else html.Div(),
            ]
        )

        return table_container

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


# Callback for the "More Info" buttons
@callback(
    [
        Output("error-modal", "is_open", allow_duplicate=True),
        Output("error-modal-body", "children", allow_duplicate=True),
    ],
    [Input({"type": "error-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-error-data", "children"), State("error-modal", "is_open")],
    prevent_initial_call=True,
)
def toggle_error_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and "index" in triggered_id:
        clicked_index = triggered_id["index"]

        try:
            # Get the data for the clicked row
            data = json.loads(json_data)
            row = data[clicked_index]

            # Create the modal content using our helper function
            modal_content = create_test_modal_content(row)

            # Check if we have the data mart flags before adding that section
            mart_flags_exist = all(
                flag in row
                for flag in [
                    "FLAG_SERVICE_CATEGORIES",
                    "FLAG_CCSR",
                    "FLAG_CMS_CHRONIC_CONDITIONS",
                    "FLAG_TUVA_CHRONIC_CONDITIONS",
                    "FLAG_CMS_HCCS",
                    "FLAG_ED_CLASSIFICATION",
                    "FLAG_FINANCIAL_PMPM",
                    "FLAG_QUALITY_MEASURES",
                    "FLAG_READMISSION",
                ]
            )

            # Add the affected data marts section if flags exist
            if mart_flags_exist:
                modal_content.extend(
                    [
                        html.Hr(),
                        html.H6("Affected Data Marts:"),
                        dbc.Row(
                            [
                                # Create icons for each affected data mart
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_SERVICE_CATEGORIES"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "Service Categories",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_CCSR"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "CCSR",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_CMS_CHRONIC_CONDITIONS"]
                                                == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "CMS Chronic Conditions",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_TUVA_CHRONIC_CONDITIONS"]
                                                == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "TUVA Chronic Conditions",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_CMS_HCCS"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "CMS HCCs",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_ED_CLASSIFICATION"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "ED Classification",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_FINANCIAL_PMPM"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "Financial PMPM",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_QUALITY_MEASURES"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "Quality Measures",
                                        ]
                                    ),
                                    width=4,
                                ),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-database text-danger mr-2"
                                                if row["FLAG_READMISSION"] == 1
                                                else "fas fa-database text-muted mr-2"
                                            ),
                                            "Readmission",
                                        ]
                                    ),
                                    width=4,
                                ),
                            ]
                        ),
                    ]
                )

            return True, modal_content

        except Exception as e:
            # Return an error message in the modal if something goes wrong
            error_content = [
                html.H5("Error Loading Test Details"),
                html.Hr(),
                html.P(f"An error occurred: {str(e)}"),
                html.Pre(traceback.format_exc()),
            ]
            return True, error_content

    return is_open, dash.no_update


@callback(
    [Output("error-modal", "is_open"), Output("error-modal-body", "children")],
    [Input({"type": "failing-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-failing-data", "children"), State("error-modal", "is_open")],
    prevent_initial_call=True,
)
def toggle_failing_test_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and "index" in triggered_id:
        clicked_index = triggered_id["index"]

        # Get the data for the clicked row
        data = json.loads(json_data)
        row = data[clicked_index]

        # Create the modal content
        modal_content = create_test_modal_content(row)

        return True, modal_content

    return is_open, dash.no_update


# Callback for the "More Info" buttons in passing tests
@callback(
    [
        Output("error-modal", "is_open", allow_duplicate=True),
        Output("error-modal-body", "children", allow_duplicate=True),
    ],
    [Input({"type": "passing-info-button", "index": dash.ALL}, "n_clicks")],
    [State("hidden-passing-data", "children"), State("error-modal", "is_open")],
    prevent_initial_call=True,
)
def toggle_passing_test_modal(btn_clicks, json_data, is_open):
    # Check if any button was clicked
    if not any(btn_clicks) or not ctx.triggered:
        return is_open, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and "index" in triggered_id:
        clicked_index = triggered_id["index"]

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
    prevent_initial_call=True,
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
        return html.Div(
            [
                html.Span(
                    "Copied to clipboard!",
                    style={"color": "green", "margin-top": "5px"},
                ),
                # Add a hidden div with JavaScript to copy to clipboard
                html.Div(
                    id="clipboard-js",
                    children=[],
                    style={"display": "none"},
                    # This will execute when the component is rendered
                    **{"data-clipboard": query_text},
                ),
            ]
        )
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
    Input("clipboard-js", "data-clipboard"),
)


@callback(
    [
        Output("mart-modal", "is_open"),
        Output("mart-modal-header", "children"),
        Output("mart-failing-tests", "children"),
        Output("mart-passing-tests", "children"),
    ],
    [Input({"type": "mart-button", "index": dash.ALL}, "n_clicks")],
    prevent_initial_call=True,
)
def toggle_mart_modal(n_clicks):
    # Check if callback was triggered by an actual click
    if not ctx.triggered_id or not any(n for n in n_clicks if n):
        return False, dash.no_update, dash.no_update, dash.no_update

    # Find which button was clicked
    triggered_id = ctx.triggered_id
    if triggered_id and "index" in triggered_id:
        clicked_mart = triggered_id["index"]

        # Format the mart name for display
        display_name = (
            clicked_mart.replace("_", " ")
            .title()
            .replace("Ccsr", "CCSR")
            .replace("Cms", "CMS")
            .replace("Ed", "ED")
            .replace("Pmpm", "PMPM")
        )

        # Get failing tests for this mart
        failing_df = get_mart_tests(clicked_mart)

        # Get passing tests for this mart
        passing_df = get_mart_tests(clicked_mart, status="pass")

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
    prevent_initial_call=True,
)
def close_mart_modal(close_clicks, is_open):
    if close_clicks:
        return False
    return is_open


@callback(
    Output("failing-tests-table-content", "children"),
    [Input("failing-pagination", "active_page")],
    [State("hidden-failing-data", "children")],
    prevent_initial_call=True,
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
            severity_level = (
                int(row["SEVERITY_LEVEL"]) if pd.notna(row["SEVERITY_LEVEL"]) else 0
            )

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
                            row["TEST_SUB_TYPE"]
                            if pd.notna(row["TEST_SUB_TYPE"])
                            else "",
                            width=2,
                            className="align-self-center table-cell",
                        ),
                        dbc.Col(
                            dbc.Button(
                                "More Info",
                                id={"type": "failing-info-button", "index": i},
                                color="info",
                                size="sm",
                                className="my-1",
                            ),
                            width=1,
                            className="d-flex align-items-center",
                        ),
                    ],
                    className="mb-2 border-bottom pb-2",
                    style={
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
        return table_content

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


@callback(
    Output("passing-tests-table-content", "children"),
    [Input("passing-pagination", "active_page")],
    [State("hidden-passing-data", "children")],
    prevent_initial_call=True,
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
            severity_level = (
                int(row["SEVERITY_LEVEL"]) if pd.notna(row["SEVERITY_LEVEL"]) else 0
            )

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
                            row["TEST_SUB_TYPE"]
                            if pd.notna(row["TEST_SUB_TYPE"])
                            else "",
                            width=2,
                            className="align-self-center table-cell",
                        ),
                        dbc.Col(
                            dbc.Button(
                                "More Info",
                                id={"type": "passing-info-button", "index": i},
                                color="info",
                                size="sm",
                                className="my-1",
                            ),
                            width=1,
                            className="d-flex align-items-center",
                        ),
                    ],
                    className=f"mb-2 border-bottom pb-2 passing-test-row passing-severity-{severity_level}",
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
        return table_content

    except Exception as e:
        return html.P(f"Error changing page: {str(e)}")


@callback(
    Output("chart-selector", "options"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_chart_selector(n_clicks, upload_output):
    charts_df = get_available_charts()

    if charts_df.empty:
        return []

    # Create options for the dropdown
    options = []
    for _, row in charts_df.iterrows():
        # Format the display name
        display_name = f"{row['DATA_QUALITY_CATEGORY'].title()}: {row['GRAPH_NAME'].replace('_', ' ').title()}"

        # Add details about the chart
        details = []
        if row["X_AXIS_DESCRIPTION"] != "N/A":
            details.append(f"X: {row['X_AXIS_DESCRIPTION']}")
        if row["Y_AXIS_DESCRIPTION"] != "N/A":
            details.append(f"Y: {row['Y_AXIS_DESCRIPTION']}")
        if row["FILTER_DESCRIPTION"] != "N/A":
            details.append(f"Filter: {row['FILTER_DESCRIPTION']}")

        if details:
            display_name += f" ({', '.join(details)})"

        options.append({"label": display_name, "value": row["GRAPH_NAME"]})

    return options


# Callback to show filter options if applicable
@callback(
    Output("chart-filter-container", "children"), Input("chart-selector", "value")
)
def update_chart_filter(selected_chart):
    if not selected_chart:
        return html.Div()

    # Get filter values for this chart
    filter_values = get_chart_filter_values(selected_chart)

    if not filter_values:
        # Return an empty div if there are no filter values
        return html.Div()

    # Get chart metadata to display filter description
    charts_df = get_available_charts()
    chart_info = charts_df[charts_df["GRAPH_NAME"] == selected_chart]

    if chart_info.empty:
        return html.Div()

    chart_info = chart_info.iloc[0]
    filter_description = chart_info["FILTER_DESCRIPTION"]

    # Create filter dropdown with a pattern-matching ID
    return html.Div(
        [
            html.Label(f"Filter by {filter_description}:"),
            dcc.Dropdown(
                id={"type": "chart-filter", "index": 0},
                options=[{"label": val, "value": val} for val in filter_values],
                value=filter_values[0] if filter_values else None,
                clearable=False,
            ),
        ]
    )


@callback(
    Output("chart-display", "children"),
    [
        Input("chart-selector", "value"),
        Input({"type": "chart-filter", "index": ALL}, "value"),
    ],
    [State("chart-filter-container", "children")],
)
def update_chart_display(selected_chart, filter_values, filter_container):
    if not selected_chart:
        return html.Div("Please select a chart to display")

    # Check if the filter exists (if filter_container is not empty)
    # If there's no filter, we can pass None to create_chart
    if not filter_container or not filter_values or not filter_values[0]:
        return create_chart(selected_chart, None)

    return create_chart(selected_chart, filter_values[0])


@callback(
    Output("data-availability-display", "children"),
    [Input("refresh-button", "n_clicks"), Input("output-data-upload", "children")],
)
def update_data_availability(n_clicks, upload_output):
    availability = get_data_availability()

    # Create badges for different data types
    test_results_badge = dbc.Badge(
        f"{availability['test_results']} records",
        color="success" if availability["test_results"] > 0 else "danger",
        className="me-1",
    )

    chart_data_badge = dbc.Badge(
        f"{availability['chart_data']} records",
        color="success" if availability["chart_data"] > 0 else "danger",
        className="me-1",
    )

    # Create cards for each data category
    cards = [
        dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H5("Test Results", className="card-title"),
                        html.P(["Status: ", test_results_badge]),
                        html.P(
                            "Data quality test results used for determining data mart usability."
                        ),
                    ]
                )
            ],
            className="mb-3",
        ),
        dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H5("Chart Data", className="card-title"),
                        html.P(["Status: ", chart_data_badge]),
                        html.P(
                            "Data for visualizing metrics across different dimensions."
                        ),
                    ]
                )
            ],
            className="mb-3",
        ),
    ]

    # Add cards for chart categories if available
    if availability["chart_categories"]:
        category_items = []
        for cat in availability["chart_categories"]:
            category_items.append(
                dbc.ListGroupItem(
                    [
                        html.Strong(cat["DATA_QUALITY_CATEGORY"].title() + ": "),
                        f"{cat['count']} data points",
                    ]
                )
            )

        cards.append(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H5(
                                "Available Chart Categories", className="card-title"
                            ),
                            dbc.ListGroup(category_items),
                        ]
                    )
                ]
            )
        )

    return html.Div(cards)

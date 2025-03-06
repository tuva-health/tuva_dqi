import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/', name='Home')

layout = html.Div([
    dbc.Row([
        dbc.Col([
            html.H1("Test Results Dashboard", className="text-center mb-4"),
            html.P("A dashboard for monitoring data quality test results.", className="lead text-center"),
            html.Hr(),

            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Test Results", className="bg-primary text-white"),
                        dbc.CardBody([
                            html.P("View and analyze test results data"),
                            dbc.Button("Go to Analytics", href="/analytics", color="primary")
                        ])
                    ])
                ], width=6),

                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Documentation", className="bg-secondary text-white"),
                        dbc.CardBody([
                            html.P("Learn about the test results schema"),
                            dbc.Button("View Documentation", id="open-docs", color="secondary")
                        ])
                    ])
                ], width=6),
            ], className="mt-4"),

            # Documentation modal
            dbc.Modal([
                dbc.ModalHeader("Test Results Schema Documentation"),
                dbc.ModalBody([
                    html.P("The test results data contains the following key fields:"),
                    html.Ul([
                        html.Li("UNIQUE_ID: Unique identifier for each test result"),
                        html.Li("DATABASE_NAME: Name of the database being tested"),
                        html.Li("SCHEMA_NAME: Name of the schema being tested"),
                        html.Li("TABLE_NAME: Name of the table being tested"),
                        html.Li("TEST_NAME: Full name of the test"),
                        html.Li("TEST_SHORT_NAME: Short name of the test"),
                        html.Li("TEST_COLUMN_NAME: Column being tested"),
                        html.Li("SEVERITY: Severity level of the test"),
                        html.Li("STATUS: Test result status (pass/fail)"),
                        html.Li("FAILURES: Number of failures"),
                        html.Li("QUALITY_DIMENSION: Quality dimension being tested"),
                    ]),
                    html.P("The dashboard allows you to:"),
                    html.Ul([
                        html.Li("Import test results from CSV files"),
                        html.Li("View summaries of test statuses and quality dimensions"),
                        html.Li("Browse detailed test results")
                    ])
                ]),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-docs", className="ml-auto")
                ),
            ], id="docs-modal", size="lg"),
        ], width=12)
    ])
])


# Add callback for documentation modal
@dash.callback(
    dash.Output("docs-modal", "is_open"),
    [dash.Input("open-docs", "n_clicks"), dash.Input("close-docs", "n_clicks")],
    [dash.State("docs-modal", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

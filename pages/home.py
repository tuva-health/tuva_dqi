import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/', name='Home')

# Hero section
hero_section = html.Div([
    html.H1("Data Quality Dashboard", className="hero-title"),
    html.P([
        "Systematically assess your healthcare data quality with the Tuva Data Quality Framework.",
    ], className="hero-subtitle"),
    dbc.Button([
        html.I(className="fas fa-chart-bar me-2"),
        " Go to DQI Dashboard"
    ], href="/analytics", color="warning", size="lg", className="btn-demo")
], className="hero-section")

# Data Quality Framework Overview
dq_framework_overview = html.Div([
    html.H2("Data Quality Framework", className="mb-4"),
    html.P([
        "Our data quality framework comprehensively assesses healthcare data mapped to the Tuva Data Model. ",
        "It provides a common language for discussing data quality issues and is designed to be easy to understand and implement."
    ], className="mb-4"),
    html.P([
        "The framework is divided into two levels:"
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Atomic-level", className="m-0")),
                dbc.CardBody([
                    html.P("Focuses on identifying problems in raw data, such as invalid values or missing fields."),
                    html.P("Examples include invalid ICD-10 codes, missing patient IDs, or date inconsistencies.")
                ])
            ], className="h-100")
        ], md=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Analytics-level", className="m-0")),
                dbc.CardBody([
                    html.P("Examines what analyses can be performed with the data and whether results are reasonable."),
                    html.P(
                        "Examples include chronic condition prevalence, encounter distributions, and readmission rates.")
                ])
            ], className="h-100")
        ], md=6, className="mb-4")
    ])
])

# Data Quality Dimensions
dq_dimensions = html.Div([
    html.H2("Quality Dimensions", className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Validity", className="m-0")),
                dbc.CardBody([
                    html.P("Data conforms to required formats and value ranges"),
                    html.P("Example: ICD-10 codes match valid diagnosis codes", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Completeness", className="m-0")),
                dbc.CardBody([
                    html.P("Required data elements are present"),
                    html.P("Example: All claims have a person ID", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Consistency", className="m-0")),
                dbc.CardBody([
                    html.P("Data values are consistent across related fields"),
                    html.P("Example: Claim start dates precede end dates", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Timeliness", className="m-0")),
                dbc.CardBody([
                    html.P("Data is available when needed and represents the correct time period"),
                    html.P("Example: Claims have appropriate date distributions", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Reasonableness", className="m-0")),
                dbc.CardBody([
                    html.P("Data values and distributions make logical sense"),
                    html.P("Example: PMPM costs fall within expected ranges", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Usability", className="m-0")),
                dbc.CardBody([
                    html.P("Data is suitable for intended analytical purposes"),
                    html.P("Example: Data marts are populated with sufficient data", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4")
    ])
])

# Data Quality Grading
dq_grading = html.Div([
    html.H2("Data Quality Grading", className="mb-4"),
    html.P("The dashboard assigns a data quality grade based on the severity of issues detected:"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade A", className="m-0 text-success")),
                dbc.CardBody([
                    html.P("No severity level 1-4 issues detected"),
                    html.P("All data marts are usable", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade B", className="m-0 text-info")),
                dbc.CardBody([
                    html.P("Has severity level 4 issues"),
                    html.P("All data marts are usable", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade C", className="m-0 text-warning")),
                dbc.CardBody([
                    html.P("Has severity level 3 issues"),
                    html.P("Some data marts may have warnings", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade D", className="m-0 text-warning")),
                dbc.CardBody([
                    html.P("Has severity level 2 issues"),
                    html.P("Some data marts may not be usable", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade F", className="m-0 text-danger")),
                dbc.CardBody([
                    html.P("Has severity level 1 issues"),
                    html.P("Most data marts are not usable", className="mb-0")
                ])
            ], className="h-100")
        ], md=4, className="mb-4"),
    ])
])

# Main layout
layout = html.Div([
    hero_section,

    dbc.Container([
        # Main content
        dbc.Tabs([
            dbc.Tab(dq_framework_overview, label="Framework Overview", tab_id="tab-1"),
            dbc.Tab(dq_dimensions, label="Quality Dimensions", tab_id="tab-2"),
            dbc.Tab(dq_grading, label="Grading System", tab_id="tab-3"),
        ], id="tabs", active_tab="tab-1", className="mb-4"),
    ])
])

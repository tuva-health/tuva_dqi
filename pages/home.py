import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/', name='Home')

# Data Quality Framework Overview
dq_framework_overview = html.Div([
    html.H4("Data Quality Framework", className="mb-3"),
    html.P([
        "Our data quality framework comprehensively assesses healthcare data mapped to the Tuva Data Model. ",
        "It provides a common language for discussing data quality issues and is designed to be easy to understand and implement."
    ]),
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
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Analytics-level", className="m-0")),
                dbc.CardBody([
                    html.P("Examines what analyses can be performed with the data and whether results are reasonable."),
                    html.P(
                        "Examples include chronic condition prevalence, encounter distributions, and readmission rates.")
                ])
            ], className="h-100")
        ], width=6)
    ], className="mb-4")
])

# Data Quality Dimensions
dq_dimensions = html.Div([
    html.H4("Quality Dimensions", className="mb-3"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Validity", className="m-0")),
                dbc.CardBody([
                    html.P("Data conforms to required formats and value ranges"),
                    html.P("Example: ICD-10 codes match valid diagnosis codes", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Completeness", className="m-0")),
                dbc.CardBody([
                    html.P("Required data elements are present"),
                    html.P("Example: All claims have a person ID", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Consistency", className="m-0")),
                dbc.CardBody([
                    html.P("Data values are consistent across related fields"),
                    html.P("Example: Claim start dates precede end dates", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Timeliness", className="m-0")),
                dbc.CardBody([
                    html.P("Data is available when needed and represents the correct time period"),
                    html.P("Example: Claims have appropriate date distributions", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Reasonableness", className="m-0")),
                dbc.CardBody([
                    html.P("Data values and distributions make logical sense"),
                    html.P("Example: PMPM costs fall within expected ranges", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Usability", className="m-0")),
                dbc.CardBody([
                    html.P("Data is suitable for intended analytical purposes"),
                    html.P("Example: Data marts are populated with sufficient data", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4")
    ])
])


# Data Quality Grading
dq_grading = html.Div([
    html.H4("Data Quality Grading", className="mb-3"),
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
        ], width=4, className="mb-4 px-2"),  
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade B", className="m-0 text-info")),
                dbc.CardBody([
                    html.P("Has severity level 4 issues"),
                    html.P("All data marts are usable", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4 px-2"),  
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade C", className="m-0 text-warning")),
                dbc.CardBody([
                    html.P("Has severity level 3 issues"),
                    html.P("Some data marts may have warnings", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4 px-2"),  
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade D", className="m-0 text-warning")),
                dbc.CardBody([
                    html.P("Has severity level 2 issues"),
                    html.P("Some data marts may not be usable", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4 px-2"),  
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Grade F", className="m-0 text-danger")),
                dbc.CardBody([
                    html.P("Has severity level 1 issues"),
                    html.P("Most data marts are not usable", className="mb-0")
                ])
            ], className="h-100")
        ], width=4, className="mb-4 px-2"),  
    ])
])

# Data Marts
data_marts = html.Div([
    html.H4("Data Marts", className="mb-3"),
    html.P("The dashboard monitors the usability status of these key data marts:"),
    dbc.Row([
        dbc.Col([
            dbc.ListGroup([
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),  # Changed mr-2 to me-3 for more space
                    "Service Categories"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "CCSR (Clinical Classifications Software Refined)"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "CMS Chronic Conditions"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "TUVA Chronic Conditions"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "CMS HCCs (Hierarchical Condition Categories)"
                ], className="d-flex align-items-center")
            ])
        ], width=6),
        dbc.Col([
            dbc.ListGroup([
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "ED Classification"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "Financial PMPM"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "Quality Measures"
                ], className="d-flex align-items-center"),
                dbc.ListGroupItem([
                    html.I(className="fas fa-database me-3"),
                    "Readmission"
                ], className="d-flex align-items-center")
            ])
        ], width=6)
    ], className="mb-4")
])

# Main layout
layout = html.Div([
    # Hero section with dashboard image
    dbc.Row([
        dbc.Col([
            html.Img(src="/assets/data_quality_dashboard.png", className="img-fluid rounded mb-4",
                     style={"max-height": "300px", "width": "auto", "margin": "0 auto", "display": "block"})
        ], width=12)
    ]),

    # Main header
    dbc.Row([
        dbc.Col([
            html.H1("Data Quality Dashboard", className="text-center mb-3"),
            html.P([
                "This dashboard monitors the quality of healthcare data mapped to the Tuva Data Model, ",
                "helping identify issues that could impact analytics and reporting."
            ], className="lead text-center mb-4"),
            html.Hr()
        ], width=12)
    ]),

    # Action buttons
    dbc.Row([
        dbc.Col([
            dbc.Button([
                html.I(className="fas fa-chart-bar mr-2"),
                " Go to DQI Dashboard"
            ], href="/analytics", color="primary", size="lg", className="w-100 mb-3")
        ], width={"size": 6, "offset": 3})
    ], className="mb-4"),

    # Tabs for different sections of documentation
    dbc.Tabs([
        dbc.Tab(dq_framework_overview, label="Framework Overview", tab_id="tab-1"),
        dbc.Tab(dq_dimensions, label="Quality Dimensions", tab_id="tab-2"),
        dbc.Tab(dq_grading, label="Grading System", tab_id="tab-3"),
        dbc.Tab(data_marts, label="Data Marts", tab_id="tab-4"),
    ], id="tabs", active_tab="tab-1", className="mb-4"),

    # Documentation modal
    dbc.Modal([
        dbc.ModalHeader("Data Quality Framework Documentation"),
        dbc.ModalBody([
            html.H5("Atomic-level Data Quality"),
            html.P([
                "Atomic-level checks focus on the raw data quality, examining issues like missing values, ",
                "invalid codes, and data consistency problems."
            ]),
            html.H6("Key atomic-level checks include:"),
            dbc.ListGroup([
                dbc.ListGroupItem("Primary key validation"),
                dbc.ListGroupItem("Person ID completeness and consistency"),
                dbc.ListGroupItem("Date field validation"),
                dbc.ListGroupItem("Diagnosis and procedure code validation"),
                dbc.ListGroupItem("Provider NPI validation"),
            ], className="mb-3"),

            html.H5("Analytics-level Data Quality"),
            html.P([
                "Analytics-level checks examine whether the data can support specific analyses ",
                "and if the results generated are reasonable."
            ]),
            html.H6("Key analytics-level checks include:"),
            dbc.ListGroup([
                dbc.ListGroupItem("Core and analytics data mart population"),
                dbc.ListGroupItem("Chronic condition prevalence"),
                dbc.ListGroupItem("Encounter types and service categories"),
                dbc.ListGroupItem("Financial PMPM metrics"),
                dbc.ListGroupItem("ED visit classification"),
                dbc.ListGroupItem("Acute inpatient metrics"),
                dbc.ListGroupItem("Readmission rates"),
                dbc.ListGroupItem("CMS-HCC risk scores"),
                dbc.ListGroupItem("Quality measure rates")
            ], className="mb-3"),

            html.H5("Severity Levels"),
            html.P("Issues are categorized by severity level:"),
            dbc.ListGroup([
                dbc.ListGroupItem(
                    [html.Strong("Level 1: "), "Critical issues that make data unusable for most analyses"]),
                dbc.ListGroupItem(
                    [html.Strong("Level 2: "), "Major issues that significantly impact specific data marts"]),
                dbc.ListGroupItem(
                    [html.Strong("Level 3: "), "Moderate issues that require caution when using certain data"]),
                dbc.ListGroupItem([html.Strong("Level 4: "), "Minor issues with limited impact on analysis"]),
                dbc.ListGroupItem([html.Strong("Level 5: "), "Informational findings with negligible impact"])
            ])
        ]),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-docs", className="ml-auto")
        ),
    ], id="docs-modal", size="lg"),
])

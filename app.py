import os

import dash
import dash_bootstrap_components as dbc
from dash import Dash, html

from utils import init_db

app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://use.fontawesome.com/releases/v5.15.1/css/all.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    ],
    suppress_callback_exceptions=True,
)

# Create a navbar with the logo and links
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use a row and col to position the logo and brand text
                dbc.Row(
                    [
                        dbc.Col(html.Img(src="/assets/tuva_logo.png", height="40px")),
                        dbc.Col(dbc.NavbarBrand("TUVA Health", className="ms-2")),
                    ],
                    align="center",
                    className="g-0",
                ),
                href="/",
                style={"textDecoration": "none"},
            ),
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                dbc.Nav(
                    [
                        dbc.NavItem(
                            dbc.NavLink(page["name"], href=page["relative_path"])
                        )
                        for page in dash.page_registry.values()
                    ],
                    className="ms-auto",
                    navbar=True,
                ),
                id="navbar-collapse",
                navbar=True,
            ),
        ]
    ),
    color="light",
    className="mb-4",
)

app.layout = html.Div(
    [
        navbar,
        dbc.Container(
            [dash.page_container], fluid=True, className="page-container px-4"
        ),
        # Add a footer
        html.Footer(
            dbc.Container(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Img(
                                        src="/assets/tuva_logo_white.png",
                                        height="40px",
                                        className="mb-3",
                                    ),
                                    html.P("© 2025 TUVA Health. All rights reserved."),
                                ],
                                width={"size": 6, "offset": 3},
                                className="text-center",
                            )
                        ]
                    )
                ]
            ),
            className="footer",
        ),
    ]
)


# Add callback for navbar toggle
@app.callback(
    dash.dependencies.Output("navbar-collapse", "is_open"),
    [dash.dependencies.Input("navbar-toggler", "n_clicks")],
    [dash.dependencies.State("navbar-collapse", "is_open")],
)
def toggle_navbar_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


server = app.server  # Expose Flask server for gunicorn

if __name__ == "__main__":
    # Get port from environment variable or use 8080 as default
    init_db()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

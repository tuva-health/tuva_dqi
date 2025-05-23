import os

import dash
import dash_bootstrap_components as dbc
from dash import Dash, html

from db import init_db
from pages.components import get_footer_component, get_navbar_component

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

app.layout = html.Div(
    [
        get_navbar_component(),
        dbc.Container(
            [dash.page_container], fluid=True, className="page-container px-4"
        ),
        # Add a footer
        get_footer_component(),
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
    dev_flag = int(os.environ.get("DEV_FLAG", 0))
    app.run(host="0.0.0.0", port=port, debug=(dev_flag == 1))

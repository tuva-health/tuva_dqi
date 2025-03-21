import dash
import dash_bootstrap_components as dbc
from dash import html


def get_navbar_component() -> dbc.Navbar:
    navbar = dbc.Navbar(
        dbc.Container(
            [
                html.A(
                    # Use a row and col to position the logo and brand text
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Img(src="/assets/tuva_logo.png", height="40px")
                            ),
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
    return navbar


def get_footer_component() -> html.Footer:
    footer = html.Footer(
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
    )
    return footer

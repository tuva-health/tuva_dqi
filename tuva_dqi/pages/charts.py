import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from tuva_dqi.pages.services import get_chart_data


def create_chart(graph_name, chart_filter=None):
    """Create a plotly figure for the specified chart."""
    # Get data for this chart
    df = get_chart_data(graph_name, chart_filter)

    if df.empty:
        # Return a message if no data is available
        return html.Div("No data available for this chart")

    # Get chart metadata from the first row
    metadata = df.iloc[0]

    # Format the title
    title = f"{metadata['DATA_QUALITY_CATEGORY'].title()}: {graph_name.replace('_', ' ').title()}"
    if chart_filter and metadata["FILTER_DESCRIPTION"] != "N/A":
        title += f" ({metadata['FILTER_DESCRIPTION']}: {chart_filter})"

    # Check if this is a matrix chart (from the name)
    is_matrix_chart = "matrix" in graph_name.lower()

    # For matrix charts, if X-axis description is not N/A but X-axis values are null, show error
    if (
        is_matrix_chart
        and metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and all(pd.isna(df["X_AXIS"]))
    ):
        return html.Div(
            [
                html.H5(title, className="text-center"),
                html.Div(
                    "Chart cannot be rendered because X axis values are missing.",
                    className="alert alert-warning",
                ),
            ]
        )

    # Check if this is a time series chart (special case)
    is_time_series = False
    if (
        "date" in metadata["X_AXIS_DESCRIPTION"].lower()
        or "date" in metadata["Y_AXIS_DESCRIPTION"].lower()
    ):
        is_time_series = True
        # Convert date strings to datetime objects for proper sorting
        if not all(pd.isna(df["X_AXIS"])):
            df["X_AXIS"] = pd.to_datetime(df["X_AXIS"], errors="coerce")
            df = df.sort_values("X_AXIS")
        if not all(pd.isna(df["Y_AXIS"])):
            df["Y_AXIS"] = pd.to_datetime(df["Y_AXIS"], errors="coerce")
            df = df.sort_values("Y_AXIS")

    # Case 1: Both X and Y axes have values - create a matrix/table view
    if (
        metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and metadata["Y_AXIS_DESCRIPTION"] != "N/A"
        and not all(pd.isna(df["X_AXIS"]))
        and not all(pd.isna(df["Y_AXIS"]))
    ):
        try:
            # Create a pivot table
            pivot_df = df.pivot_table(
                values="VALUE", index="Y_AXIS", columns="X_AXIS", aggfunc="first"
            ).reset_index()

            # Create a table figure
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=[metadata["Y_AXIS_DESCRIPTION"]]
                            + pivot_df.columns.tolist()[1:],
                            fill_color="paleturquoise",
                            align="left",
                        ),
                        cells=dict(
                            values=[pivot_df["Y_AXIS"]]
                            + [pivot_df[col] for col in pivot_df.columns[1:]],
                            fill_color="lavender",
                            align="left",
                        ),
                    )
                ]
            )

            fig.update_layout(title=title)
            return dcc.Graph(figure=fig)
        except Exception:
            # If pivot fails, fall back to a standard bar chart
            pass

    # Case 2: X axis has values, Y axis is empty or N/A - create a bar chart with X axis
    elif metadata["X_AXIS_DESCRIPTION"] != "N/A" and not all(pd.isna(df["X_AXIS"])):
        fig = px.bar(
            df,
            x="X_AXIS",
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                "X_AXIS": metadata["X_AXIS_DESCRIPTION"],
            },
        )

    # Case 3: Y axis has values, X axis is empty or N/A - create a bar chart with Y axis as X
    elif (
        metadata["Y_AXIS_DESCRIPTION"] != "N/A"
        and not all(pd.isna(df["Y_AXIS"]))
        and not is_matrix_chart
    ):
        fig = px.bar(
            df,
            x="Y_AXIS",  # Use Y_AXIS for the X-axis of the chart
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                "Y_AXIS": metadata["Y_AXIS_DESCRIPTION"],
            },
        )

    # Case 4: For non-matrix charts, if X axis description is not N/A but values are null
    elif (
        not is_matrix_chart
        and metadata["X_AXIS_DESCRIPTION"] != "N/A"
        and all(pd.isna(df["X_AXIS"]))
    ):
        # For time series charts, use the X_AXIS column in X_AXIS_DESCRIPTION
        if "over_time" in graph_name.lower():
            fig = px.bar(
                df,
                x="X_AXIS",  # This will be blank but we'll use the description
                y="VALUE",
                title=title,
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"]
                    if pd.notna(metadata["SUM_DESCRIPTION"])
                    else "Value",
                    "X_AXIS": metadata["X_AXIS_DESCRIPTION"],
                },
            )
        else:
            # For other charts, use Y_AXIS as X
            fig = px.bar(
                df,
                x="Y_AXIS",
                y="VALUE",
                title=title,
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"]
                    if pd.notna(metadata["SUM_DESCRIPTION"])
                    else "Value",
                    "Y_AXIS": metadata["Y_AXIS_DESCRIPTION"]
                    if metadata["Y_AXIS_DESCRIPTION"] != "N/A"
                    else metadata["X_AXIS_DESCRIPTION"],
                },
            )

    # Fallback case: Use whatever axis has values
    else:
        # Determine which column to use for the x-axis
        if not all(pd.isna(df["X_AXIS"])):
            x_col = "X_AXIS"
            x_label = (
                metadata["X_AXIS_DESCRIPTION"]
                if metadata["X_AXIS_DESCRIPTION"] != "N/A"
                else "X Axis"
            )
        elif not all(pd.isna(df["Y_AXIS"])) and not is_matrix_chart:
            x_col = "Y_AXIS"
            x_label = (
                metadata["Y_AXIS_DESCRIPTION"]
                if metadata["Y_AXIS_DESCRIPTION"] != "N/A"
                else "Y Axis"
            )
        else:
            # If both axes are empty or it's a matrix chart with missing X values, show error
            return html.Div(
                [
                    html.H5(title, className="text-center"),
                    html.Div(
                        "Chart cannot be rendered because necessary axis values are missing.",
                        className="alert alert-warning",
                    ),
                ]
            )

        # Create a simple bar chart
        fig = px.bar(
            df,
            x=x_col,
            y="VALUE",
            title=title,
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"]
                if pd.notna(metadata["SUM_DESCRIPTION"])
                else "Value",
                x_col: x_label,
            },
        )

    # Improve layout
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=40, t=60, b=40),
    )

    # For time series data, format the x-axis to show dates properly
    if is_time_series:
        fig.update_xaxes(tickformat="%b %Y", tickangle=45)

    return dcc.Graph(figure=fig)

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from datetime import datetime

from services.dqi_service import get_chart_data


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

    # Handle multi-filter: only filter if chart_filter is a non-empty list
    color_discrete_map = None
    filter_active = False
    if chart_filter and isinstance(chart_filter, list) and 'CHART_FILTER' in df.columns:
            df = df[df['CHART_FILTER'].isin(chart_filter)]
            filter_active = True
            color_sequence = px.colors.qualitative.Plotly
            color_map = {val: color_sequence[i % len(color_sequence)] for i, val in enumerate(chart_filter)}
            color_discrete_map = color_map
    else:
        color_discrete_map = None

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
        # Convert date strings to datetime objects for sorting
        if not all(pd.isna(df["X_AXIS"])):
            df["X_AXIS"] = pd.to_datetime(df["X_AXIS"], errors="coerce")
            df = df.sort_values("X_AXIS")
            # Add year and month columns for coloring and grouping
            df["YEAR"] = df["X_AXIS"].dt.year.astype(str)
            df["MONTH"] = df["X_AXIS"].dt.strftime("%b")
            df["MONTH_YEAR"] = df["X_AXIS"].dt.strftime("%b %Y")
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
                        columnwidth=[150] + [120] * (len(pivot_df.columns) - 1)
                    )
                ]
            )

            fig.update_layout(title=title.replace("[", " ").replace("]", ""))
            return dcc.Graph(figure=fig)
        except Exception:
            # If pivot fails, fall back to a standard bar chart
            pass

    # Case 2: X axis has values, Y axis is empty or N/A - create a bar chart with X axis
    elif metadata["X_AXIS_DESCRIPTION"] != "N/A" and not all(pd.isna(df["X_AXIS"])) and is_time_series:
        # Group by month, color by year, filter by CHART_FILTER if filter is active
        unique_years = df["YEAR"].unique()
        color_sequence = px.colors.qualitative.Plotly
        color_map = {year: color_sequence[i % len(color_sequence)] for i, year in enumerate(sorted(unique_years))}
        if "monthly" in graph_name.lower():
            fig = px.bar(
                df,
                x="MONTH",
                y="VALUE",
                color="YEAR",
                color_discrete_map=color_map,
                text="MONTH_YEAR",
                hover_data={"YEAR": True, "MONTH": True, "MONTH_YEAR": False},
                title=title.replace("[", " ").replace("]", ""),
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ") if pd.notna(metadata["SUM_DESCRIPTION"]) else "Value",
                    "MONTH": "Month",
                    "YEAR": "Year",
                },
                barmode="group"
            )
            fig.update_xaxes(categoryorder="array", categoryarray=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
        else:
            #for yearly graphs           
            fig = px.bar(
                df,
                x="YEAR",
                y="VALUE",
                hover_data={"YEAR": True, "MONTH": False, "MONTH_YEAR": False},
                title=title.replace("[", " ").replace("]", ""),
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ") if pd.notna(metadata["SUM_DESCRIPTION"]) else "Value",
                    "YEAR": "Year",
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
            x="Y_AXIS",
            y="VALUE",
            color="CHART_FILTER" if filter_active else None,
            color_discrete_map=color_discrete_map if filter_active else None,
            title=title.replace("[", " ").replace("]", ""),
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ") if pd.notna(metadata["SUM_DESCRIPTION"]) else "Value",
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
                #x=datetime.strptime("X_AXIS", "%m-%d"),  # This will be blank but we'll use the description
                x="X_AXIS",
                y="VALUE",
                color="CHART_FILTER" if color_discrete_map else None,
                color_discrete_map=color_discrete_map,
                title=title.replace("[", " ").replace("]", ""),
                barmode="group",
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ")
                    if pd.notna(metadata["SUM_DESCRIPTION"])
                    else "Value",
                    "X_AXIS":  metadata["X_AXIS_DESCRIPTION"].replace( "_", " "),
                },
            )
        else:
            # For other charts, use Y_AXIS as X
            fig = px.bar(
                df,
                x="Y_AXIS",
                y="VALUE",
                color="CHART_FILTER" if color_discrete_map else None,
                color_discrete_map=color_discrete_map,
                title=title.replace("[", " ").replace("]", ""),
                labels={
                    "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ")
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
            title=title.replace("[", " ").replace("]", ""),
            labels={
                "VALUE": metadata["SUM_DESCRIPTION"].replace( "_", " ") if pd.notna(metadata["SUM_DESCRIPTION"]) else "Value",
                x_col: x_label,
            }
        )

    # Improve layout
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=40, t=60, b=40),
    )
    if is_time_series:
        fig.update_xaxes(tickangle=45)
    return dcc.Graph(figure=fig)

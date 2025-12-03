"""
Reusable Plotly chart functions for FunnelPulse Dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

# Color palette - professional, dark theme compatible
COLORS = {
    "primary": "#0066CC",
    "secondary": "#00B4D8",
    "success": "#10B981",
    "warning": "#F59E0B",
    "danger": "#EF4444",
    "neutral": "#6B7280",
}

BRAND_COLORS = px.colors.qualitative.Set2


def create_bar_chart(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    horizontal: bool = False,
) -> go.Figure:
    """Create a bar chart with consistent styling."""
    pdf = df.to_pandas()

    if horizontal:
        fig = px.bar(
            pdf,
            y=x,
            x=y,
            title=title,
            color=color,
            orientation="h",
            color_discrete_sequence=[COLORS["primary"]],
        )
    else:
        fig = px.bar(
            pdf,
            x=x,
            y=y,
            title=title,
            color=color,
            color_discrete_sequence=[COLORS["primary"]],
        )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        title={"x": 0.5, "xanchor": "center"},
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.1)")

    return fig


def create_line_chart(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
) -> go.Figure:
    """Create a line chart with consistent styling."""
    pdf = df.to_pandas()

    fig = px.line(
        pdf,
        x=x,
        y=y,
        title=title,
        color=color,
        color_discrete_sequence=BRAND_COLORS,
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        title={"x": 0.5, "xanchor": "center"},
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": -0.3},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.1)")

    return fig


def create_funnel_chart(views: int, carts: int, purchases: int) -> go.Figure:
    """Create a funnel chart showing conversion stages."""
    fig = go.Figure(
        go.Funnel(
            y=["Views", "Carts", "Purchases"],
            x=[views, carts, purchases],
            textinfo="value+percent initial",
            marker={
                "color": [COLORS["primary"], COLORS["secondary"], COLORS["success"]]
            },
        )
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        margin={"l": 40, "r": 40, "t": 20, "b": 20},
    )

    return fig


def create_anomaly_timeline(df: pl.DataFrame) -> go.Figure:
    """Create a timeline showing anomaly counts by type."""
    pdf = df.to_pandas()

    fig = px.line(
        pdf,
        x="window_date",
        y="count",
        color="anomaly_type",
        title="Anomalies Over Time",
        color_discrete_map={"spike": COLORS["warning"], "drop": COLORS["danger"]},
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        title={"x": 0.5, "xanchor": "center"},
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": -0.3},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.1)")

    return fig


def create_brand_deep_dive(
    hourly_df: pl.DataFrame,
    anomaly_df: pl.DataFrame,
    brand: str,
) -> go.Figure:
    """Create a conversion rate chart with anomaly markers for a specific brand."""
    # Filter data for the brand
    hourly_pdf = (
        hourly_df.filter(pl.col("brand") == brand)
        .sort("window_start")
        .to_pandas()
    )
    anomaly_pdf = (
        anomaly_df.filter(pl.col("brand") == brand)
        .to_pandas()
    )

    fig = go.Figure()

    # Conversion rate line
    fig.add_trace(
        go.Scatter(
            x=hourly_pdf["window_start"],
            y=hourly_pdf["conversion_rate"],
            mode="lines",
            name="Conversion Rate",
            line={"color": COLORS["primary"]},
        )
    )

    # Add anomaly markers
    if not anomaly_pdf.empty:
        drops = anomaly_pdf[anomaly_pdf["anomaly_type"] == "drop"]
        spikes = anomaly_pdf[anomaly_pdf["anomaly_type"] == "spike"]

        if not drops.empty:
            fig.add_trace(
                go.Scatter(
                    x=drops["window_start"],
                    y=[0.01] * len(drops),  # Place at bottom
                    mode="markers",
                    name="Drops",
                    marker={"color": COLORS["danger"], "size": 10, "symbol": "triangle-down"},
                )
            )

        if not spikes.empty:
            fig.add_trace(
                go.Scatter(
                    x=spikes["window_start"],
                    y=[0.01] * len(spikes),
                    mode="markers",
                    name="Spikes",
                    marker={"color": COLORS["warning"], "size": 10, "symbol": "triangle-up"},
                )
            )

    fig.update_layout(
        title=f"Conversion Rate - {brand}",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        title_x=0.5,
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": -0.3},
        xaxis_title="Time",
        yaxis_title="Conversion Rate",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.1)")

    return fig


def format_currency(value: float) -> str:
    """Format a number as currency."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.1f}K"
    else:
        return f"${value:.2f}"


def format_number(value: int) -> str:
    """Format a large number with K/M suffix."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return str(value)


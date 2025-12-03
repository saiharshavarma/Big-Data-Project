"""
FunnelPulse Dashboard - Main Overview Page
==========================================
Real-time e-commerce funnel analytics dashboard.
"""

import streamlit as st
import polars as pl

from utils.data_loader import (
    load_daily_brand,
    load_anomalies,
    get_overall_metrics,
    get_date_range,
)
from utils.charts import (
    create_funnel_chart,
    format_currency,
    format_number,
)

# Page configuration
st.set_page_config(
    page_title="FunnelPulse Dashboard",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional styling
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #1A1F2C;
        border-radius: 8px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #0066CC;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #9CA3AF;
    }
    .stMetric {
        background-color: #1A1F2C;
        padding: 15px;
        border-radius: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def main():
    # Header
    st.title("FunnelPulse Dashboard")
    st.markdown("Real-time e-commerce funnel analytics")

    # Load data
    daily_brand = load_daily_brand()
    anomalies = load_anomalies()

    if daily_brand.is_empty():
        st.error(
            "No data available. Please ensure the gold tables exist in the tables/ directory."
        )
        st.info(
            "Run the batch pipeline notebooks (01-02) first to generate the gold tables."
        )
        return

    # Sidebar filters
    st.sidebar.header("Filters")

    min_date, max_date = get_date_range(daily_brand)
    if min_date and max_date:
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        # Filter data by date range
        if len(date_range) == 2:
            daily_brand = daily_brand.filter(
                (pl.col("date") >= date_range[0]) & (pl.col("date") <= date_range[1])
            )

    # Calculate metrics
    metrics = get_overall_metrics(daily_brand)
    anomaly_count = anomalies.height if not anomalies.is_empty() else 0

    # KPI Cards Row
    st.markdown("### Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="Total Views",
            value=format_number(metrics["total_views"]),
        )

    with col2:
        st.metric(
            label="Total Carts",
            value=format_number(metrics["total_carts"]),
        )

    with col3:
        st.metric(
            label="Total Purchases",
            value=format_number(metrics["total_purchases"]),
        )

    with col4:
        st.metric(
            label="Total Revenue",
            value=format_currency(metrics["total_revenue"]),
        )

    with col5:
        st.metric(
            label="Anomalies Detected",
            value=format_number(anomaly_count),
        )

    st.divider()

    # Funnel Visualization and Conversion Rates
    col_funnel, col_rates = st.columns([1, 1])

    with col_funnel:
        st.markdown("### Conversion Funnel")
        funnel_fig = create_funnel_chart(
            views=metrics["total_views"],
            carts=metrics["total_carts"],
            purchases=metrics["total_purchases"],
        )
        st.plotly_chart(funnel_fig, use_container_width=True)

    with col_rates:
        st.markdown("### Conversion Rates")

        # Calculate conversion rates
        view_to_cart = (
            metrics["total_carts"] / metrics["total_views"] * 100
            if metrics["total_views"] > 0
            else 0
        )
        cart_to_purchase = (
            metrics["total_purchases"] / metrics["total_carts"] * 100
            if metrics["total_carts"] > 0
            else 0
        )
        overall_conversion = (
            metrics["total_purchases"] / metrics["total_views"] * 100
            if metrics["total_views"] > 0
            else 0
        )

        rate_col1, rate_col2, rate_col3 = st.columns(3)

        with rate_col1:
            st.metric(
                label="View to Cart",
                value=f"{view_to_cart:.2f}%",
            )

        with rate_col2:
            st.metric(
                label="Cart to Purchase",
                value=f"{cart_to_purchase:.2f}%",
            )

        with rate_col3:
            st.metric(
                label="Overall Conversion",
                value=f"{overall_conversion:.2f}%",
            )

        # Summary stats
        st.markdown("---")
        st.markdown("#### Data Summary")
        st.markdown(f"- **Date Range**: {min_date} to {max_date}")
        st.markdown(f"- **Unique Brands**: {daily_brand.select('brand').n_unique()}")
        st.markdown(f"- **Data Points**: {daily_brand.height:,} daily brand records")

    st.divider()

    # Quick Stats Table
    st.markdown("### Top 10 Brands by Revenue")

    top_brands = (
        daily_brand.filter(pl.col("brand").is_not_null())
        .group_by("brand")
        .agg(
            pl.col("revenue").sum().alias("Total Revenue"),
            pl.col("views").sum().alias("Total Views"),
            pl.col("purchases").sum().alias("Total Purchases"),
        )
        .sort("Total Revenue", descending=True)
        .head(10)
    )

    st.dataframe(
        top_brands.to_pandas(),
        use_container_width=True,
        hide_index=True,
    )

    # Footer
    st.divider()
    st.markdown(
        """
        <div style='text-align: center; color: #6B7280; font-size: 0.8rem;'>
        FunnelPulse - E-Commerce Funnel Analytics Platform
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()


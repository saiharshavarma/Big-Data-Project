"""
FunnelPulse Dashboard - Executive Overview
==========================================
Real-time e-commerce conversion funnel monitoring and anomaly detection.
"""

import streamlit as st
import polars as pl

from utils.data_loader import (
    load_daily_brand,
    load_anomalies,
    get_overall_metrics,
)
from utils.charts import (
    create_funnel_chart,
    format_currency,
    format_number,
)
from utils.analytics import (
    calculate_funnel_drop_insights,
    calculate_revenue_impact,
    analyze_anomaly_patterns,
    identify_underperforming_brands,
)

# Page configuration
st.set_page_config(
    page_title="FunnelPulse - E-Commerce Funnel Analytics",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional styling
st.markdown(
    """
    <style>
    .problem-statement {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
    }
    .insight-card {
        background-color: #1A1F2C;
        border-left: 4px solid #0066CC;
        padding: 1.5rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    /* Standardize KPI metric cards */
    [data-testid="stMetric"] {
        background-color: #1A1F2C;
        padding: 20px;
        border-radius: 8px;
        height: 140px;
        box-sizing: border-box;
    }
    [data-testid="stMetricLabel"] {
        font-size: 14px !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 28px !important;
        font-weight: 600;
    }
    [data-testid="stMetricDelta"] {
        font-size: 12px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def main():
    # Problem Statement Header
    st.markdown(
        """
        <div class="problem-statement">
        <h1>FunnelPulse: E-Commerce Conversion Funnel Monitoring</h1>
        <h3>Problem Statement</h3>
        <p>E-commerce platforms lose millions in revenue due to undetected conversion funnel breakdowns. 
        Without real-time monitoring, businesses discover checkout issues, cart abandonment spikes, or 
        brand performance drops only after significant revenue loss.</p>
        <p><strong>Solution:</strong> FunnelPulse provides real-time funnel analytics with statistical 
        anomaly detection to identify conversion drops and spikes before they impact revenue.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

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

    # Calculate metrics and insights
    metrics = get_overall_metrics(daily_brand)
    funnel_insights = calculate_funnel_drop_insights(daily_brand)
    revenue_insights = calculate_revenue_impact(daily_brand)
    anomaly_insights = analyze_anomaly_patterns(anomalies) if not anomalies.is_empty() else {}
    underperformers = identify_underperforming_brands(daily_brand)

    # Sidebar info
    st.sidebar.header("Data Information")
    from utils.data_loader import ENVIRONMENT, TABLES_DIR
    st.sidebar.markdown(f"**Environment:** `{ENVIRONMENT}`")
    st.sidebar.markdown("**Dataset:** Oct-Nov 2019")
    st.sidebar.caption(f"Source: {TABLES_DIR[:40]}...")

    # Key Performance Indicators
    st.markdown("## Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="Total Views",
            value=format_number(metrics["total_views"]),
            help="Total product page views across all brands",
        )

    with col2:
        st.metric(
            label="Cart Additions",
            value=format_number(metrics["total_carts"]),
            delta=f"{funnel_insights.get('view_to_cart_rate', 0):.1f}%",
            delta_color="normal",
            help="Total items added to cart",
        )

    with col3:
        st.metric(
            label="Completed Purchases",
            value=format_number(metrics["total_purchases"]),
            delta=f"{funnel_insights.get('overall_conversion', 0):.1f}%",
            delta_color="normal",
            help="Total completed purchases",
        )

    with col4:
        st.metric(
            label="Total Revenue",
            value=format_currency(metrics["total_revenue"]),
            help="Total revenue generated",
        )

    with col5:
        anomaly_count = anomalies.height if not anomalies.is_empty() else 0
        delta_color = "inverse" if anomaly_count > 0 else "normal"
        st.metric(
            label="Anomalies Detected",
            value=format_number(anomaly_count),
            delta=f"{anomaly_insights.get('drops', 0)} drops" if anomaly_insights else None,
            delta_color=delta_color,
            help="Statistical anomalies in conversion rates",
        )

    st.divider()

    # Funnel Visualization and Insights
    col_funnel, col_insights = st.columns([1, 1])

    with col_funnel:
        st.markdown("### Conversion Funnel")
        funnel_fig = create_funnel_chart(
            views=metrics["total_views"],
            carts=metrics["total_carts"],
            purchases=metrics["total_purchases"],
        )
        st.plotly_chart(funnel_fig, use_container_width=True)

    with col_insights:
        st.markdown("### Funnel Insights")
        
        # Biggest drop-off point
        biggest_drop = funnel_insights.get("biggest_drop_off", "")
        if biggest_drop == "view_to_cart":
            st.warning(
                f"**Primary Drop-Off: View to Cart**\n\n"
                f"Only {funnel_insights.get('view_to_cart_rate', 0):.1f}% of viewers add items to cart. "
                f"This represents {format_number(funnel_insights.get('drop_off_view_cart', 0))} lost opportunities."
            )
        elif biggest_drop == "cart_to_purchase":
            st.warning(
                f"**Primary Drop-Off: Cart to Purchase**\n\n"
                f"Only {funnel_insights.get('cart_to_purchase_rate', 0):.1f}% of cart additions convert. "
                f"This represents {format_number(funnel_insights.get('drop_off_cart_purchase', 0))} abandoned carts."
            )
        
        # Revenue opportunity
        if revenue_insights.get("revenue_opportunity", 0) > 0:
            st.info(
                f"**Revenue Opportunity**: {format_currency(revenue_insights.get('revenue_opportunity', 0))}\n\n"
                f"If conversion improved by 10%, potential additional revenue: "
                f"{format_currency(revenue_insights.get('revenue_opportunity', 0))}"
            )

    st.divider()

    # Anomaly Alerts Section
    if not anomalies.is_empty() and anomaly_insights:
        st.markdown("## Anomaly Alerts")
        
        col_alert1, col_alert2, col_alert3 = st.columns(3)
        
        with col_alert1:
            st.metric(
                "Drop Anomalies",
                anomaly_insights.get("drops", 0),
                delta=f"{anomaly_insights.get('drop_percentage', 0):.1f}% of total",
                delta_color="inverse",
            )
        
        with col_alert2:
            st.metric(
                "Spike Anomalies",
                anomaly_insights.get("spikes", 0),
                delta=f"{anomaly_insights.get('spike_percentage', 0):.1f}% of total",
                delta_color="normal",
            )
        
        with col_alert3:
            st.metric(
                "Avg Severity (Z-score)",
                f"{anomaly_insights.get('avg_severity', 0):.2f}",
                help="Average absolute z-score across all anomalies",
            )
        
        # Most affected brands
        if anomaly_insights.get("most_affected_brands"):
            st.markdown("#### Most Affected Brands")
            affected_brands_df = pl.DataFrame(anomaly_insights["most_affected_brands"])
            st.dataframe(
                affected_brands_df.to_pandas(),
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # Underperforming Brands
    if not underperformers.is_empty():
        st.markdown("## Underperforming Brands (Action Required)")
        st.markdown(
            "These brands have conversion rates below the 25th percentile. "
            "Consider investigating checkout flows, pricing, or product quality."
        )
        
        display_underperformers = underperformers.select([
            pl.col("brand").alias("Brand"),
            pl.col("total_views").alias("Views"),
            pl.col("total_purchases").alias("Purchases"),
            (pl.col("conversion_rate")).alias("Conversion Rate %"),
            pl.col("total_revenue").alias("Revenue"),
        ]).head(10)
        
        st.dataframe(
            display_underperformers.to_pandas().style.format(
                {
                    "Views": "{:,.0f}",
                    "Purchases": "{:,.0f}",
                    "Conversion Rate %": "{:.2f}%",
                    "Revenue": "${:,.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # Top Performing Brands
    st.markdown("## Top 10 Brands by Revenue")
    
    top_brands = (
        daily_brand.filter(pl.col("brand").is_not_null())
        .group_by("brand")
        .agg(
            pl.col("revenue").sum().alias("Total Revenue"),
            pl.col("views").sum().alias("Total Views"),
            pl.col("purchases").sum().alias("Total Purchases"),
            (pl.col("purchases").sum() / pl.col("views").sum() * 100).alias("Conversion Rate %"),
        )
        .sort("Total Revenue", descending=True)
        .head(10)
    )

    st.dataframe(
        top_brands.to_pandas().style.format(
            {
                "Total Revenue": "${:,.2f}",
                "Total Views": "{:,.0f}",
                "Total Purchases": "{:,.0f}",
                "Conversion Rate %": "{:.2f}%",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    # Footer
    st.divider()
    st.markdown(
        """
        <div style='text-align: center; color: #6B7280; font-size: 0.8rem;'>
        FunnelPulse - Real-Time E-Commerce Funnel Analytics Platform | 
        Built with Apache Spark & Streamlit
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

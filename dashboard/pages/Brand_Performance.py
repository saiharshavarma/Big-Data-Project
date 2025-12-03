"""
FunnelPulse Dashboard - Brand Performance Analysis
===================================================
Detailed brand performance analysis with conversion metrics and comparisons.
"""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from utils.data_loader import (
    load_daily_brand,
    get_top_brands,
)
from utils.charts import (
    create_bar_chart,
    format_currency,
    format_number,
    COLORS,
)
from utils.analytics import (
    identify_underperforming_brands,
    calculate_revenue_impact,
)

st.set_page_config(
    page_title="Brand Performance Analysis - FunnelPulse",
    page_icon=None,
    layout="wide",
)

st.title("Brand Performance Analysis")
st.markdown(
    """
    **Business Context**: In e-commerce, brand performance varies significantly. 
    Some brands convert well but generate low revenue, while others have high traffic but poor conversion.
    Understanding these patterns helps prioritize optimization efforts and identify revenue opportunities.
    """
)

# Load data
daily_brand = load_daily_brand()

if daily_brand.is_empty():
    st.error("No data available. Please run the batch pipeline first.")
    st.stop()

# Sidebar filters
st.sidebar.header("Brand Filters")

# Get top brands for selection
all_brands = (
    daily_brand.filter(pl.col("brand").is_not_null())
    .select("brand")
    .unique()
    .sort("brand")
    .to_series()
    .to_list()
)
top_brands = get_top_brands(daily_brand, n=10)

# Brand selector
selected_brands = st.sidebar.multiselect(
    "Compare Brands",
    options=all_brands,
    default=top_brands[:5],
    help="Select brands to compare in detail",
)

if not selected_brands:
    st.warning("Please select at least one brand from the sidebar.")
    st.stop()

st.divider()

# Aggregate brand data
brand_metrics = (
    daily_brand.filter(pl.col("brand").is_not_null())
    .group_by("brand")
    .agg(
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("views").sum().alias("total_views"),
        pl.col("carts").sum().alias("total_carts"),
        pl.col("purchases").sum().alias("total_purchases"),
    )
    .with_columns([
        (pl.col("total_carts") / pl.col("total_views") * 100).alias("view_to_cart_rate"),
        (pl.col("total_purchases") / pl.col("total_carts") * 100).alias("cart_to_purchase_rate"),
        (pl.col("total_purchases") / pl.col("total_views") * 100).alias("conversion_rate"),
        (pl.col("total_revenue") / pl.col("total_purchases")).alias("avg_order_value"),
    ])
    .sort("total_revenue", descending=True)
)

# Section 1: Revenue Analysis
st.markdown("## Revenue Distribution by Brand")
st.markdown(
    "Understanding which brands drive the most revenue helps prioritize marketing and optimization efforts."
)

col_rev1, col_rev2 = st.columns([2, 1])

with col_rev1:
    # Top 10 brands revenue bar chart
    top_10_revenue = brand_metrics.head(10)
    
    fig_revenue = px.bar(
        top_10_revenue.to_pandas(),
        x="brand",
        y="total_revenue",
        title="Top 10 Brands by Revenue",
        color="total_revenue",
        color_continuous_scale=["#1a1f2c", "#0066CC"],
    )
    fig_revenue.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        showlegend=False,
        xaxis_title="Brand",
        yaxis_title="Total Revenue ($)",
    )
    fig_revenue.update_coloraxes(showscale=False)
    st.plotly_chart(fig_revenue, use_container_width=True)

with col_rev2:
    # Revenue concentration insight
    total_revenue = brand_metrics.select("total_revenue").sum().item()
    top_10_revenue_sum = top_10_revenue.select("total_revenue").sum().item()
    concentration = (top_10_revenue_sum / total_revenue * 100) if total_revenue > 0 else 0
    
    st.metric("Revenue Concentration", f"{concentration:.1f}%")
    st.caption("Top 10 brands' share of total revenue")
    
    st.metric("Total Brands", f"{brand_metrics.height:,}")
    st.caption("Active brands in dataset")
    
    st.metric("Top Brand Revenue", format_currency(top_10_revenue.head(1).select("total_revenue").item()))
    top_brand_name = top_10_revenue.head(1).select("brand").item()
    st.caption(f"Leading brand: {top_brand_name}")

st.divider()

# Section 2: Conversion Funnel Comparison
st.markdown("## Conversion Performance Comparison")
st.markdown(
    "Compare how selected brands perform at each stage of the funnel. "
    "Identify which brands excel at attracting views vs. converting to purchases."
)

# Filter to selected brands
selected_metrics = brand_metrics.filter(pl.col("brand").is_in(selected_brands))

col_conv1, col_conv2 = st.columns(2)

with col_conv1:
    # View to Cart Rate comparison
    fig_v2c = px.bar(
        selected_metrics.sort("view_to_cart_rate", descending=True).to_pandas(),
        x="brand",
        y="view_to_cart_rate",
        title="View to Cart Rate by Brand",
        color="view_to_cart_rate",
        color_continuous_scale=["#EF4444", "#F59E0B", "#10B981"],
    )
    fig_v2c.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        showlegend=False,
        xaxis_title="Brand",
        yaxis_title="View to Cart Rate (%)",
    )
    fig_v2c.update_coloraxes(showscale=False)
    st.plotly_chart(fig_v2c, use_container_width=True)

with col_conv2:
    # Cart to Purchase Rate comparison
    fig_c2p = px.bar(
        selected_metrics.sort("cart_to_purchase_rate", descending=True).to_pandas(),
        x="brand",
        y="cart_to_purchase_rate",
        title="Cart to Purchase Rate by Brand",
        color="cart_to_purchase_rate",
        color_continuous_scale=["#EF4444", "#F59E0B", "#10B981"],
    )
    fig_c2p.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#FAFAFA"},
        showlegend=False,
        xaxis_title="Brand",
        yaxis_title="Cart to Purchase Rate (%)",
    )
    fig_c2p.update_coloraxes(showscale=False)
    st.plotly_chart(fig_c2p, use_container_width=True)

st.divider()

# Section 3: Traffic vs Conversion Scatter Plot
st.markdown("## Traffic vs. Conversion Analysis")
st.markdown(
    "This scatter plot reveals brand positioning: high-traffic/low-conversion brands need checkout optimization, "
    "while high-conversion/low-traffic brands could benefit from more marketing investment."
)

# Scatter plot: Views vs Conversion Rate, sized by revenue
scatter_data = selected_metrics.to_pandas()

fig_scatter = px.scatter(
    scatter_data,
    x="total_views",
    y="conversion_rate",
    size="total_revenue",
    color="brand",
    hover_name="brand",
    hover_data={
        "total_views": ":,",
        "conversion_rate": ":.2f",
        "total_revenue": ":$,.0f",
    },
    title="Brand Positioning: Traffic vs. Conversion (Bubble Size = Revenue)",
)
fig_scatter.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font={"color": "#FAFAFA"},
    xaxis_title="Total Views (Traffic Volume)",
    yaxis_title="Conversion Rate (%)",
    legend={"orientation": "h", "yanchor": "bottom", "y": -0.3},
)
st.plotly_chart(fig_scatter, use_container_width=True)

# Quadrant analysis
avg_views = scatter_data["total_views"].mean()
avg_conv = scatter_data["conversion_rate"].mean()

high_traffic_low_conv = scatter_data[
    (scatter_data["total_views"] > avg_views) & (scatter_data["conversion_rate"] < avg_conv)
]["brand"].tolist()

low_traffic_high_conv = scatter_data[
    (scatter_data["total_views"] < avg_views) & (scatter_data["conversion_rate"] > avg_conv)
]["brand"].tolist()

col_quad1, col_quad2 = st.columns(2)

with col_quad1:
    if high_traffic_low_conv:
        st.warning(
            f"**Optimize Conversion**: {', '.join(high_traffic_low_conv[:3])}\n\n"
            "These brands have high traffic but below-average conversion. "
            "Focus on checkout optimization, pricing, or product presentation."
        )

with col_quad2:
    if low_traffic_high_conv:
        st.success(
            f"**Growth Opportunity**: {', '.join(low_traffic_high_conv[:3])}\n\n"
            "These brands convert well but have lower traffic. "
            "Increasing marketing spend could drive significant revenue growth."
        )

st.divider()

# Section 4: Revenue Opportunity Analysis
st.markdown("## Revenue Opportunity Analysis")

brand_revenue_impact = calculate_revenue_impact(
    daily_brand.filter(pl.col("brand").is_in(selected_brands))
)

col_opp1, col_opp2, col_opp3 = st.columns(3)

with col_opp1:
    st.metric(
        "Current Conversion Rate",
        f"{brand_revenue_impact.get('current_conversion', 0):.2f}%",
    )
    st.caption("For selected brands")

with col_opp2:
    st.metric(
        "If Conversion Improved 10%",
        format_currency(brand_revenue_impact.get('revenue_opportunity', 0)),
        delta=f"+{brand_revenue_impact.get('additional_purchases', 0):,.0f} orders",
    )
    st.caption("Potential additional revenue")

with col_opp3:
    current_rev = brand_revenue_impact.get('current_revenue', 0)
    potential = brand_revenue_impact.get('revenue_opportunity', 0)
    growth_pct = (potential / current_rev * 100) if current_rev > 0 else 0
    st.metric(
        "Revenue Growth Potential",
        f"+{growth_pct:.1f}%",
    )
    st.caption("From conversion optimization")

st.divider()

# Section 5: Brand Performance Leaderboard
st.markdown("## Brand Performance Leaderboard")

leaderboard = (
    brand_metrics
    .select([
        pl.col("brand").alias("Brand"),
        pl.col("total_revenue").alias("Revenue"),
        pl.col("total_views").alias("Views"),
        pl.col("total_purchases").alias("Purchases"),
        pl.col("conversion_rate").alias("Conv Rate %"),
        pl.col("avg_order_value").alias("Avg Order $"),
    ])
    .head(20)
)

st.dataframe(
    leaderboard.to_pandas().style.format(
        {
            "Revenue": "${:,.2f}",
            "Views": "{:,.0f}",
            "Purchases": "{:,.0f}",
            "Conv Rate %": "{:.2f}%",
            "Avg Order $": "${:,.2f}",
        }
    ),
    use_container_width=True,
    hide_index=True,
)

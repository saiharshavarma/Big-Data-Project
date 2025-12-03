"""
FunnelPulse Dashboard - Brand Analytics Page
============================================
Detailed brand performance analysis.
"""

import streamlit as st
import polars as pl

from utils.data_loader import (
    load_daily_brand,
    get_top_brands,
    get_date_range,
)
from utils.charts import (
    create_bar_chart,
    create_line_chart,
    format_currency,
    format_number,
)

st.set_page_config(
    page_title="Brand Analytics - FunnelPulse",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.title("Brand Analytics")
st.markdown("Performance analysis by brand")

# Load data
daily_brand = load_daily_brand()

if daily_brand.is_empty():
    st.error("No data available. Please run the batch pipeline first.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date, max_date = get_date_range(daily_brand)
if min_date and max_date:
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if len(date_range) == 2:
        daily_brand = daily_brand.filter(
            (pl.col("date") >= date_range[0]) & (pl.col("date") <= date_range[1])
        )

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
    "Select Brands",
    options=all_brands,
    default=top_brands[:5],
    help="Select brands to compare",
)

if not selected_brands:
    st.warning("Please select at least one brand from the sidebar.")
    st.stop()

st.divider()

# Top Brands by Revenue Bar Chart
st.markdown("### Top 10 Brands by Revenue")

brand_totals = (
    daily_brand.filter(pl.col("brand").is_not_null())
    .group_by("brand")
    .agg(
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("views").sum().alias("total_views"),
        pl.col("purchases").sum().alias("total_purchases"),
    )
    .sort("total_revenue", descending=True)
    .head(10)
)

bar_fig = create_bar_chart(
    brand_totals,
    x="brand",
    y="total_revenue",
    title="Total Revenue by Brand (Oct-Nov 2019)",
)
st.plotly_chart(bar_fig, use_container_width=True)

st.divider()

# Filter for selected brands
filtered_data = daily_brand.filter(pl.col("brand").is_in(selected_brands))

# Revenue Trends
st.markdown("### Daily Revenue Trends")

revenue_data = (
    filtered_data.select(["date", "brand", "revenue"])
    .sort(["date", "brand"])
)

revenue_fig = create_line_chart(
    revenue_data,
    x="date",
    y="revenue",
    color="brand",
    title="Daily Revenue by Brand",
)
st.plotly_chart(revenue_fig, use_container_width=True)

st.divider()

# Conversion Rate Trends
st.markdown("### Daily Conversion Rate Trends")

conversion_data = (
    filtered_data.select(["date", "brand", "conversion_rate"])
    .sort(["date", "brand"])
)

conversion_fig = create_line_chart(
    conversion_data,
    x="date",
    y="conversion_rate",
    color="brand",
    title="Daily Conversion Rate by Brand",
)
st.plotly_chart(conversion_fig, use_container_width=True)

st.divider()

# Brand Leaderboard Table
st.markdown("### Brand Leaderboard")

leaderboard = (
    daily_brand.filter(pl.col("brand").is_not_null())
    .group_by("brand")
    .agg(
        pl.col("revenue").sum().alias("Total Revenue"),
        pl.col("views").sum().alias("Total Views"),
        pl.col("carts").sum().alias("Total Carts"),
        pl.col("purchases").sum().alias("Total Purchases"),
    )
    .with_columns(
        (pl.col("Total Purchases") / pl.col("Total Views") * 100).alias("Conv Rate %")
    )
    .sort("Total Revenue", descending=True)
)

# Add rank column
leaderboard = leaderboard.with_row_index("Rank", offset=1)

st.dataframe(
    leaderboard.to_pandas().style.format(
        {
            "Total Revenue": "${:,.2f}",
            "Total Views": "{:,.0f}",
            "Total Carts": "{:,.0f}",
            "Total Purchases": "{:,.0f}",
            "Conv Rate %": "{:.2f}%",
        }
    ),
    use_container_width=True,
    hide_index=True,
)


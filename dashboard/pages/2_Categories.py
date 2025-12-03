"""
FunnelPulse Dashboard - Category Performance Page
=================================================
Performance analysis by product category.
"""

import streamlit as st
import polars as pl

from utils.data_loader import load_daily_category, get_date_range
from utils.charts import create_bar_chart, format_currency, format_number

st.set_page_config(
    page_title="Category Performance - FunnelPulse",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.title("Category Performance")
st.markdown("Performance analysis by product category")

# Load data
daily_category = load_daily_category()

if daily_category.is_empty():
    st.error("No data available. Please run the batch pipeline first.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date, max_date = get_date_range(daily_category)
if min_date and max_date:
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if len(date_range) == 2:
        daily_category = daily_category.filter(
            (pl.col("date") >= date_range[0]) & (pl.col("date") <= date_range[1])
        )

st.divider()

# Aggregate by category
category_totals = (
    daily_category.group_by("category_root")
    .agg(
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("views").sum().alias("total_views"),
        pl.col("purchases").sum().alias("total_purchases"),
    )
    .with_columns(
        pl.when(pl.col("category_root").is_null())
        .then(pl.lit("Unknown"))
        .otherwise(pl.col("category_root"))
        .alias("category_root")
    )
    .with_columns(
        (pl.col("total_purchases") / pl.col("total_views")).alias("conversion_rate")
    )
    .sort("total_revenue", descending=True)
)

# KPI Cards
st.markdown("### Category Overview")

col1, col2, col3 = st.columns(3)

with col1:
    total_revenue = category_totals.select("total_revenue").sum().item()
    st.metric("Total Revenue", format_currency(total_revenue))

with col2:
    num_categories = category_totals.height
    st.metric("Categories", str(num_categories))

with col3:
    avg_conversion = (
        category_totals.select("total_purchases").sum().item()
        / category_totals.select("total_views").sum().item()
        * 100
    )
    st.metric("Avg Conversion", f"{avg_conversion:.2f}%")

st.divider()

# Revenue by Category Bar Chart
col_rev, col_conv = st.columns(2)

with col_rev:
    st.markdown("### Revenue by Category")
    revenue_fig = create_bar_chart(
        category_totals,
        x="category_root",
        y="total_revenue",
        title="Total Revenue by Category",
    )
    st.plotly_chart(revenue_fig, use_container_width=True)

with col_conv:
    st.markdown("### Conversion Rate by Category")
    conversion_fig = create_bar_chart(
        category_totals,
        x="category_root",
        y="conversion_rate",
        title="Conversion Rate by Category",
    )
    st.plotly_chart(conversion_fig, use_container_width=True)

st.divider()

# Category Details Table
st.markdown("### Category Details")

display_df = category_totals.select(
    pl.col("category_root").alias("Category"),
    pl.col("total_revenue").alias("Total Revenue"),
    pl.col("total_views").alias("Total Views"),
    pl.col("total_purchases").alias("Total Purchases"),
    (pl.col("conversion_rate") * 100).alias("Conversion Rate %"),
)

st.dataframe(
    display_df.to_pandas().style.format(
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

st.divider()

# Category Insights
st.markdown("### Insights")

top_category = category_totals.head(1).to_dicts()[0]
bottom_category = category_totals.tail(1).to_dicts()[0]

col_insight1, col_insight2 = st.columns(2)

with col_insight1:
    st.markdown("#### Top Performing Category")
    st.markdown(f"**{top_category['category_root']}**")
    st.markdown(f"- Revenue: {format_currency(top_category['total_revenue'])}")
    st.markdown(f"- Views: {format_number(top_category['total_views'])}")
    st.markdown(f"- Conversion: {top_category['conversion_rate']*100:.2f}%")

with col_insight2:
    st.markdown("#### Lowest Performing Category")
    st.markdown(f"**{bottom_category['category_root']}**")
    st.markdown(f"- Revenue: {format_currency(bottom_category['total_revenue'])}")
    st.markdown(f"- Views: {format_number(bottom_category['total_views'])}")
    st.markdown(f"- Conversion: {bottom_category['conversion_rate']*100:.2f}%")


"""
FunnelPulse Dashboard - Price Band Analysis Page
================================================
Performance analysis by price segment.
"""

import streamlit as st
import polars as pl

from utils.data_loader import load_hourly_price_band
from utils.charts import create_bar_chart, create_line_chart, format_currency, format_number

st.set_page_config(
    page_title="Price Band Analysis - FunnelPulse",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.title("Price Band Analysis")
st.markdown("Performance analysis by price segment")

# Load data
hourly_price = load_hourly_price_band()

if hourly_price.is_empty():
    st.error("No data available. Please run the batch pipeline first.")
    st.stop()

# Define price band order for consistent display
PRICE_BAND_ORDER = ["<10", "10-30", "30-60", "60_plus"]

st.divider()

# Aggregate by price band
price_totals = (
    hourly_price.group_by("price_band")
    .agg(
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("views").sum().alias("total_views"),
        pl.col("purchases").sum().alias("total_purchases"),
    )
    .with_columns(
        pl.when(pl.col("price_band").is_null())
        .then(pl.lit("Unknown"))
        .otherwise(pl.col("price_band"))
        .alias("price_band")
    )
    .with_columns(
        (pl.col("total_purchases") / pl.col("total_views")).alias("conversion_rate")
    )
)

# Sort by price band order
price_totals = price_totals.with_columns(
    pl.col("price_band")
    .replace(
        {band: str(i) for i, band in enumerate(PRICE_BAND_ORDER)},
        default="99",
    )
    .alias("sort_order")
).sort("sort_order").drop("sort_order")

# KPI Cards
st.markdown("### Price Band Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_revenue = price_totals.select("total_revenue").sum().item()
    st.metric("Total Revenue", format_currency(total_revenue))

with col2:
    total_views = price_totals.select("total_views").sum().item()
    st.metric("Total Views", format_number(total_views))

with col3:
    total_purchases = price_totals.select("total_purchases").sum().item()
    st.metric("Total Purchases", format_number(total_purchases))

with col4:
    overall_conv = total_purchases / total_views * 100 if total_views > 0 else 0
    st.metric("Overall Conversion", f"{overall_conv:.2f}%")

st.divider()

# Charts
col_rev, col_conv = st.columns(2)

with col_rev:
    st.markdown("### Revenue by Price Band")
    revenue_fig = create_bar_chart(
        price_totals,
        x="price_band",
        y="total_revenue",
        title="Total Revenue by Price Band",
    )
    st.plotly_chart(revenue_fig, use_container_width=True)

with col_conv:
    st.markdown("### Conversion Rate by Price Band")
    conversion_fig = create_bar_chart(
        price_totals,
        x="price_band",
        y="conversion_rate",
        title="Conversion Rate by Price Band",
    )
    st.plotly_chart(conversion_fig, use_container_width=True)

st.divider()

# Daily trends by price band
st.markdown("### Daily Conversion Trends by Price Band")

daily_price = (
    hourly_price.with_columns(pl.col("window_start").cast(pl.Date).alias("date"))
    .group_by(["date", "price_band"])
    .agg(
        pl.col("views").sum().alias("views"),
        pl.col("purchases").sum().alias("purchases"),
    )
    .with_columns(
        (pl.col("purchases") / pl.col("views")).alias("conversion_rate")
    )
    .sort(["date", "price_band"])
)

if not daily_price.is_empty():
    trend_fig = create_line_chart(
        daily_price,
        x="date",
        y="conversion_rate",
        color="price_band",
        title="Daily Conversion Rate by Price Band",
    )
    st.plotly_chart(trend_fig, use_container_width=True)

st.divider()

# Price Band Details Table
st.markdown("### Price Band Details")

display_df = price_totals.select(
    pl.col("price_band").alias("Price Band"),
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

# Insights
st.markdown("### Insights")

# Find best and worst performing bands
best_conv = price_totals.sort("conversion_rate", descending=True).head(1).to_dicts()[0]
most_revenue = price_totals.sort("total_revenue", descending=True).head(1).to_dicts()[0]

col_i1, col_i2 = st.columns(2)

with col_i1:
    st.markdown("#### Highest Conversion Rate")
    st.markdown(f"**Price Band: {best_conv['price_band']}**")
    st.markdown(f"- Conversion Rate: {best_conv['conversion_rate']*100:.2f}%")
    st.markdown(f"- Revenue: {format_currency(best_conv['total_revenue'])}")

with col_i2:
    st.markdown("#### Highest Revenue")
    st.markdown(f"**Price Band: {most_revenue['price_band']}**")
    st.markdown(f"- Revenue: {format_currency(most_revenue['total_revenue'])}")
    st.markdown(f"- Conversion Rate: {most_revenue['conversion_rate']*100:.2f}%")


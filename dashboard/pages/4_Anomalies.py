"""
FunnelPulse Dashboard - Anomaly Monitoring Page
===============================================
Statistical anomaly detection and alerting.
"""

import streamlit as st
import polars as pl

from utils.data_loader import (
    load_anomalies,
    load_hourly_brand,
    get_top_brands,
    load_daily_brand,
)
from utils.charts import (
    create_anomaly_timeline,
    create_brand_deep_dive,
    format_number,
)

st.set_page_config(
    page_title="Anomaly Monitoring - FunnelPulse",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.title("Anomaly Monitoring")
st.markdown("Statistical anomaly detection for conversion rate changes")

# Load data
anomalies = load_anomalies()
hourly_brand = load_hourly_brand()
daily_brand = load_daily_brand()

if anomalies.is_empty():
    st.warning("No anomalies detected in the current dataset.")
    st.info("Anomalies are flagged when conversion rates deviate significantly from historical baselines (z-score > 2.0).")
    st.stop()

st.divider()

# KPI Cards
st.markdown("### Anomaly Summary")

total_anomalies = anomalies.height
drops = anomalies.filter(pl.col("anomaly_type") == "drop").height
spikes = anomalies.filter(pl.col("anomaly_type") == "spike").height

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Anomalies", format_number(total_anomalies))

with col2:
    st.metric("Drops", format_number(drops), delta=None)

with col3:
    st.metric("Spikes", format_number(spikes), delta=None)

with col4:
    affected_brands = anomalies.select("brand").n_unique()
    st.metric("Affected Brands", format_number(affected_brands))

st.divider()

# Anomaly Timeline
st.markdown("### Anomalies Over Time")

# Aggregate anomalies by date and type
anomaly_by_date = (
    anomalies.group_by(["window_date", "anomaly_type"])
    .agg(pl.count().alias("count"))
    .sort(["window_date", "anomaly_type"])
)

timeline_fig = create_anomaly_timeline(anomaly_by_date)
st.plotly_chart(timeline_fig, use_container_width=True)

st.divider()

# Sidebar filters
st.sidebar.header("Filters")

anomaly_types = ["All", "drop", "spike"]
selected_type = st.sidebar.selectbox("Anomaly Type", anomaly_types)

# Filter anomalies
filtered_anomalies = anomalies
if selected_type != "All":
    filtered_anomalies = anomalies.filter(pl.col("anomaly_type") == selected_type)

# Brand filter for deep dive
top_brands = get_top_brands(daily_brand, n=20)
anomaly_brands = (
    anomalies.select("brand")
    .unique()
    .sort("brand")
    .to_series()
    .to_list()
)

# Anomaly Table
st.markdown("### Anomaly Details")

# Select columns to display
display_cols = [
    "window_date",
    "window_start",
    "brand",
    "anomaly_type",
    "views",
    "conversion_rate",
    "z_brand",
    "z_brand_hour",
]

available_cols = [c for c in display_cols if c in filtered_anomalies.columns]
display_df = filtered_anomalies.select(available_cols).sort("window_date", descending=True)

# Add severity indicator
display_df = display_df.with_columns(
    pl.when(pl.col("z_brand").abs() > 3)
    .then(pl.lit("High"))
    .when(pl.col("z_brand").abs() > 2.5)
    .then(pl.lit("Medium"))
    .otherwise(pl.lit("Low"))
    .alias("Severity")
)

st.dataframe(
    display_df.to_pandas().style.format(
        {
            "conversion_rate": "{:.4f}",
            "z_brand": "{:.2f}",
            "z_brand_hour": "{:.2f}",
        }
    ),
    use_container_width=True,
    hide_index=True,
    height=400,
)

st.divider()

# Brand Deep Dive
st.markdown("### Brand Deep Dive")
st.markdown("Select a brand to view its conversion rate history with anomaly markers.")

# Brand selector
selected_brand = st.selectbox(
    "Select Brand",
    options=anomaly_brands if anomaly_brands else top_brands,
    index=0,
)

if selected_brand and not hourly_brand.is_empty():
    deep_dive_fig = create_brand_deep_dive(
        hourly_brand,
        anomalies,
        selected_brand,
    )
    st.plotly_chart(deep_dive_fig, use_container_width=True)

    # Show brand-specific anomalies
    brand_anomalies = filtered_anomalies.filter(pl.col("brand") == selected_brand)
    if not brand_anomalies.is_empty():
        st.markdown(f"#### Anomalies for {selected_brand}")
        st.dataframe(
            brand_anomalies.select(available_cols).to_pandas().style.format(
                {
                    "conversion_rate": "{:.4f}",
                    "z_brand": "{:.2f}",
                    "z_brand_hour": "{:.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(f"No anomalies detected for {selected_brand} with current filters.")

st.divider()

# Top Anomalous Brands
st.markdown("### Most Anomalous Brands")

brand_anomaly_counts = (
    anomalies.group_by("brand")
    .agg(
        pl.count().alias("anomaly_count"),
        pl.col("anomaly_type").filter(pl.col("anomaly_type") == "drop").count().alias("drops"),
        pl.col("anomaly_type").filter(pl.col("anomaly_type") == "spike").count().alias("spikes"),
    )
    .sort("anomaly_count", descending=True)
    .head(10)
)

st.dataframe(
    brand_anomaly_counts.to_pandas(),
    use_container_width=True,
    hide_index=True,
)

# Methodology note
st.divider()
st.markdown("### Methodology")
st.markdown(
    """
    Anomalies are detected using **Z-score analysis**:

    1. **Per-brand baseline**: Mean and standard deviation of conversion rate across all hours
    2. **Per-brand-hour baseline**: Mean and standard deviation at each hour of day (0-23)
    3. **Z-score calculation**: `z = (current - mean) / std`
    4. **Thresholds**:
       - **Drop**: z-score <= -2.0 (conversion significantly below normal)
       - **Spike**: z-score >= +2.0 (conversion significantly above normal)
       - Minimum 50 views required to flag anomaly
    """
)


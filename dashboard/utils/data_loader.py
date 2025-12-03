"""
Data loading utilities for FunnelPulse Dashboard.
Supports both local filesystem and GCS paths using Polars.
"""

import os
from functools import lru_cache

import polars as pl
import streamlit as st


# Environment configuration (mirrors config.py pattern)
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

if ENVIRONMENT == "gcp":
    GCS_BUCKET = os.getenv("GCS_BUCKET", "funnelpulse-ss18851-data")
    TABLES_DIR = f"gs://{GCS_BUCKET}/tables"
else:
    # Local development - relative to project root
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    TABLES_DIR = os.path.join(PROJECT_ROOT, "tables")

# Table paths
GOLD_FUNNEL_DAILY_BRAND = f"{TABLES_DIR}/gold_funnel_daily_brand"
GOLD_FUNNEL_DAILY_CATEGORY = f"{TABLES_DIR}/gold_funnel_daily_category"
GOLD_FUNNEL_HOURLY_PRICE_BAND = f"{TABLES_DIR}/gold_funnel_hourly_price_band"
GOLD_FUNNEL_HOURLY_BRAND = f"{TABLES_DIR}/gold_funnel_hourly_brand"
GOLD_ANOMALIES_HOURLY_BRAND = f"{TABLES_DIR}/gold_anomalies_hourly_brand"


def _read_parquet(path: str) -> pl.DataFrame:
    """Read parquet files from local or GCS path."""
    try:
        if path.startswith("gs://"):
            # GCS path - use fsspec/gcsfs through Polars
            return pl.read_parquet(path, use_pyarrow=True)
        else:
            # Local path
            return pl.read_parquet(path)
    except Exception as e:
        st.error(f"Failed to load data from {path}: {e}")
        return pl.DataFrame()


@st.cache_data(ttl=3600)
def load_daily_brand() -> pl.DataFrame:
    """Load gold_funnel_daily_brand table."""
    return _read_parquet(GOLD_FUNNEL_DAILY_BRAND)


@st.cache_data(ttl=3600)
def load_daily_category() -> pl.DataFrame:
    """Load gold_funnel_daily_category table."""
    return _read_parquet(GOLD_FUNNEL_DAILY_CATEGORY)


@st.cache_data(ttl=3600)
def load_hourly_price_band() -> pl.DataFrame:
    """Load gold_funnel_hourly_price_band table."""
    return _read_parquet(GOLD_FUNNEL_HOURLY_PRICE_BAND)


@st.cache_data(ttl=3600)
def load_hourly_brand() -> pl.DataFrame:
    """Load gold_funnel_hourly_brand table."""
    return _read_parquet(GOLD_FUNNEL_HOURLY_BRAND)


@st.cache_data(ttl=3600)
def load_anomalies() -> pl.DataFrame:
    """Load gold_anomalies_hourly_brand table."""
    return _read_parquet(GOLD_ANOMALIES_HOURLY_BRAND)


def get_date_range(df: pl.DataFrame, date_col: str = "date") -> tuple:
    """Get min and max dates from a dataframe."""
    if df.is_empty():
        return None, None
    dates = df.select(pl.col(date_col)).to_series()
    return dates.min(), dates.max()


def get_top_brands(df: pl.DataFrame, n: int = 10) -> list[str]:
    """Get top N brands by total revenue."""
    if df.is_empty():
        return []
    top = (
        df.filter(pl.col("brand").is_not_null())
        .group_by("brand")
        .agg(pl.col("revenue").sum().alias("total_revenue"))
        .sort("total_revenue", descending=True)
        .head(n)
    )
    return top["brand"].to_list()


def get_overall_metrics(df: pl.DataFrame) -> dict:
    """Calculate overall funnel metrics from daily brand data."""
    if df.is_empty():
        return {
            "total_views": 0,
            "total_carts": 0,
            "total_purchases": 0,
            "total_revenue": 0.0,
        }
    metrics = df.select(
        pl.col("views").sum().alias("total_views"),
        pl.col("carts").sum().alias("total_carts"),
        pl.col("purchases").sum().alias("total_purchases"),
        pl.col("revenue").sum().alias("total_revenue"),
    ).to_dicts()[0]
    return metrics


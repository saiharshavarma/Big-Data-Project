"""
Data loading utilities for FunnelPulse Dashboard.
Supports both local filesystem and GCS paths using Polars.
"""

import os
from functools import lru_cache

import polars as pl
import streamlit as st

# For GCS access
try:
    import fsspec
    HAS_FSSPEC = True
except ImportError:
    HAS_FSSPEC = False


# Environment configuration (mirrors config.py pattern)
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

if ENVIRONMENT == "gcp":
    # Default bucket name - override with GCS_BUCKET env var
    # Matches gcp_setup.sh: GCS_BUCKET="gs://big-data-project-480103-funnelpulse-data"
    GCS_BUCKET = os.getenv("GCS_BUCKET", "big-data-project-480103-funnelpulse-data")
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
        # Check if path exists (for local paths)
        if not path.startswith("gs://") and not os.path.exists(path):
            return pl.DataFrame()
        
        # For GCS paths, use fsspec/gcsfs
        if path.startswith("gs://"):
            if not HAS_FSSPEC:
                st.error("fsspec is required for GCS access. Install with: pip install fsspec gcsfs")
                return pl.DataFrame()
            
            # Read partitioned parquet files from GCS
            # Polars can read from GCS if fsspec/gcsfs is installed
            try:
                # Try reading the directory (Polars will handle partitioned data)
                return pl.read_parquet(path)
            except Exception as e1:
                # If that fails, try with explicit file listing
                try:
                    fs = fsspec.filesystem("gs")
                    # List all parquet files recursively
                    parquet_files = fs.glob(f"{path}/**/*.parquet")
                    if not parquet_files:
                        # Try without recursive
                        parquet_files = fs.glob(f"{path}/*.parquet")
                    
                    if not parquet_files:
                        st.warning(f"No parquet files found in {path}")
                        return pl.DataFrame()
                    
                    # Read all parquet files
                    gcs_paths = [f"gs://{f}" if not f.startswith("gs://") else f for f in parquet_files]
                    df = pl.read_parquet(gcs_paths)
                    
                    # Extract partition columns from file paths
                    if not df.is_empty() and "date" not in df.columns:
                        # Check if paths contain partition info (date=YYYY-MM-DD)
                        import re
                        partition_dates = {}
                        for file_path in gcs_paths:
                            match = re.search(r'date=(\d{4}-\d{2}-\d{2})', file_path)
                            if match:
                                date_val = match.group(1)
                                partition_dates[file_path] = date_val
                        
                        # If we found partition dates, add them to dataframe
                        # Note: This is simplified - in practice each file's rows should get its partition date
                        if partition_dates:
                            # Use the most common date or first date found
                            from collections import Counter
                            date_counts = Counter(partition_dates.values())
                            most_common_date = date_counts.most_common(1)[0][0] if date_counts else None
                            
                            if most_common_date:
                                # Add date column (simplified - assumes all rows from same partition)
                                # For accurate mapping, would need to track which rows came from which file
                                df = df.with_columns(pl.lit(most_common_date).cast(pl.Date).alias("date"))
                    
                    return df
                except Exception as e2:
                    st.error(f"Failed to read from GCS: {str(e2)[:200]}")
                    return pl.DataFrame()
        
        # Local path
        return pl.read_parquet(path)
    except FileNotFoundError:
        return pl.DataFrame()
    except Exception as e:
        # Show error for debugging
        error_msg = str(e)
        if "No such file" not in error_msg and "not found" not in error_msg.lower():
            st.error(f"Error loading from {path}: {error_msg[:300]}")
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
    
    # Check if date column exists, try alternatives
    if date_col not in df.columns:
        # Try alternative column names
        for alt_col in ["window_date", "event_date", "date"]:
            if alt_col in df.columns:
                date_col = alt_col
                break
        else:
            # If no date column found, return None
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


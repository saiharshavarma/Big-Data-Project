"""
Analytical insights and root cause analysis for FunnelPulse Dashboard.
"""

import polars as pl


def calculate_funnel_drop_insights(df: pl.DataFrame) -> dict:
    """Calculate where users drop off in the funnel."""
    if df.is_empty():
        return {}
    
    total_views = df.select(pl.col("views").sum()).item()
    total_carts = df.select(pl.col("carts").sum()).item()
    total_purchases = df.select(pl.col("purchases").sum()).item()
    
    view_to_cart_rate = (total_carts / total_views * 100) if total_views > 0 else 0
    cart_to_purchase_rate = (total_purchases / total_carts * 100) if total_carts > 0 else 0
    overall_conversion = (total_purchases / total_views * 100) if total_views > 0 else 0
    
    # Identify biggest drop-off point
    drop_off_view_cart = total_views - total_carts
    drop_off_cart_purchase = total_carts - total_purchases
    
    biggest_drop = "view_to_cart" if drop_off_view_cart > drop_off_cart_purchase else "cart_to_purchase"
    
    return {
        "view_to_cart_rate": view_to_cart_rate,
        "cart_to_purchase_rate": cart_to_purchase_rate,
        "overall_conversion": overall_conversion,
        "biggest_drop_off": biggest_drop,
        "drop_off_view_cart": drop_off_view_cart,
        "drop_off_cart_purchase": drop_off_cart_purchase,
    }


def identify_underperforming_brands(df: pl.DataFrame, threshold_percentile: float = 25) -> pl.DataFrame:
    """Identify brands with conversion rates below threshold percentile."""
    if df.is_empty():
        return pl.DataFrame()
    
    brand_performance = (
        df.filter(pl.col("brand").is_not_null())
        .group_by("brand")
        .agg(
            pl.col("views").sum().alias("total_views"),
            pl.col("purchases").sum().alias("total_purchases"),
            pl.col("revenue").sum().alias("total_revenue"),
        )
        .with_columns(
            (pl.col("total_purchases") / pl.col("total_views") * 100).alias("conversion_rate")
        )
        .filter(pl.col("total_views") >= 1000)  # Minimum views threshold
    )
    
    if brand_performance.is_empty():
        return pl.DataFrame()
    
    threshold = brand_performance.select(pl.col("conversion_rate").quantile(threshold_percentile / 100)).item()
    
    underperformers = brand_performance.filter(pl.col("conversion_rate") < threshold).sort("conversion_rate")
    
    return underperformers


def calculate_revenue_impact(df: pl.DataFrame, target_conversion_rate: float = None) -> dict:
    """Calculate potential revenue impact if conversion rates improved."""
    if df.is_empty():
        return {}
    
    total_views = df.select(pl.col("views").sum()).item()
    total_purchases = df.select(pl.col("purchases").sum()).item()
    total_revenue = df.select(pl.col("revenue").sum()).item()
    current_conversion = (total_purchases / total_views * 100) if total_views > 0 else 0
    
    if target_conversion_rate is None:
        # Use industry average or 10% improvement
        target_conversion_rate = current_conversion * 1.1
    
    potential_purchases = (total_views * target_conversion_rate / 100)
    additional_purchases = potential_purchases - total_purchases
    
    # Estimate revenue per purchase
    avg_revenue_per_purchase = (total_revenue / total_purchases) if total_purchases > 0 else 0
    potential_revenue = additional_purchases * avg_revenue_per_purchase
    
    return {
        "current_conversion": current_conversion,
        "target_conversion": target_conversion_rate,
        "current_revenue": total_revenue,
        "potential_revenue": potential_revenue,
        "revenue_opportunity": potential_revenue,
        "additional_purchases": additional_purchases,
    }


def analyze_anomaly_patterns(anomalies_df: pl.DataFrame) -> dict:
    """Analyze patterns in anomalies."""
    if anomalies_df.is_empty():
        return {}
    
    total_anomalies = anomalies_df.height
    drops = anomalies_df.filter(pl.col("anomaly_type") == "drop").height
    spikes = anomalies_df.filter(pl.col("anomaly_type") == "spike").height
    
    # Most affected brands
    brand_anomaly_counts = (
        anomalies_df.group_by("brand")
        .agg(pl.count().alias("anomaly_count"))
        .sort("anomaly_count", descending=True)
        .head(5)
    )
    
    # Average severity
    avg_z_score = anomalies_df.select(pl.col("z_brand").abs().mean()).item()
    
    # Time patterns (if window_start exists)
    if "window_start" in anomalies_df.columns:
        # Extract hour of day
        hourly_patterns = (
            anomalies_df.with_columns(
                pl.col("window_start").dt.hour().alias("hour")
            )
            .group_by("hour")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
        )
        peak_hour = hourly_patterns.head(1).to_dicts()[0] if not hourly_patterns.is_empty() else None
    else:
        peak_hour = None
    
    return {
        "total_anomalies": total_anomalies,
        "drops": drops,
        "spikes": spikes,
        "drop_percentage": (drops / total_anomalies * 100) if total_anomalies > 0 else 0,
        "spike_percentage": (spikes / total_anomalies * 100) if total_anomalies > 0 else 0,
        "avg_severity": avg_z_score,
        "most_affected_brands": brand_anomaly_counts.to_dicts() if not brand_anomaly_counts.is_empty() else [],
        "peak_anomaly_hour": peak_hour,
    }


def calculate_category_performance_insights(category_df: pl.DataFrame) -> dict:
    """Analyze category performance and identify opportunities."""
    if category_df.is_empty():
        return {}
    
    category_perf = (
        category_df.group_by("category_root")
        .agg(
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("views").sum().alias("total_views"),
            pl.col("purchases").sum().alias("total_purchases"),
        )
        .with_columns(
            (pl.col("total_purchases") / pl.col("total_views") * 100).alias("conversion_rate")
        )
        .sort("total_revenue", descending=True)
    )
    
    if category_perf.is_empty():
        return {}
    
    top_category = category_perf.head(1).to_dicts()[0]
    best_conversion = category_perf.sort("conversion_rate", descending=True).head(1).to_dicts()[0]
    
    return {
        "top_revenue_category": top_category,
        "best_conversion_category": best_conversion,
        "category_count": category_perf.height,
    }


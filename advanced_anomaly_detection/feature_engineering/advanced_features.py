"""
Advanced Feature Engineering for Anomaly Detection
===================================================
Generates comprehensive features from e-commerce funnel data including:
- Multi-stage funnel metrics
- Time-based features
- Rolling statistics
- Lag features
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, dayofmonth, month,
    when, avg, stddev, min as _min, max as _max,
    lag, lead, coalesce, lit
)
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config.anomaly_config import FEATURE_CONFIG


class AdvancedFeatureEngineering:
    """Advanced feature engineering for funnel analytics."""
    
    def __init__(self, config=None):
        """
        Initialize feature engineering.
        
        Args:
            config (dict, optional): Feature configuration. Defaults to FEATURE_CONFIG.
        """
        self.config = config or FEATURE_CONFIG
    
    def add_time_features(self, df: DataFrame) -> DataFrame:
        """
        Add time-based features.
        
        Args:
            df: Input DataFrame with window_start column
            
        Returns:
            DataFrame with time features
        """
        if not self.config["time_features"].get("hour_of_day", True):
            return df
        
        # Extract time components
        df = df.withColumn("hour_of_day", hour(col("window_start")))
        
        if self.config["time_features"].get("day_of_week", True):
            df = df.withColumn("day_of_week", dayofweek(col("window_start")))
            # Add is_weekend flag
            if self.config["time_features"].get("is_weekend", True):
                df = df.withColumn(
                    "is_weekend",
                    when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
                )
        
        if self.config["time_features"].get("day_of_month", True):
            df = df.withColumn("day_of_month", dayofmonth(col("window_start")))
        
        if self.config["time_features"].get("month", True):
            df = df.withColumn("month", month(col("window_start")))
        
        # Business hours flag (9am-5pm)
        if self.config["time_features"].get("is_business_hours", True):
            df = df.withColumn(
                "is_business_hours",
                when(
                    (col("hour_of_day") >= 9) & (col("hour_of_day") < 17),
                    1
                ).otherwise(0)
            )
        
        return df
    
    def add_funnel_metrics(self, df: DataFrame) -> DataFrame:
        """
        Add multi-stage funnel metrics.
        
        Args:
            df: Input DataFrame with views, cart_additions, purchases
            
        Returns:
            DataFrame with funnel metrics
        """
        config = self.config["funnel_metrics"]
        
        # View to cart conversion rate
        if config.get("view_to_cart_rate", True):
            df = df.withColumn(
                "view_to_cart_rate",
                when(col("views") > 0, col("cart_additions") / col("views")).otherwise(0)
            )
        
        # Cart to purchase conversion rate
        if config.get("cart_to_purchase_rate", True):
            df = df.withColumn(
                "cart_to_purchase_rate",
                when(col("cart_additions") > 0, col("purchases") / col("cart_additions")).otherwise(0)
            )
        
        # Overall conversion rate (already exists but ensure it's there)
        if config.get("overall_conversion_rate", True):
            df = df.withColumn(
                "conversion_rate",
                when(col("views") > 0, col("purchases") / col("views")).otherwise(0)
            )
        
        # Revenue per view
        if config.get("revenue_per_view", True):
            df = df.withColumn(
                "revenue_per_view",
                when(col("views") > 0, col("revenue") / col("views")).otherwise(0)
            )
        
        # Average order value
        if config.get("avg_order_value", True):
            df = df.withColumn(
                "avg_order_value",
                when(col("purchases") > 0, col("revenue") / col("purchases")).otherwise(0)
            )
        
        return df
    
    def add_rolling_statistics(self, df: DataFrame) -> DataFrame:
        """
        Add rolling window statistics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with rolling statistics
        """
        rolling_config = self.config.get("rolling_windows", {})
        
        for window_name, window_hours in rolling_config.items():
            # Define window for each brand
            window_spec = Window.partitionBy("brand") \
                .orderBy(col("window_start").cast("long")) \
                .rangeBetween(-window_hours * 3600, 0)  # Convert hours to seconds
            
            # Rolling averages
            df = df.withColumn(
                f"rolling_{window_name}_conversion",
                avg("conversion_rate").over(window_spec)
            )
            
            df = df.withColumn(
                f"rolling_{window_name}_views",
                avg("views").over(window_spec)
            )
            
            df = df.withColumn(
                f"rolling_{window_name}_purchases",
                avg("purchases").over(window_spec)
            )
            
            df = df.withColumn(
                f"rolling_{window_name}_revenue",
                avg("revenue").over(window_spec)
            )
            
            # Rolling standard deviations
            df = df.withColumn(
                f"rolling_{window_name}_conversion_std",
                stddev("conversion_rate").over(window_spec)
            )
        
        return df
    
    def add_lag_features(self, df: DataFrame) -> DataFrame:
        """
        Add lag features for temporal dependencies.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with lag features
        """
        lag_config = self.config.get("lag_features", {})
        lags = lag_config.get("lags", [1, 24, 168])
        metrics = lag_config.get("metrics", ["conversion_rate", "views", "purchases"])
        
        # Window partitioned by brand, ordered by time
        window_spec = Window.partitionBy("brand").orderBy("window_start")
        
        for metric in metrics:
            for lag_period in lags:
                lag_name = f"{metric}_lag_{lag_period}h"
                df = df.withColumn(
                    lag_name,
                    lag(metric, lag_period).over(window_spec)
                )
                
                # Add rate of change vs lag
                if metric in ["conversion_rate"]:
                    df = df.withColumn(
                        f"{metric}_change_from_{lag_period}h",
                        when(
                            col(lag_name).isNotNull() & (col(lag_name) != 0),
                            (col(metric) - col(lag_name)) / col(lag_name)
                        ).otherwise(0)
                    )
        
        return df
    
    def add_statistical_features(self, df: DataFrame) -> DataFrame:
        """
        Add statistical features comparing current to historical.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with statistical features
        """
        # Window for brand statistics (all time)
        w_brand = Window.partitionBy("brand")
        
        # Brand-level statistics
        df = df.withColumn("brand_avg_conversion", avg("conversion_rate").over(w_brand))
        df = df.withColumn("brand_std_conversion", stddev("conversion_rate").over(w_brand))
        df = df.withColumn("brand_min_conversion", _min("conversion_rate").over(w_brand))
        df = df.withColumn("brand_max_conversion", _max("conversion_rate").over(w_brand))
        
        # Z-score (standardized deviation from brand mean)
        df = df.withColumn(
            "conversion_z_score",
            when(
                (col("brand_std_conversion").isNotNull()) & (col("brand_std_conversion") > 0),
                (col("conversion_rate") - col("brand_avg_conversion")) / col("brand_std_conversion")
            ).otherwise(0)
        )
        
        # Percentile rank within brand
        w_brand_rank = Window.partitionBy("brand").orderBy("conversion_rate")
        df = df.withColumn(
            "conversion_percentile",
            (col("conversion_z_score") + 3) / 6  # Approximate percentile from z-score
        )
        
        return df
    
    def add_interaction_features(self, df: DataFrame) -> DataFrame:
        """
        Add interaction features between different metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with interaction features
        """
        # View velocity (views per hour of day)
        df = df.withColumn(
            "view_velocity",
            col("views") * (24.0 / (col("hour_of_day") + 1))
        )
        
        # Weekend effect on conversion
        if "is_weekend" in df.columns:
            df = df.withColumn(
                "weekend_conversion_effect",
                col("is_weekend") * col("conversion_rate")
            )
        
        # Business hours conversion
        if "is_business_hours" in df.columns:
            df = df.withColumn(
                "business_hours_conversion_effect",
                col("is_business_hours") * col("conversion_rate")
            )
        
        return df
    
    def engineer_all_features(self, df: DataFrame) -> DataFrame:
        """
        Apply all feature engineering steps.
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            DataFrame with all engineered features
        """
        print("Starting feature engineering...")
        
        # Ensure required columns exist
        required_cols = ["window_start", "brand", "views", "purchases"]
        for col_name in required_cols:
            if col_name not in df.columns:
                raise ValueError(f"Required column '{col_name}' not found in DataFrame")
        
        # Add missing columns with defaults if needed
        if "cart_additions" not in df.columns:
            df = df.withColumn("cart_additions", lit(0))
        if "revenue" not in df.columns:
            df = df.withColumn("revenue", lit(0.0))
        
        # Apply feature engineering steps
        df = self.add_time_features(df)
        print("  ✓ Time features added")
        
        df = self.add_funnel_metrics(df)
        print("  ✓ Funnel metrics added")
        
        df = self.add_rolling_statistics(df)
        print("  ✓ Rolling statistics added")
        
        df = self.add_lag_features(df)
        print("  ✓ Lag features added")
        
        df = self.add_statistical_features(df)
        print("  ✓ Statistical features added")
        
        df = self.add_interaction_features(df)
        print("  ✓ Interaction features added")
        
        print("Feature engineering complete!")
        
        return df
    
    def get_feature_list(self) -> list:
        """
        Get list of all engineered feature names.
        
        Returns:
            list: Feature names
        """
        features = [
            # Time features
            "hour_of_day", "day_of_week", "day_of_month", "month",
            "is_weekend", "is_business_hours",
            
            # Funnel metrics
            "view_to_cart_rate", "cart_to_purchase_rate", "conversion_rate",
            "revenue_per_view", "avg_order_value",
            
            # Rolling statistics (example for 7d)
            "rolling_7d_conversion", "rolling_7d_views", "rolling_7d_purchases",
            "rolling_7d_revenue", "rolling_7d_conversion_std",
            
            # Statistical features
            "conversion_z_score", "conversion_percentile",
            
            # Interaction features
            "view_velocity", "weekend_conversion_effect", "business_hours_conversion_effect",
        ]
        
        return features


def create_feature_engineering_pipeline(config=None):
    """
    Factory function to create feature engineering pipeline.
    
    Args:
        config (dict, optional): Feature configuration
        
    Returns:
        AdvancedFeatureEngineering: Configured feature engineering instance
    """
    return AdvancedFeatureEngineering(config=config)

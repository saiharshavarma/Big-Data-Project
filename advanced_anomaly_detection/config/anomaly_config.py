"""
Configuration for Advanced Anomaly Detection System
====================================================
Central configuration for all anomaly detection models, features, and thresholds.
"""

# ============================================================================
# GENERAL SETTINGS
# ============================================================================

# Minimum data requirements
MIN_VIEWS_BASELINE = 20  # Minimum views for baseline calculation
MIN_VIEWS_ANOMALY = 50   # Minimum views to flag as anomaly
MIN_TRAINING_SAMPLES = 100  # Minimum samples required for model training

# GCS Configuration (will be overridden for GCP deployments)
GCS_BUCKET = "gs://funnelpulse-ss18851-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"
GOLD_HOURLY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_hourly_brand"
ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_hourly_brand"
ADVANCED_ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_advanced"


# ============================================================================
# FEATURE ENGINEERING CONFIG
# ============================================================================

FEATURE_CONFIG = {
    # Time-based features
    "time_features": {
        "hour_of_day": True,
        "day_of_week": True,
        "day_of_month": True,
        "month": True,
        "is_weekend": True,
        "is_business_hours": True,  # 9am-5pm
    },
    
    # Rolling window statistics
    "rolling_windows": {
        "7d": 7 * 24,   # 7 days in hours
        "30d": 30 * 24,  # 30 days in hours
    },
    
    # Funnel metrics
    "funnel_metrics": {
        "view_to_cart_rate": True,
        "cart_to_purchase_rate": True,
        "overall_conversion_rate": True,
        "revenue_per_view": True,
        "avg_order_value": True,
    },
    
    # Lag features
    "lag_features": {
        "lags": [1, 24, 168],  # 1 hour, 1 day, 1 week ago
        "metrics": ["conversion_rate", "views", "purchases"],
    },
}


# ============================================================================
# LSTM AUTOENCODER CONFIG
# ============================================================================

LSTM_CONFIG = {
    # Model architecture
    "sequence_length": 168,  # 1 week of hourly data
    "encoding_dim": 32,
    "lstm_units": [64, 32],  # Two LSTM layers
    "dropout_rate": 0.2,
    
    # Training parameters
    "epochs": 50,
    "batch_size": 32,
    "validation_split": 0.2,
    "learning_rate": 0.001,
    "early_stopping_patience": 10,
    
    # Anomaly detection
    "reconstruction_threshold_percentile": 95,  # 95th percentile of reconstruction error
    "anomaly_threshold_multiplier": 1.5,  # Multiply threshold by this factor
}


# ============================================================================
# PROPHET CONFIG
# ============================================================================

PROPHET_CONFIG = {
    # Model parameters
    "changepoint_prior_scale": 0.05,  # Flexibility of trend changes
    "seasonality_prior_scale": 10.0,  # Strength of seasonality
    "seasonality_mode": "multiplicative",  # or "additive"
    
    # Seasonality components
    "yearly_seasonality": False,  # Not enough data for yearly patterns
    "weekly_seasonality": True,
    "daily_seasonality": True,
    
    # Holidays and special events (can be extended)
    "holidays": None,  # Can add custom holidays dataframe
    
    # Anomaly detection
    "interval_width": 0.95,  # 95% confidence interval
    "uncertainty_samples": 1000,
    "anomaly_threshold": 0.05,  # Outside 95% interval = anomaly
}


# ============================================================================
# ISOLATION FOREST CONFIG
# ============================================================================

ISOLATION_FOREST_CONFIG = {
    # Model parameters
    "n_estimators": 100,
    "max_samples": "auto",
    "contamination": 0.1,  # Expected proportion of outliers (10%)
    "max_features": 1.0,
    "bootstrap": False,
    "random_state": 42,
    
    # Feature selection
    "features": [
        "conversion_rate",
        "views",
        "purchases",
        "revenue",
        "hour_of_day",
        "day_of_week",
        "rolling_7d_conversion",
        "rolling_30d_conversion",
    ],
}


# ============================================================================
# CHANGEPOINT DETECTION CONFIG
# ============================================================================

CHANGEPOINT_CONFIG = {
    # Detection algorithm
    "method": "pelt",  # PELT (Pruned Exact Linear Time) algorithm
    "model": "rbf",  # Kernel-based cost function
    "min_size": 24,  # Minimum segment size (24 hours)
    "jump": 1,  # Subsample (1 = no subsampling)
    "penalty": 10,  # Penalty for adding changepoints (higher = fewer changepoints)
    
    # Anomaly thresholds
    "min_changepoint_magnitude": 0.15,  # Minimum 15% change in conversion rate
    "significance_level": 0.05,
}


# ============================================================================
# ENSEMBLE CONFIG
# ============================================================================

ENSEMBLE_CONFIG = {
    # Model weights (must sum to 1.0)
    "model_weights": {
        "lstm_autoencoder": 0.30,
        "prophet": 0.30,
        "isolation_forest": 0.25,
        "changepoint": 0.15,
    },
    
    # Voting strategy
    "voting_strategy": "weighted",  # "weighted" or "majority"
    "min_model_agreement": 2,  # Minimum models that must agree for anomaly
    
    # Dynamic thresholds
    "adaptive_thresholds": True,
    "threshold_lookback_hours": 168,  # 1 week for threshold calculation
    
    # Anomaly severity levels
    "severity_thresholds": {
        "low": 0.5,
        "medium": 0.7,
        "high": 0.85,
        "critical": 0.95,
    },
}


# ============================================================================
# STREAMING CONFIG
# ============================================================================

STREAMING_CONFIG = {
    # Spark Streaming
    "trigger_interval": "5 minutes",
    "checkpoint_location": f"{TABLES_DIR}/checkpoints/advanced_anomaly",
    "watermark_delay": "10 minutes",
    
    # Kafka integration (if applicable)
    "kafka_bootstrap_servers": "localhost:9092",
    "kafka_topic_input": "funnelpulse_events",
    "kafka_topic_output": "funnelpulse_anomalies",
    
    # Window settings
    "sliding_window_size": "1 hour",
    "sliding_window_slide": "5 minutes",
    
    # Online learning
    "model_update_frequency": "daily",  # How often to retrain models
    "model_cache_size": 100,  # Cache predictions for efficiency
}


# ============================================================================
# ANALYTICS CONFIG
# ============================================================================

ANALYTICS_CONFIG = {
    # Root cause analysis
    "rca_features": [
        "views_change",
        "cart_additions_change",
        "purchase_rate_change",
        "avg_order_value_change",
        "hour_of_day",
        "day_of_week",
    ],
    
    # Impact assessment
    "impact_metrics": {
        "revenue_loss": True,
        "conversion_loss": True,
        "customer_loss": True,
    },
    
    # Reporting thresholds
    "min_impact_threshold": 100,  # Minimum $100 revenue impact to report
    "alert_severity_threshold": "medium",  # Minimum severity to trigger alert
}


# ============================================================================
# DEPLOYMENT CONFIG
# ============================================================================

DEPLOYMENT_CONFIG = {
    # Model persistence
    "model_save_path": f"{TABLES_DIR}/models/advanced_anomaly",
    "model_version": "1.0.0",
    
    # Monitoring
    "enable_monitoring": True,
    "log_level": "INFO",
    "metrics_collection": True,
    
    # Performance
    "parallel_model_execution": True,
    "max_workers": 4,
    
    # Error handling
    "retry_attempts": 3,
    "fallback_to_zscore": True,  # Fall back to simple Z-score if models fail
}


# ============================================================================
# OUTPUT SCHEMA CONFIG
# ============================================================================

OUTPUT_SCHEMA = {
    # Enhanced anomaly output fields
    "fields": [
        "window_start",
        "window_date",
        "brand",
        "views",
        "purchases",
        "revenue",
        "conversion_rate",
        
        # Model scores
        "lstm_anomaly_score",
        "prophet_anomaly_score",
        "isolation_forest_anomaly_score",
        "changepoint_anomaly_score",
        
        # Ensemble results
        "ensemble_anomaly_score",
        "anomaly_confidence",
        "anomaly_severity",
        "is_anomaly",
        "anomaly_type",  # "drop", "spike", "changepoint"
        
        # Context
        "expected_value",
        "deviation_percentage",
        "contributing_factors",
        
        # Impact assessment
        "estimated_revenue_impact",
        "estimated_conversion_loss",
    ],
}


def get_config(section=None):
    """
    Get configuration for a specific section or all configurations.
    
    Args:
        section (str, optional): Section name (e.g., "LSTM_CONFIG")
    
    Returns:
        dict: Configuration dictionary
    """
    if section:
        return globals().get(section, {})
    
    return {
        "feature": FEATURE_CONFIG,
        "lstm": LSTM_CONFIG,
        "prophet": PROPHET_CONFIG,
        "isolation_forest": ISOLATION_FOREST_CONFIG,
        "changepoint": CHANGEPOINT_CONFIG,
        "ensemble": ENSEMBLE_CONFIG,
        "streaming": STREAMING_CONFIG,
        "analytics": ANALYTICS_CONFIG,
        "deployment": DEPLOYMENT_CONFIG,
        "output_schema": OUTPUT_SCHEMA,
    }


def validate_config():
    """Validate configuration settings."""
    errors = []
    
    # Validate ensemble weights sum to 1.0
    weights = ENSEMBLE_CONFIG["model_weights"]
    weight_sum = sum(weights.values())
    if abs(weight_sum - 1.0) > 0.01:
        errors.append(f"Ensemble weights sum to {weight_sum}, should be 1.0")
    
    # Validate minimum samples
    if MIN_TRAINING_SAMPLES < 50:
        errors.append("MIN_TRAINING_SAMPLES should be at least 50")
    
    # Validate LSTM sequence length
    if LSTM_CONFIG["sequence_length"] < 24:
        errors.append("LSTM sequence_length should be at least 24 hours")
    
    if errors:
        raise ValueError(f"Configuration validation errors: {'; '.join(errors)}")
    
    return True


# Validate on import
validate_config()

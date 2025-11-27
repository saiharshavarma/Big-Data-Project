"""
FunnelPulse Configuration
=========================
Centralized configuration for paths, Spark settings, and Kafka parameters.
Switch between local and cloud environments by changing ENVIRONMENT.
"""

import os
from pathlib import Path

# =============================================================================
# JAVA CONFIGURATION (required for PySpark 3.4+)
# =============================================================================
# Set JAVA_HOME to Java 17 before any Spark imports
JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["JAVA_HOME"] = JAVA_HOME

# =============================================================================
# ENVIRONMENT SETTING
# =============================================================================
# Options: "local", "jupyterhub", "gcp"
ENVIRONMENT = "local"

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

if ENVIRONMENT == "local":
    # Local development paths (current directory structure)
    PROJECT_ROOT = Path(__file__).parent.resolve()
elif ENVIRONMENT == "jupyterhub":
    # Course JupyterHub environment
    PROJECT_ROOT = Path(os.path.expanduser("~")) / "funnelpulse"
elif ENVIRONMENT == "gcp":
    # Google Cloud Platform paths
    GCS_BUCKET = "funnelpulse-data-479512"
    GCS_PROJECT = "funnelpulse-479512"
    GCS_REGION = "us-central1"
    # Note: Using string paths for GCS (Path doesn't support gs://)
    PROJECT_ROOT = f"gs://{GCS_BUCKET}"
else:
    raise ValueError(f"Unknown environment: {ENVIRONMENT}")

# Directory paths (use strings for GCP, Path for local)
if ENVIRONMENT == "gcp":
    DATA_RAW_DIR = f"{PROJECT_ROOT}/data_raw"
    TABLES_DIR = f"{PROJECT_ROOT}/tables"
    STREAM_INPUT_DIR = f"{PROJECT_ROOT}/stream_input"
    CHECKPOINTS_DIR = f"{PROJECT_ROOT}/checkpoints"

    # Table paths - Bronze, Silver, Gold layers
    BRONZE_PATH = f"{TABLES_DIR}/bronze_events"
    SILVER_PATH = f"{TABLES_DIR}/silver_events"

    # Gold tables (batch)
    GOLD_FUNNEL_HOURLY_BRAND = f"{TABLES_DIR}/gold_funnel_hourly_brand"
    GOLD_FUNNEL_DAILY_BRAND = f"{TABLES_DIR}/gold_funnel_daily_brand"
    GOLD_FUNNEL_DAILY_CATEGORY = f"{TABLES_DIR}/gold_funnel_daily_category"
    GOLD_FUNNEL_HOURLY_PRICE_BAND = f"{TABLES_DIR}/gold_funnel_hourly_price_band"

    # Gold tables (streaming)
    GOLD_STREAM_FUNNEL_HOURLY_BRAND = f"{TABLES_DIR}/gold_stream_funnel_hourly_brand"

    # Anomaly detection output
    GOLD_ANOMALIES_HOURLY_BRAND = f"{TABLES_DIR}/gold_anomalies_hourly_brand"

    # Streaming checkpoint
    CHECKPOINT_STREAM_HOURLY_BRAND = f"{CHECKPOINTS_DIR}/stream_hourly_brand"
else:
    DATA_RAW_DIR = PROJECT_ROOT / "data_raw"
    TABLES_DIR = PROJECT_ROOT / "tables"
    STREAM_INPUT_DIR = PROJECT_ROOT / "stream_input"
    CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"

    # Table paths - Bronze, Silver, Gold layers
    BRONZE_PATH = TABLES_DIR / "bronze_events"
    SILVER_PATH = TABLES_DIR / "silver_events"

    # Gold tables (batch)
    GOLD_FUNNEL_HOURLY_BRAND = TABLES_DIR / "gold_funnel_hourly_brand"
    GOLD_FUNNEL_DAILY_BRAND = TABLES_DIR / "gold_funnel_daily_brand"
    GOLD_FUNNEL_DAILY_CATEGORY = TABLES_DIR / "gold_funnel_daily_category"
    GOLD_FUNNEL_HOURLY_PRICE_BAND = TABLES_DIR / "gold_funnel_hourly_price_band"

    # Gold tables (streaming)
    GOLD_STREAM_FUNNEL_HOURLY_BRAND = TABLES_DIR / "gold_stream_funnel_hourly_brand"

    # Anomaly detection output
    GOLD_ANOMALIES_HOURLY_BRAND = TABLES_DIR / "gold_anomalies_hourly_brand"

    # Streaming checkpoint
    CHECKPOINT_STREAM_HOURLY_BRAND = CHECKPOINTS_DIR / "stream_hourly_brand"

# =============================================================================
# SPARK CONFIGURATION
# =============================================================================

SPARK_CONFIG = {
    "app_name": "FunnelPulse",
    "master": "local[*]",  # Use all available cores locally
    "driver_memory": "4g",  # Adjust based on your machine
    "executor_memory": "4g",
    "sql_shuffle_partitions": 200,
}

# For JupyterHub environment (uncomment if needed)
# SPARK_CONFIG["ui_proxy_base"] = f"/user/{os.environ.get('JUPYTERHUB_USER', '')}/proxy/4040"

# =============================================================================
# KAFKA CONFIGURATION (for production streaming)
# =============================================================================

KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",  # Update for production
    "topic": "funnelpulse_events",
    "consumer_group": "funnelpulse_consumer",
}

# Replay producer settings
KAFKA_REPLAY_CONFIG = {
    "target_events_per_sec": 1000,
    "sleep_every_n": 5000,
    "sleep_seconds": 0.5,
}

# =============================================================================
# DATA SCHEMA
# =============================================================================

# Raw CSV files from Kaggle
RAW_CSV_FILES = [
    "2019-Oct.csv",
    "2019-Nov.csv",
]

# Bronze schema columns
BRONZE_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
]

# Event types in the dataset
EVENT_TYPES = ["view", "cart", "purchase", "remove_from_cart"]

# =============================================================================
# ANOMALY DETECTION SETTINGS
# =============================================================================

ANOMALY_CONFIG = {
    "min_views_threshold": 10,  # Minimum views to consider for anomaly detection
    "z_score_threshold": 2.0,   # Z-score threshold for flagging anomalies
}

# =============================================================================
# VISUALIZATION SETTINGS
# =============================================================================

VIZ_CONFIG = {
    "top_n_brands": 10,
    "figure_size_wide": (12, 6),
    "figure_size_standard": (10, 5),
    "figure_size_small": (8, 4),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_spark_session():
    """Create and return a configured Spark session."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(SPARK_CONFIG["app_name"])
        .master(SPARK_CONFIG["master"])
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG["sql_shuffle_partitions"])
    )

    # Add proxy config for JupyterHub if available
    if "ui_proxy_base" in SPARK_CONFIG:
        builder = builder.config("spark.ui.proxyBase", SPARK_CONFIG["ui_proxy_base"])

    return builder.getOrCreate()


def ensure_directories():
    """Create necessary directories if they don't exist (local only)."""
    if ENVIRONMENT == "local":
        for dir_path in [DATA_RAW_DIR, TABLES_DIR, STREAM_INPUT_DIR, CHECKPOINTS_DIR]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"Directories ensured at: {PROJECT_ROOT}")


def get_path_str(path):
    """Convert Path to string for Spark compatibility."""
    return str(path)


# =============================================================================
# PRINT CONFIGURATION (for debugging)
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("FunnelPulse Configuration")
    print("=" * 60)
    print(f"Environment: {ENVIRONMENT}")
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Data Raw Dir: {DATA_RAW_DIR}")
    print(f"Tables Dir: {TABLES_DIR}")
    print(f"Bronze Path: {BRONZE_PATH}")
    print(f"Silver Path: {SILVER_PATH}")
    print("=" * 60)

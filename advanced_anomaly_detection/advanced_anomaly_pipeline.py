"""
Advanced Anomaly Detection Pipeline - Main Orchestrator
========================================================
Coordinates all components of the advanced anomaly detection system.
Supports both batch and streaming modes.
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from typing import Optional, Dict, List
import warnings
warnings.filterwarnings('ignore')

# Import all modules
from feature_engineering.advanced_features import create_feature_engineering_pipeline
from advanced_models.lstm_autoencoder import create_lstm_detector, TENSORFLOW_AVAILABLE
from advanced_models.prophet_detector import create_prophet_detector, PROPHET_AVAILABLE
from advanced_models.isolation_forest_detector import create_isolation_forest_detector, SKLEARN_AVAILABLE
from advanced_models.changepoint_detector import create_changepoint_detector, RUPTURES_AVAILABLE
from ensemble.anomaly_ensemble import create_ensemble
from analytics.anomaly_analytics import create_analytics
from config.anomaly_config import (
    MIN_TRAINING_SAMPLES, MIN_VIEWS_ANOMALY,
    FEATURE_CONFIG, ISOLATION_FOREST_CONFIG,
    get_config
)


class AdvancedAnomalyPipeline:
    """
    Main pipeline orchestrating advanced anomaly detection.
    Coordinates feature engineering, model training, ensemble, and analytics.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """
        Initialize pipeline.
        
        Args:
            spark: SparkSession instance
            config: Pipeline configuration (uses defaults if None)
        """
        self.spark = spark
        self.config = config or get_config()
        
        # Initialize components
        self.feature_engineer = create_feature_engineering_pipeline(FEATURE_CONFIG)
        self.ensemble = create_ensemble()
        self.analytics = create_analytics()
        
        # Initialize models (conditionally based on library availability)
        self.models = {}
        self._initialize_models()
        
        print("Advanced Anomaly Detection Pipeline initialized")
        self._print_available_models()
    
    def _initialize_models(self):
        """Initialize all available anomaly detection models."""
        if SKLEARN_AVAILABLE:
            self.models['isolation_forest'] = create_isolation_forest_detector()
        
        if PROPHET_AVAILABLE:
            self.models['prophet'] = create_prophet_detector()
        
        if TENSORFLOW_AVAILABLE:
            self.models['lstm'] = create_lstm_detector()
        
        if RUPTURES_AVAILABLE:
            self.models['changepoint'] = create_changepoint_detector()
    
    def _print_available_models(self):
        """Print which models are available."""
        print("\nAvailable models:")
        for model_name in self.models.keys():
            print(f"  ✓ {model_name}")
        
        unavailable = []
        if not SKLEARN_AVAILABLE:
            unavailable.append("isolation_forest (requires scikit-learn)")
        if not PROPHET_AVAILABLE:
            unavailable.append("prophet (requires prophet)")
        if not TENSORFLOW_AVAILABLE:
            unavailable.append("lstm (requires tensorflow)")
        if not RUPTURES_AVAILABLE:
            unavailable.append("changepoint (requires ruptures)")
        
        if unavailable:
            print("\nUnavailable models:")
            for model_name in unavailable:
                print(f"  ✗ {model_name}")
    
    def load_data(self, input_path: str) -> DataFrame:
        """
        Load data from GCS or local path.
        
        Args:
            input_path: Path to input data
            
        Returns:
            Spark DataFrame
        """
        print(f"\nLoading data from: {input_path}")
        df = self.spark.read.parquet(input_path)
        count = df.count()
        print(f"Loaded {count:,} records")
        
        return df
    
    def prepare_features(self, df: DataFrame) -> DataFrame:
        """
        Apply feature engineering to input data.
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            DataFrame with engineered features
        """
        print("\n" + "=" * 60)
        print("Feature Engineering")
        print("=" * 60)
        
        df_features = self.feature_engineer.engineer_all_features(df)
        
        return df_features
    
    def train_models(self, df: DataFrame, train_split: float = 0.8) -> Dict:
        """
        Train all available models.
        
        Args:
            df: Feature-engineered DataFrame
            train_split: Fraction of data to use for training
            
        Returns:
            Training statistics
        """
        print("\n" + "=" * 60)
        print("Model Training")
        print("=" * 60)
        
        # Convert to Pandas for model training
        # (In production, you'd sample or use distributed training)
        print("Converting to Pandas for model training...")
        pdf = df.toPandas()
        
        # Sort by time
        pdf = pdf.sort_values('window_start').reset_index(drop=True)
        
        # Split train/test
        train_size = int(len(pdf) * train_split)
        train_data = pdf.iloc[:train_size]
        
        print(f"Training on {len(train_data):,} samples")
        
        stats = {}
        
        # Train Isolation Forest
        if 'isolation_forest' in self.models:
            print("\n[1/4] Training Isolation Forest...")
            try:
                feature_cols = ISOLATION_FOREST_CONFIG.get("features", [])
                available_features = [col for col in feature_cols if col in train_data.columns]
                
                if available_features:
                    stats['isolation_forest'] = self.models['isolation_forest'].train(
                        train_data, feature_cols=available_features
                    )
                else:
                    print("  Warning: No features available for Isolation Forest")
            except Exception as e:
                print(f"  Error training Isolation Forest: {e}")
                stats['isolation_forest'] = {'error': str(e)}
        
        # Train Prophet
        if 'prophet' in self.models:
            print("\n[2/4] Training Prophet...")
            try:
                stats['prophet'] = self.models['prophet'].train(train_data)
            except Exception as e:
                print(f"  Error training Prophet: {e}")
                stats['prophet'] = {'error': str(e)}
        
        # Train LSTM (requires more samples)
        if 'lstm' in self.models:
            print("\n[3/4] Training LSTM Autoencoder...")
            try:
                if len(train_data) >= MIN_TRAINING_SAMPLES * 2:
                    feature_cols = ['conversion_rate', 'views', 'purchases']
                    available_features = [col for col in feature_cols if col in train_data.columns]
                    
                    if available_features:
                        stats['lstm'] = self.models['lstm'].train(
                            train_data, feature_cols=available_features
                        )
                else:
                    print(f"  Skipping: Need at least {MIN_TRAINING_SAMPLES * 2} samples")
                    stats['lstm'] = {'skipped': 'insufficient_data'}
            except Exception as e:
                print(f"  Error training LSTM: {e}")
                stats['lstm'] = {'error': str(e)}
        
        # Detect changepoints
        if 'changepoint' in self.models:
            print("\n[4/4] Detecting Changepoints...")
            try:
                stats['changepoint'] = self.models['changepoint'].train(train_data)
            except Exception as e:
                print(f"  Error detecting changepoints: {e}")
                stats['changepoint'] = {'error': str(e)}
        
        print("\nModel training complete!")
        
        return stats
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies using all trained models.
        
        Args:
            df: Feature-engineered DataFrame
            
        Returns:
            DataFrame with anomaly predictions
        """
        print("\n" + "=" * 60)
        print("Anomaly Detection")
        print("=" * 60)
        
        # Convert to Pandas for model inference
        pdf = df.toPandas()
        
        # Run each model
        results = pdf.copy()
        
        # Isolation Forest
        if 'isolation_forest' in self.models:
            print("\n[1/4] Running Isolation Forest...")
            try:
                feature_cols = ISOLATION_FOREST_CONFIG.get("features", [])
                available_features = [col for col in feature_cols if col in results.columns]
                
                if available_features:
                    results = self.models['isolation_forest'].detect_anomalies(
                        results, feature_cols=available_features
                    )
            except Exception as e:
                print(f"  Error: {e}")
        
        # Prophet
        if 'prophet' in self.models:
            print("\n[2/4] Running Prophet...")
            try:
                results = self.models['prophet'].detect_anomalies(results)
            except Exception as e:
                print(f"  Error: {e}")
        
        # LSTM
        if 'lstm' in self.models:
            print("\n[3/4] Running LSTM Autoencoder...")
            try:
                feature_cols = ['conversion_rate', 'views', 'purchases']
                available_features = [col for col in feature_cols if col in results.columns]
                
                if available_features:
                    results = self.models['lstm'].detect_anomalies(
                        results, feature_cols=available_features
                    )
            except Exception as e:
                print(f"  Error: {e}")
        
        # Changepoint
        if 'changepoint' in self.models:
            print("\n[4/4] Running Changepoint Detection...")
            try:
                results = self.models['changepoint'].detect_anomalies(results)
            except Exception as e:
                print(f"  Error: {e}")
        
        print("\nAll models executed!")
        
        return results
    
    def apply_ensemble(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine model predictions using ensemble.
        
        Args:
            df: DataFrame with individual model predictions
            
        Returns:
            DataFrame with ensemble predictions
        """
        print("\n" + "=" * 60)
        print("Ensemble Combination")
        print("=" * 60)
        
        # Combine predictions
        df_ensemble = self.ensemble.combine_predictions(df)
        
        # Calculate impact metrics
        df_ensemble = self.ensemble.calculate_impact_metrics(df_ensemble)
        
        # Get summary
        summary = self.ensemble.get_summary_statistics(df_ensemble)
        
        print("\nEnsemble Summary:")
        print(f"  Total records: {summary['total_records']:,}")
        print(f"  Total anomalies: {summary['total_anomalies']:,}")
        print(f"  Anomaly rate: {summary['anomaly_rate']:.2%}")
        print(f"  By severity: {summary['by_severity']}")
        
        return df_ensemble
    
    def run_analytics(self, df: pd.DataFrame) -> Dict:
        """
        Run analytics on detected anomalies.
        
        Args:
            df: DataFrame with detected anomalies
            
        Returns:
            Analytics results
        """
        print("\n" + "=" * 60)
        print("Analytics & Reporting")
        print("=" * 60)
        
        # Get anomalies only
        anomalies = df[df['is_anomaly']].copy()
        
        if len(anomalies) == 0:
            print("No anomalies detected!")
            return {}
        
        # Impact assessment
        impact = self.analytics.assess_impact(anomalies)
        
        print("\nImpact Assessment:")
        print(f"  Total anomalies: {impact['total_anomalies']:,}")
        if 'total_revenue_impact' in impact:
            print(f"  Total revenue impact: ${impact['total_revenue_impact']:,.2f}")
        if 'total_conversion_loss' in impact:
            print(f"  Total conversion loss: {impact['total_conversion_loss']:,.0f} purchases")
        
        return impact
    
    def save_results(self, df: pd.DataFrame, output_path: str):
        """
        Save anomaly detection results.
        
        Args:
            df: Results DataFrame
            output_path: Output path
        """
        print("\n" + "=" * 60)
        print("Saving Results")
        print("=" * 60)
        
        # Convert back to Spark DataFrame
        spark_df = self.spark.createDataFrame(df)
        
        # Add partition column
        spark_df = spark_df.withColumn("window_date", to_date(col("window_start")))
        
        # Filter to anomalies only
        anomalies_df = spark_df.filter(col("is_anomaly"))
        
        anomaly_count = anomalies_df.count()
        print(f"Saving {anomaly_count:,} anomalies to: {output_path}")
        
        # Write
        anomalies_df.write \
            .mode("overwrite") \
            .partitionBy("window_date") \
            .parquet(output_path)
        
        print("Results saved successfully!")
    
    def run_pipeline(self, input_path: str, output_path: str, 
                    train_first: bool = True) -> Dict:
        """
        Run the complete anomaly detection pipeline.
        
        Args:
            input_path: Path to input data
            output_path: Path to save results
            train_first: Whether to train models first
            
        Returns:
            Pipeline execution statistics
        """
        print("\n" + "=" * 60)
        print("ADVANCED ANOMALY DETECTION PIPELINE")
        print("=" * 60)
        
        stats = {}
        
        # Load data
        df = self.load_data(input_path)
        
        # Feature engineering
        df_features = self.prepare_features(df)
        
        # Train models (if requested)
        if train_first:
            stats['training'] = self.train_models(df_features)
        
        # Detect anomalies
        df_anomalies = self.detect_anomalies(df_features)
        
        # Apply ensemble
        df_ensemble = self.apply_ensemble(df_anomalies)
        
        # Analytics
        stats['analytics'] = self.run_analytics(df_ensemble)
        
        # Save results
        self.save_results(df_ensemble, output_path)
        
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE!")
        print("=" * 60)
        
        return stats


def create_pipeline(spark: SparkSession, config: Optional[Dict] = None) -> AdvancedAnomalyPipeline:
    """
    Factory function to create pipeline.
    
    Args:
        spark: SparkSession instance
        config: Pipeline configuration
        
    Returns:
        AdvancedAnomalyPipeline instance
    """
    return AdvancedAnomalyPipeline(spark, config=config)

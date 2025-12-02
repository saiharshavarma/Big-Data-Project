"""
Ensemble Anomaly Detection Framework
=====================================
Combines predictions from multiple anomaly detection models using
weighted voting to reduce false positives and improve accuracy.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional

import sys
sys.path.append('..')
from config.anomaly_config import ENSEMBLE_CONFIG


class AnomalyEnsemble:
    """
    Ensemble framework combining multiple anomaly detection models.
    Uses weighted voting and consensus to reduce false positives.
    """
    
    def __init__(self, config=None):
        """
        Initialize ensemble framework.
        
        Args:
            config (dict, optional): Ensemble configuration. Defaults to ENSEMBLE_CONFIG.
        """
        self.config = config or ENSEMBLE_CONFIG
        self.model_weights = self.config["model_weights"]
        self.voting_strategy = self.config["voting_strategy"]
        self.min_model_agreement = self.config["min_model_agreement"]
        self.severity_thresholds = self.config["severity_thresholds"]
        
        # Validate weights
        self._validate_weights()
    
    def _validate_weights(self):
        """Validate that weights sum to 1.0."""
        weight_sum = sum(self.model_weights.values())
        if abs(weight_sum - 1.0) > 0.01:
            raise ValueError(f"Model weights must sum to 1.0, got {weight_sum}")
    
    def combine_predictions(self, predictions: pd.DataFrame) -> pd.DataFrame:
        """
        Combine predictions from multiple models using weighted voting.
        
        Args:
            predictions: DataFrame with scores from individual models
                Expected columns: lstm_anomaly_score, prophet_anomaly_score, etc.
            
        Returns:
            DataFrame with ensemble predictions
        """
        result = predictions.copy()
        
        # Map model names to column names
        model_score_columns = {
            'lstm_autoencoder': 'lstm_anomaly_score',
            'prophet': 'prophet_anomaly_score',
            'isolation_forest': 'isolation_forest_anomaly_score',
            'changepoint': 'changepoint_anomaly_score',
        }
        
        # Initialize ensemble score
        result['ensemble_anomaly_score'] = 0.0
        
        # Weighted voting
        available_models = []
        for model_name, weight in self.model_weights.items():
            col_name = model_score_columns.get(model_name)
            
            if col_name and col_name in result.columns:
                # Add weighted score
                result['ensemble_anomaly_score'] += weight * result[col_name].fillna(0)
                available_models.append(model_name)
        
        # Normalize if not all models are available
        if available_models:
            total_weight = sum(self.model_weights[m] for m in available_models)
            if total_weight > 0 and abs(total_weight - 1.0) > 0.01:
                result['ensemble_anomaly_score'] /= total_weight
        
        # Count how many models flagged as anomaly
        result['n_models_flagged'] = 0
        for model_name in available_models:
            col_name = model_score_columns[model_name]
            is_anomaly_col = col_name.replace('_score', '_is_anomaly')
            
            if is_anomaly_col in result.columns:
                result['n_models_flagged'] += result[is_anomaly_col].astype(int)
        
        # Calculate confidence based on model agreement
        result['anomaly_confidence'] = result['n_models_flagged'] / len(available_models)
        
        # Determine if point is anomaly based on voting strategy
        if self.voting_strategy == "weighted":
            # Use weighted score threshold
            result['is_anomaly'] = result['ensemble_anomaly_score'] > 0.5
        elif self.voting_strategy == "majority":
            # Require minimum number of models to agree
            result['is_anomaly'] = result['n_models_flagged'] >= self.min_model_agreement
        else:
            # Default to weighted
            result['is_anomaly'] = result['ensemble_anomaly_score'] > 0.5
        
        # Assign severity levels
        result['anomaly_severity'] = self._assign_severity(result['ensemble_anomaly_score'])
        
        # Determine anomaly type (drop vs spike vs changepoint)
        result['anomaly_type'] = self._determine_anomaly_type(result)
        
        return result
    
    def _assign_severity(self, scores: pd.Series) -> pd.Series:
        """
        Assign severity levels based on anomaly scores.
        
        Args:
            scores: Anomaly scores
            
        Returns:
            Series with severity labels
        """
        severity = pd.Series('normal', index=scores.index)
        
        thresholds = self.severity_thresholds
        severity[scores >= thresholds['low']] = 'low'
        severity[scores >= thresholds['medium']] = 'medium'
        severity[scores >= thresholds['high']] = 'high'
        severity[scores >= thresholds['critical']] = 'critical'
        
        return severity
    
    def _determine_anomaly_type(self, df: pd.DataFrame) -> pd.Series:
        """
        Determine anomaly type based on individual model predictions.
        
        Args:
            df: DataFrame with model predictions
            
        Returns:
            Series with anomaly types
        """
        anomaly_type = pd.Series('none', index=df.index)
        
        # Check changepoints first
        if 'is_at_changepoint' in df.columns:
            anomaly_type[df['is_at_changepoint']] = 'changepoint'
        
        # Check for drops (below expected)
        is_drop = False
        if 'prophet_expected_value' in df.columns and 'conversion_rate' in df.columns:
            is_drop = df['conversion_rate'] < df['prophet_expected_value'] * 0.8
            anomaly_type[is_drop & df['is_anomaly']] = 'drop'
        
        # Check for spikes (above expected)
        is_spike = False
        if 'prophet_expected_value' in df.columns and 'conversion_rate' in df.columns:
            is_spike = df['conversion_rate'] > df['prophet_expected_value'] * 1.2
            anomaly_type[is_spike & df['is_anomaly']] = 'spike'
        
        # If still none but is anomaly, use generic 'anomaly' label
        anomaly_type[(anomaly_type == 'none') & df['is_anomaly']] = 'anomaly'
        
        return anomaly_type
    
    def calculate_impact_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate impact metrics for detected anomalies.
        
        Args:
            df: DataFrame with anomalies
            
        Returns:
            DataFrame with impact metrics added
        """
        result = df.copy()
        
        # Expected value (use Prophet if available, else rolling average)
        if 'prophet_expected_value' in result.columns:
            expected_conversion = result['prophet_expected_value']
        elif 'rolling_7d_conversion' in result.columns:
            expected_conversion = result['rolling_7d_conversion']
        elif 'brand_avg_conversion' in result.columns:
            expected_conversion = result['brand_avg_conversion']
        else:
            # Fallback to mean by brand
            expected_conversion = result.groupby('brand')['conversion_rate'].transform('mean')
        
        result['expected_value'] = expected_conversion
        
        # Calculate deviation
        if 'conversion_rate' in result.columns:
            result['deviation_percentage'] = (
                (result['conversion_rate'] - expected_conversion) / 
                (expected_conversion + 1e-8) * 100
            )
        else:
            result['deviation_percentage'] = 0.0
        
        # Estimated revenue impact
        if 'views' in result.columns and 'avg_order_value' in result.columns:
            expected_purchases = result['views'] * expected_conversion
            actual_purchases = result.get('purchases', 0)
            purchase_diff = actual_purchases - expected_purchases
            
            result['estimated_revenue_impact'] = purchase_diff * result['avg_order_value']
        elif 'views' in result.columns and 'revenue' in result.columns:
            # Estimate based on revenue per view
            expected_revenue_per_view = result.groupby('brand')['revenue'].transform('mean') / (
                result.groupby('brand')['views'].transform('mean') + 1e-8
            )
            expected_revenue = result['views'] * expected_revenue_per_view
            result['estimated_revenue_impact'] = result['revenue'] - expected_revenue
        else:
            result['estimated_revenue_impact'] = 0.0
        
        # Estimated conversion loss (in number of purchases)
        if 'views' in result.columns:
            expected_purchases = result['views'] * expected_conversion
            actual_purchases = result.get('purchases', 0)
            result['estimated_conversion_loss'] = expected_purchases - actual_purchases
        else:
            result['estimated_conversion_loss'] = 0.0
        
        return result
    
    def filter_by_severity(self, df: pd.DataFrame, min_severity: str = 'medium') -> pd.DataFrame:
        """
        Filter anomalies by minimum severity level.
        
        Args:
            df: DataFrame with anomalies
            min_severity: Minimum severity ('low', 'medium', 'high', 'critical')
            
        Returns:
            Filtered DataFrame
        """
        severity_order = ['normal', 'low', 'medium', 'high', 'critical']
        
        if min_severity not in severity_order:
            raise ValueError(f"Invalid severity: {min_severity}")
        
        min_level = severity_order.index(min_severity)
        
        def severity_level(s):
            return severity_order.index(s) if s in severity_order else 0
        
        mask = df['anomaly_severity'].apply(severity_level) >= min_level
        
        return df[mask]
    
    def get_top_anomalies(self, df: pd.DataFrame, n: int = 10, 
                         by: str = 'ensemble_anomaly_score') -> pd.DataFrame:
        """
        Get top N anomalies by specified metric.
        
        Args:
            df: DataFrame with anomalies
            n: Number of top anomalies to return
            by: Column to sort by
            
        Returns:
            Top N anomalies
        """
        anomalies = df[df['is_anomaly']].copy()
        
        if len(anomalies) == 0:
            return anomalies
        
        if by not in anomalies.columns:
            by = 'ensemble_anomaly_score'
        
        return anomalies.nlargest(n, by)
    
    def get_summary_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Get summary statistics for ensemble predictions.
        
        Args:
            df: DataFrame with predictions
            
        Returns:
            Dictionary with summary statistics
        """
        anomalies = df[df['is_anomaly']]
        
        stats = {
            'total_records': len(df),
            'total_anomalies': len(anomalies),
            'anomaly_rate': len(anomalies) / len(df) if len(df) > 0 else 0,
            
            'by_severity': df['anomaly_severity'].value_counts().to_dict(),
            'by_type': df[df['is_anomaly']]['anomaly_type'].value_counts().to_dict(),
            
            'avg_anomaly_score': df['ensemble_anomaly_score'].mean(),
            'max_anomaly_score': df['ensemble_anomaly_score'].max(),
            
            'avg_confidence': df[df['is_anomaly']]['anomaly_confidence'].mean() if len(anomalies) > 0 else 0,
            
            'total_estimated_revenue_impact': anomalies['estimated_revenue_impact'].sum() if 'estimated_revenue_impact' in anomalies.columns else 0,
            'total_estimated_conversion_loss': anomalies['estimated_conversion_loss'].sum() if 'estimated_conversion_loss' in anomalies.columns else 0,
        }
        
        return stats


def create_ensemble(config=None) -> AnomalyEnsemble:
    """
    Factory function to create ensemble framework.
    
    Args:
        config (dict, optional): Ensemble configuration
        
    Returns:
        AnomalyEnsemble instance
    """
    return AnomalyEnsemble(config=config)

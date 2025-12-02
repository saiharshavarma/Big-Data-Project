"""
Prophet-Based Anomaly Detection
================================
Uses Facebook Prophet for time series forecasting with seasonality
to detect anomalies based on prediction intervals.
"""

import pandas as pd
import numpy as np
from typing import Optional
import warnings
warnings.filterwarnings('ignore')

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    print("Warning: Prophet not available. Prophet detector will not be functional.")

import sys
sys.path.append('..')
from config.anomaly_config import PROPHET_CONFIG


class ProphetAnomalyDetector:
    """
    Prophet-based anomaly detector for time series with seasonality.
    Detects anomalies when observations fall outside prediction intervals.
    """
    
    def __init__(self, config=None):
        """
        Initialize Prophet detector.
        
        Args:
            config (dict, optional): Model configuration. Defaults to PROPHET_CONFIG.
        """
        if not PROPHET_AVAILABLE:
            raise ImportError("Prophet is required for ProphetAnomalyDetector")
        
        self.config = config or PROPHET_CONFIG
        self.models = {}  # One model per brand
    
    def _create_prophet_model(self) -> Prophet:
        """Create and configure a Prophet model."""
        model = Prophet(
            changepoint_prior_scale=self.config["changepoint_prior_scale"],
            seasonality_prior_scale=self.config["seasonality_prior_scale"],
            seasonality_mode=self.config["seasonality_mode"],
            interval_width=self.config["interval_width"],
            uncertainty_samples=self.config["uncertainty_samples"],
            yearly_seasonality=self.config["yearly_seasonality"],
            weekly_seasonality=self.config["weekly_seasonality"],
            daily_seasonality=self.config["daily_seasonality"],
        )
        
        return model
    
    def _prepare_data_for_prophet(self, data: pd.DataFrame, value_col: str) -> pd.DataFrame:
        """
        Prepare data in Prophet's required format.
        
        Args:
            data: Input data
            value_col: Column to forecast
            
        Returns:
            DataFrame with 'ds' and 'y' columns
        """
        df = data[['window_start', value_col]].copy()
        df.columns = ['ds', 'y']
        df = df.sort_values('ds').reset_index(drop=True)
        
        # Remove any missing values
        df = df.dropna()
        
        return df
    
    def train(self, data: pd.DataFrame, value_col: str = 'conversion_rate') -> dict:
        """
        Train Prophet models for each brand.
        
        Args:
            data: Training data with brand, window_start, and value columns
            value_col: Column name to model
            
        Returns:
            Training statistics
        """
        stats = {}
        brands = data['brand'].unique()
        
        print(f"Training Prophet models for {len(brands)} brands...")
        
        for i, brand in enumerate(brands):
            if (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(brands)} brands")
            
            brand_data = data[data['brand'] == brand]
            
            # Skip if not enough data
            if len(brand_data) < 30:  # Need at least 30 data points
                print(f"  Skipping {brand}: insufficient data ({len(brand_data)} points)")
                continue
            
            try:
                # Prepare data
                prophet_df = self._prepare_data_for_prophet(brand_data, value_col)
                
                # Create and fit model
                model = self._create_prophet_model()
                
                # Suppress Prophet's verbose output
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model.fit(prophet_df)
                
                self.models[brand] = model
                
                stats[brand] = {
                    'n_samples': len(prophet_df),
                    'trained': True
                }
                
            except Exception as e:
                print(f"  Error training model for {brand}: {e}")
                stats[brand] = {
                    'n_samples': len(brand_data),
                    'trained': False,
                    'error': str(e)
                }
        
        print(f"Successfully trained {len(self.models)}/{len(brands)} models")
        
        return stats
    
    def detect_anomalies(self, data: pd.DataFrame, value_col: str = 'conversion_rate') -> pd.DataFrame:
        """
        Detect anomalies using Prophet predictions.
        
        Args:
            data: Data to analyze
            value_col: Column to check for anomalies
            
        Returns:
            DataFrame with anomaly scores and flags
        """
        if not self.models:
            raise ValueError("No models trained. Call train() first.")
        
        result = data.copy()
        result['prophet_anomaly_score'] = 0.0
        result['prophet_is_anomaly'] = False
        result['prophet_expected_value'] = result[value_col]
        result['prophet_lower_bound'] = result[value_col]
        result['prophet_upper_bound'] = result[value_col]
        
        for brand, model in self.models.items():
            brand_mask = result['brand'] == brand
            brand_data = result[brand_mask].copy()
            
            if len(brand_data) == 0:
                continue
            
            try:
                # Prepare forecast dataframe
                future = brand_data[['window_start']].copy()
                future.columns = ['ds']
                future = future.sort_values('ds').reset_index(drop=True)
                
                # Make predictions
                forecast = model.predict(future)
                
                # Get predictions and bounds
                yhat = forecast['yhat'].values
                yhat_lower = forecast['yhat_lower'].values
                yhat_upper = forecast['yhat_upper'].values
                
                # Get actual values
                actual = brand_data[value_col].values
                
                # Calculate anomaly scores
                # Score is how far outside the interval the value is
                anomaly_scores = np.zeros_like(actual)
                
                # Below lower bound
                below_mask = actual < yhat_lower
                if below_mask.any():
                    interval_width = yhat[below_mask] - yhat_lower[below_mask]
                    deviation = yhat_lower[below_mask] - actual[below_mask]
                    anomaly_scores[below_mask] = np.clip(deviation / (interval_width + 1e-8), 0, 1)
                
                # Above upper bound
                above_mask = actual > yhat_upper
                if above_mask.any():
                    interval_width = yhat_upper[above_mask] - yhat[above_mask]
                    deviation = actual[above_mask] - yhat_upper[above_mask]
                    anomaly_scores[above_mask] = np.clip(deviation / (interval_width + 1e-8), 0, 1)
                
                # Assign back to result
                result.loc[brand_mask, 'prophet_anomaly_score'] = anomaly_scores
                result.loc[brand_mask, 'prophet_is_anomaly'] = (
                    (actual < yhat_lower) | (actual > yhat_upper)
                )
                result.loc[brand_mask, 'prophet_expected_value'] = yhat
                result.loc[brand_mask, 'prophet_lower_bound'] = yhat_lower
                result.loc[brand_mask, 'prophet_upper_bound'] = yhat_upper
                
            except Exception as e:
                print(f"Error predicting for brand {brand}: {e}")
                continue
        
        return result
    
    def get_forecast(self, brand: str, periods: int = 168) -> Optional[pd.DataFrame]:
        """
        Get future forecast for a specific brand.
        
        Args:
            brand: Brand name
            periods: Number of hours to forecast
            
        Returns:
            Forecast DataFrame or None if brand not found
        """
        if brand not in self.models:
            return None
        
        model = self.models[brand]
        
        # Create future dataframe
        future = model.make_future_dataframe(periods=periods, freq='H')
        
        # Make forecast
        forecast = model.predict(future)
        
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'trend']]
    
    def get_seasonality_components(self, brand: str) -> Optional[dict]:
        """
        Extract seasonality components for a brand.
        
        Args:
            brand: Brand name
            
        Returns:
            Dictionary with seasonality information
        """
        if brand not in self.models:
            return None
        
        model = self.models[brand]
        
        # Get a representative forecast to extract components
        future = model.make_future_dataframe(periods=168, freq='H')
        forecast = model.predict(future)
        
        components = {
            'trend': forecast['trend'].values,
            'timestamps': forecast['ds'].values,
        }
        
        # Add seasonality components if they exist
        if 'weekly' in forecast.columns:
            components['weekly'] = forecast['weekly'].values
        if 'daily' in forecast.columns:
            components['daily'] = forecast['daily'].values
        if 'yearly' in forecast.columns:
            components['yearly'] = forecast['yearly'].values
        
        return components


def create_prophet_detector(config=None) -> ProphetAnomalyDetector:
    """
    Factory function to create Prophet anomaly detector.
    
    Args:
        config (dict, optional): Model configuration
        
    Returns:
        ProphetAnomalyDetector instance
    """
    return ProphetAnomalyDetector(config=config)

"""
Bayesian Changepoint Detection
===============================
Detects structural breaks and sudden shifts in conversion patterns.
Uses the ruptures library for changepoint detection.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Tuple

try:
    import ruptures as rpt
    RUPTURES_AVAILABLE = True
except ImportError:
    RUPTURES_AVAILABLE = False
    print("Warning: ruptures library not available. Changepoint detector will not be functional.")

import sys
sys.path.append('..')
from config.anomaly_config import CHANGEPOINT_CONFIG


class ChangepointDetector:
    """
    Detects changepoints (structural breaks) in time series data.
    Useful for identifying sudden shifts in user behavior or campaign impacts.
    """
    
    def __init__(self, config=None):
        """
        Initialize changepoint detector.
        
        Args:
            config (dict, optional): Model configuration. Defaults to CHANGEPOINT_CONFIG.
        """
        if not RUPTURES_AVAILABLE:
            raise ImportError("ruptures library is required for ChangepointDetector")
        
        self.config = config or CHANGEPOINT_CONFIG
        self.changepoints = {}  # Store detected changepoints per brand
    
    def _detect_changepoints_single_series(self, signal: np.ndarray) -> List[int]:
        """
        Detect changepoints in a single time series.
        
        Args:
            signal: 1D time series data
            
        Returns:
            List of changepoint indices
        """
        method = self.config["method"]
        model = self.config["model"]
        min_size = self.config["min_size"]
        jump = self.config["jump"]
        penalty = self.config["penalty"]
        
        if len(signal) < min_size * 2:
            return []
        
        try:
            # Create detection algorithm
            if method.lower() == "pelt":
                algo = rpt.Pelt(model=model, min_size=min_size, jump=jump)
            elif method.lower() == "binseg":
                algo = rpt.Binseg(model=model, min_size=min_size, jump=jump)
            elif method.lower() == "bottomup":
                algo = rpt.BottomUp(model=model, min_size=min_size, jump=jump)
            else:
                algo = rpt.Pelt(model=model, min_size=min_size, jump=jump)
            
            # Fit and predict
            algo.fit(signal)
            changepoints = algo.predict(pen=penalty)
            
            # Remove the last point (end of signal)
            if changepoints and changepoints[-1] == len(signal):
                changepoints = changepoints[:-1]
            
            return changepoints
            
        except Exception as e:
            print(f"Error detecting changepoints: {e}")
            return []
    
    def _calculate_magnitude(self, signal: np.ndarray, changepoints: List[int]) -> List[float]:
        """
        Calculate magnitude of change at each changepoint.
        
        Args:
            signal: Time series data
            changepoints: List of changepoint indices
            
        Returns:
            List of change magnitudes (absolute percentage change)
        """
        magnitudes = []
        
        if not changepoints:
            return magnitudes
        
        # Add start and end points
        all_points = [0] + changepoints + [len(signal)]
        
        for i in range(len(all_points) - 1):
            start_idx = all_points[i]
            end_idx = all_points[i + 1]
            
            # Calculate mean before and after changepoint
            segment = signal[start_idx:end_idx]
            segment_mean = np.mean(segment)
            
            if i > 0:
                prev_segment = signal[all_points[i-1]:start_idx]
                prev_mean = np.mean(prev_segment)
                
                # Calculate percentage change
                if prev_mean != 0:
                    change = abs((segment_mean - prev_mean) / prev_mean)
                else:
                    change = 0.0
                
                magnitudes.append(change)
        
        return magnitudes
    
    def train(self, data: pd.DataFrame, value_col: str = 'conversion_rate') -> dict:
        """
        Detect changepoints for each brand.
        
        Args:
            data: Training data with brand, window_start, and value columns
            value_col: Column to analyze for changepoints
            
        Returns:
            Dictionary with detected changepoints per brand
        """
        stats = {}
        brands = data['brand'].unique()
        
        print(f"Detecting changepoints for {len(brands)} brands...")
        
        for i, brand in enumerate(brands):
            if (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(brands)} brands")
            
            brand_data = data[data['brand'] == brand].sort_values('window_start')
            
            # Skip if not enough data
            min_size = self.config["min_size"]
            if len(brand_data) < min_size * 2:
                print(f"  Skipping {brand}: insufficient data ({len(brand_data)} points)")
                continue
            
            try:
                # Get time series
                signal = brand_data[value_col].values
                timestamps = brand_data['window_start'].values
                
                # Detect changepoints
                changepoints = self._detect_changepoints_single_series(signal)
                
                # Calculate magnitudes
                magnitudes = self._calculate_magnitude(signal, changepoints)
                
                # Filter by minimum magnitude
                min_magnitude = self.config["min_changepoint_magnitude"]
                significant_changepoints = []
                
                for cp_idx, mag in zip(changepoints, magnitudes):
                    if mag >= min_magnitude:
                        significant_changepoints.append({
                            'index': cp_idx,
                            'timestamp': timestamps[cp_idx] if cp_idx < len(timestamps) else timestamps[-1],
                            'magnitude': mag,
                            'value_before': signal[max(0, cp_idx - 1)],
                            'value_after': signal[min(cp_idx, len(signal) - 1)]
                        })
                
                self.changepoints[brand] = significant_changepoints
                
                stats[brand] = {
                    'n_samples': len(brand_data),
                    'n_changepoints_detected': len(changepoints),
                    'n_significant_changepoints': len(significant_changepoints),
                    'changepoints': significant_changepoints
                }
                
            except Exception as e:
                print(f"  Error detecting changepoints for {brand}: {e}")
                stats[brand] = {
                    'n_samples': len(brand_data),
                    'error': str(e)
                }
        
        print(f"Successfully analyzed {len(self.changepoints)}/{len(brands)} brands")
        
        total_changepoints = sum(len(cps) for cps in self.changepoints.values())
        print(f"Total significant changepoints detected: {total_changepoints}")
        
        return stats
    
    def detect_anomalies(self, data: pd.DataFrame, value_col: str = 'conversion_rate') -> pd.DataFrame:
        """
        Flag data points near changepoints as anomalies.
        
        Args:
            data: Data to analyze
            value_col: Column that was analyzed for changepoints
            
        Returns:
            DataFrame with changepoint anomaly scores and flags
        """
        if not self.changepoints:
            print("Warning: No changepoints detected. Call train() first.")
        
        result = data.copy()
        result['changepoint_anomaly_score'] = 0.0
        result['changepoint_is_anomaly'] = False
        result['changepoint_magnitude'] = 0.0
        result['is_at_changepoint'] = False
        
        # Window for marking points near changepoints (e.g., ±2 hours)
        window_hours = 2
        
        for brand, changepoint_list in self.changepoints.items():
            brand_data = result[result['brand'] == brand].copy()
            
            if len(brand_data) == 0 or not changepoint_list:
                continue
            
            brand_data = brand_data.sort_values('window_start').reset_index(drop=True)
            
            for cp in changepoint_list:
                cp_timestamp = cp['timestamp']
                magnitude = cp['magnitude']
                
                # Find points near this changepoint
                time_diff = (brand_data['window_start'] - cp_timestamp).dt.total_seconds() / 3600
                near_changepoint = abs(time_diff) <= window_hours
                
                # Create anomaly score based on magnitude
                anomaly_score = min(magnitude * 2, 1.0)  # Scale to [0, 1]
                
                # Update result
                mask = (result['brand'] == brand) & \
                       (result['window_start'].isin(brand_data.loc[near_changepoint, 'window_start']))
                
                result.loc[mask, 'changepoint_anomaly_score'] = anomaly_score
                result.loc[mask, 'changepoint_is_anomaly'] = True
                result.loc[mask, 'changepoint_magnitude'] = magnitude
                result.loc[mask, 'is_at_changepoint'] = True
        
        return result
    
    def get_changepoint_summary(self, brand: Optional[str] = None) -> pd.DataFrame:
        """
        Get summary of detected changepoints.
        
        Args:
            brand: Specific brand to get summary for (None = all brands)
            
        Returns:
            DataFrame with changepoint information
        """
        if brand:
            if brand not in self.changepoints:
                return pd.DataFrame()
            brands_to_process = {brand: self.changepoints[brand]}
        else:
            brands_to_process = self.changepoints
        
        records = []
        for brand_name, changepoint_list in brands_to_process.items():
            for cp in changepoint_list:
                records.append({
                    'brand': brand_name,
                    'timestamp': cp['timestamp'],
                    'magnitude': cp['magnitude'],
                    'value_before': cp['value_before'],
                    'value_after': cp['value_after'],
                    'change_direction': 'increase' if cp['value_after'] > cp['value_before'] else 'decrease'
                })
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = df.sort_values(['brand', 'timestamp'])
        
        return df
    
    def visualize_changepoints(self, brand: str, data: pd.DataFrame, 
                               value_col: str = 'conversion_rate'):
        """
        Prepare data for visualizing changepoints (returns data, doesn't plot).
        
        Args:
            brand: Brand to visualize
            data: Full dataset
            value_col: Column to plot
            
        Returns:
            Tuple of (time series data, changepoint locations)
        """
        if brand not in self.changepoints:
            return None, None
        
        brand_data = data[data['brand'] == brand].sort_values('window_start')
        signal = brand_data[value_col].values
        timestamps = brand_data['window_start'].values
        
        changepoint_info = self.changepoints[brand]
        changepoint_timestamps = [cp['timestamp'] for cp in changepoint_info]
        
        return (timestamps, signal), changepoint_timestamps


def create_changepoint_detector(config=None) -> ChangepointDetector:
    """
    Factory function to create changepoint detector.
    
    Args:
        config (dict, optional): Model configuration
        
    Returns:
        ChangepointDetector instance
    """
    return ChangepointDetector(config=config)

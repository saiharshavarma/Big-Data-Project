"""
Isolation Forest Anomaly Detection
===================================
Uses Isolation Forest for multivariate outlier detection.
Good for detecting global anomalies across multiple features.
"""

import pandas as pd
import numpy as np
from typing import List, Optional

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn not available. Isolation Forest will not be functional.")

import sys
sys.path.append('..')
from config.anomaly_config import ISOLATION_FOREST_CONFIG


class IsolationForestDetector:
    """
    Isolation Forest for multivariate anomaly detection.
    Detects anomalies based on how isolated points are in the feature space.
    """
    
    def __init__(self, config=None):
        """
        Initialize Isolation Forest detector.
        
        Args:
            config (dict, optional): Model configuration. Defaults to ISOLATION_FOREST_CONFIG.
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for IsolationForestDetector")
        
        self.config = config or ISOLATION_FOREST_CONFIG
        self.models = {}  # One model per brand
        self.scalers = {}  # One scaler per brand
        self.feature_cols = self.config.get("features", [])
    
    def _create_isolation_forest(self) -> IsolationForest:
        """Create and configure an Isolation Forest model."""
        model = IsolationForest(
            n_estimators=self.config["n_estimators"],
            max_samples=self.config["max_samples"],
            contamination=self.config["contamination"],
            max_features=self.config["max_features"],
            bootstrap=self.config["bootstrap"],
            random_state=self.config["random_state"],
            verbose=0
        )
        
        return model
    
    def _prepare_features(self, data: pd.DataFrame, feature_cols: List[str]) -> np.ndarray:
        """
        Prepare and validate features.
        
        Args:
            data: Input data
            feature_cols: List of feature column names
            
        Returns:
            Feature matrix
        """
        # Check which features are available
        available_features = [col for col in feature_cols if col in data.columns]
        
        if not available_features:
            raise ValueError(f"None of the specified features found in data: {feature_cols}")
        
        if len(available_features) < len(feature_cols):
            missing = set(feature_cols) - set(available_features)
            print(f"Warning: Missing features: {missing}")
        
        # Extract features
        X = data[available_features].values
        
        # Handle missing values
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        return X, available_features
    
    def train(self, data: pd.DataFrame, feature_cols: Optional[List[str]] = None) -> dict:
        """
        Train Isolation Forest models for each brand.
        
        Args:
            data: Training data with brand and feature columns
            feature_cols: List of feature columns (uses config default if None)
            
        Returns:
            Training statistics
        """
        if feature_cols is None:
            feature_cols = self.feature_cols
        
        if not feature_cols:
            raise ValueError("No features specified for training")
        
        stats = {}
        brands = data['brand'].unique()
        
        print(f"Training Isolation Forest models for {len(brands)} brands...")
        print(f"Using features: {feature_cols}")
        
        for i, brand in enumerate(brands):
            if (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(brands)} brands")
            
            brand_data = data[data['brand'] == brand]
            
            # Skip if not enough data
            min_samples = self.config.get("n_estimators", 100)
            if len(brand_data) < min_samples:
                print(f"  Skipping {brand}: insufficient data ({len(brand_data)} points)")
                continue
            
            try:
                # Prepare features
                X, available_features = self._prepare_features(brand_data, feature_cols)
                
                if X.shape[0] < min_samples:
                    continue
                
                # Scale features
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Create and fit model
                model = self._create_isolation_forest()
                model.fit(X_scaled)
                
                # Store model and scaler
                self.models[brand] = model
                self.scalers[brand] = scaler
                
                stats[brand] = {
                    'n_samples': len(brand_data),
                    'n_features': X.shape[1],
                    'features': available_features,
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
    
    def detect_anomalies(self, data: pd.DataFrame, feature_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Detect anomalies using Isolation Forest.
        
        Args:
            data: Data to analyze
            feature_cols: List of feature columns (uses config default if None)
            
        Returns:
            DataFrame with anomaly scores and flags
        """
        if not self.models:
            raise ValueError("No models trained. Call train() first.")
        
        if feature_cols is None:
            feature_cols = self.feature_cols
        
        result = data.copy()
        result['isolation_forest_anomaly_score'] = 0.0
        result['isolation_forest_is_anomaly'] = False
        result['isolation_forest_decision_score'] = 0.0
        
        for brand, model in self.models.items():
            brand_mask = result['brand'] == brand
            brand_data = result[brand_mask].copy()
            
            if len(brand_data) == 0:
                continue
            
            try:
                # Prepare features
                X, _ = self._prepare_features(brand_data, feature_cols)
                
                # Scale features
                scaler = self.scalers[brand]
                X_scaled = scaler.transform(X)
                
                # Predict anomalies (-1 = anomaly, 1 = normal)
                predictions = model.predict(X_scaled)
                
                # Get anomaly scores (negative values = anomalies)
                decision_scores = model.decision_function(X_scaled)
                
                # Convert scores to 0-1 range (higher = more anomalous)
                # Decision scores are typically in range [-0.5, 0.5]
                # Negative = anomaly, positive = normal
                anomaly_scores = np.clip(-decision_scores, 0, 1)
                
                # Normalize to ensure range [0, 1]
                if anomaly_scores.max() > 0:
                    anomaly_scores = anomaly_scores / anomaly_scores.max()
                
                # Assign back to result
                result.loc[brand_mask, 'isolation_forest_decision_score'] = decision_scores
                result.loc[brand_mask, 'isolation_forest_anomaly_score'] = anomaly_scores
                result.loc[brand_mask, 'isolation_forest_is_anomaly'] = (predictions == -1)
                
            except Exception as e:
                print(f"Error predicting for brand {brand}: {e}")
                continue
        
        return result
    
    def get_feature_importance(self, brand: str, n_samples: int = 1000) -> Optional[dict]:
        """
        Estimate feature importance for a brand (using permutation).
        
        Args:
            brand: Brand name
            n_samples: Number of samples to use for estimation
            
        Returns:
            Dictionary with feature importance scores
        """
        if brand not in self.models:
            return None
        
        model = self.models[brand]
        scaler = self.scalers[brand]
        
        # Note: Isolation Forest doesn't have built-in feature importance
        # This is a simplified estimation
        return {
            'note': 'Feature importance not directly available for Isolation Forest',
            'n_estimators': self.config["n_estimators"],
            'contamination': self.config["contamination"]
        }
    
    def update_model(self, brand: str, new_data: pd.DataFrame, feature_cols: Optional[List[str]] = None):
        """
        Update (retrain) model for a specific brand with new data.
        
        Args:
            brand: Brand name
            new_data: New training data
            feature_cols: Feature columns
        """
        if feature_cols is None:
            feature_cols = self.feature_cols
        
        try:
            # Prepare features
            X, available_features = self._prepare_features(new_data, feature_cols)
            
            # Scale features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Create and fit new model
            model = self._create_isolation_forest()
            model.fit(X_scaled)
            
            # Update stored model and scaler
            self.models[brand] = model
            self.scalers[brand] = scaler
            
            print(f"Model updated for brand {brand} with {len(new_data)} samples")
            
        except Exception as e:
            print(f"Error updating model for {brand}: {e}")


def create_isolation_forest_detector(config=None) -> IsolationForestDetector:
    """
    Factory function to create Isolation Forest detector.
    
    Args:
        config (dict, optional): Model configuration
        
    Returns:
        IsolationForestDetector instance
    """
    return IsolationForestDetector(config=config)

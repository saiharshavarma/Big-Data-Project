"""
LSTM Autoencoder for Anomaly Detection
=======================================
Uses LSTM autoencoder to learn normal patterns in time series data
and detect anomalies based on reconstruction error.
"""

import numpy as np
import pandas as pd
from typing import Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras.models import Model, Sequential
    from tensorflow.keras.layers import LSTM, RepeatVector, TimeDistributed, Dense, Dropout, Input
    from tensorflow.keras.callbacks import EarlyStopping
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    print("Warning: TensorFlow not available. LSTM Autoencoder will not be functional.")

import sys
sys.path.append('..')
from config.anomaly_config import LSTM_CONFIG


class LSTMAutoencoder:
    """
    LSTM-based autoencoder for temporal anomaly detection.
    Learns normal patterns and flags anomalies based on reconstruction error.
    """
    
    def __init__(self, config=None):
        """
        Initialize LSTM Autoencoder.
        
        Args:
            config (dict, optional): Model configuration. Defaults to LSTM_CONFIG.
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required for LSTM Autoencoder")
        
        self.config = config or LSTM_CONFIG
        self.model = None
        self.threshold = None
        self.scaler_mean = None
        self.scaler_std = None
    
    def build_model(self, input_shape: Tuple[int, int]) -> Model:
        """
        Build LSTM autoencoder architecture.
        
        Args:
            input_shape: (sequence_length, n_features)
            
        Returns:
            Compiled Keras model
        """
        sequence_length, n_features = input_shape
        encoding_dim = self.config["encoding_dim"]
        lstm_units = self.config["lstm_units"]
        dropout = self.config["dropout_rate"]
        
        # Encoder
        encoder_input = Input(shape=input_shape, name='encoder_input')
        x = encoder_input
        
        for i, units in enumerate(lstm_units):
            return_sequences = (i < len(lstm_units) - 1)
            x = LSTM(units, activation='tanh', return_sequences=return_sequences,
                    name=f'encoder_lstm_{i}')(x)
            x = Dropout(dropout, name=f'encoder_dropout_{i}')(x)
        
        # Bottleneck
        encoded = Dense(encoding_dim, activation='relu', name='bottleneck')(x)
        
        # Decoder
        x = RepeatVector(sequence_length, name='repeat_vector')(encoded)
        
        for i, units in enumerate(reversed(lstm_units)):
            x = LSTM(units, activation='tanh', return_sequences=True,
                    name=f'decoder_lstm_{i}')(x)
            x = Dropout(dropout, name=f'decoder_dropout_{i}')(x)
        
        # Output
        decoder_output = TimeDistributed(Dense(n_features), name='decoder_output')(x)
        
        # Build model
        autoencoder = Model(encoder_input, decoder_output, name='lstm_autoencoder')
        
        # Compile
        optimizer = keras.optimizers.Adam(learning_rate=self.config["learning_rate"])
        autoencoder.compile(optimizer=optimizer, loss='mse', metrics=['mae'])
        
        return autoencoder
    
    def prepare_sequences(self, data: pd.DataFrame, feature_cols: list) -> np.ndarray:
        """
        Prepare sequential data for LSTM.
        
        Args:
            data: Time series data
            feature_cols: List of feature column names
            
        Returns:
            3D numpy array (samples, sequence_length, features)
        """
        sequence_length = self.config["sequence_length"]
        
        # Sort by time
        data = data.sort_values('window_start').reset_index(drop=True)
        
        # Extract features
        values = data[feature_cols].values
        
        # Normalize
        if self.scaler_mean is None:
            self.scaler_mean = np.mean(values, axis=0)
            self.scaler_std = np.std(values, axis=0) + 1e-8
        
        values_scaled = (values - self.scaler_mean) / self.scaler_std
        
        # Create sequences
        sequences = []
        for i in range(len(values_scaled) - sequence_length + 1):
            sequences.append(values_scaled[i:i + sequence_length])
        
        return np.array(sequences)
    
    def train(self, data: pd.DataFrame, feature_cols: list) -> dict:
        """
        Train the LSTM autoencoder.
        
        Args:
            data: Training data
            feature_cols: List of feature columns to use
            
        Returns:
            Training history
        """
        print("Preparing training sequences...")
        X_train = self.prepare_sequences(data, feature_cols)
        
        if len(X_train) == 0:
            raise ValueError("Not enough data to create training sequences")
        
        print(f"Training sequences shape: {X_train.shape}")
        
        # Build model
        input_shape = (X_train.shape[1], X_train.shape[2])
        self.model = self.build_model(input_shape)
        
        print("\nModel architecture:")
        self.model.summary()
        
        # Callbacks
        early_stopping = EarlyStopping(
            monitor='val_loss',
            patience=self.config["early_stopping_patience"],
            restore_best_weights=True,
            verbose=1
        )
        
        # Train
        print("\nTraining model...")
        history = self.model.fit(
            X_train, X_train,  # Autoencoder: input = output
            epochs=self.config["epochs"],
            batch_size=self.config["batch_size"],
            validation_split=self.config["validation_split"],
            callbacks=[early_stopping],
            verbose=1
        )
        
        # Calculate reconstruction threshold
        print("\nCalculating anomaly threshold...")
        train_reconstructions = self.model.predict(X_train, verbose=0)
        train_errors = np.mean(np.abs(X_train - train_reconstructions), axis=(1, 2))
        
        percentile = self.config["reconstruction_threshold_percentile"]
        multiplier = self.config["anomaly_threshold_multiplier"]
        self.threshold = np.percentile(train_errors, percentile) * multiplier
        
        print(f"Anomaly threshold set to: {self.threshold:.6f}")
        
        return {
            'history': history.history,
            'threshold': self.threshold,
            'final_loss': history.history['loss'][-1],
            'final_val_loss': history.history['val_loss'][-1],
        }
    
    def detect_anomalies(self, data: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
        """
        Detect anomalies in new data.
        
        Args:
            data: Data to analyze
            feature_cols: Feature columns
            
        Returns:
            DataFrame with anomaly scores and flags
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        # Prepare sequences
        X = self.prepare_sequences(data, feature_cols)
        
        if len(X) == 0:
            # Not enough data for sequences
            result = data.copy()
            result['lstm_anomaly_score'] = 0.0
            result['lstm_is_anomaly'] = False
            return result
        
        # Get reconstructions
        X_reconstructed = self.model.predict(X, verbose=0)
        
        # Calculate reconstruction errors
        reconstruction_errors = np.mean(np.abs(X - X_reconstructed), axis=(1, 2))
        
        # Normalize scores to 0-1 range
        if self.threshold > 0:
            anomaly_scores = np.clip(reconstruction_errors / self.threshold, 0, 1)
        else:
            anomaly_scores = np.zeros_like(reconstruction_errors)
        
        # Pad scores for sequences
        sequence_length = self.config["sequence_length"]
        full_scores = np.zeros(len(data))
        full_scores[sequence_length - 1:] = anomaly_scores
        
        # Create result DataFrame
        result = data.copy()
        result['lstm_reconstruction_error'] = full_scores
        result['lstm_anomaly_score'] = full_scores
        result['lstm_is_anomaly'] = full_scores > 1.0
        
        return result
    
    def save_model(self, path: str):
        """Save model and scaler parameters."""
        if self.model is None:
            raise ValueError("No model to save")
        
        # Save Keras model
        self.model.save(f"{path}_model.h5")
        
        # Save scaler and threshold
        np.savez(
            f"{path}_params.npz",
            scaler_mean=self.scaler_mean,
            scaler_std=self.scaler_std,
            threshold=self.threshold
        )
        
        print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        """Load model and scaler parameters."""
        # Load Keras model
        self.model = keras.models.load_model(f"{path}_model.h5")
        
        # Load scaler and threshold
        params = np.load(f"{path}_params.npz")
        self.scaler_mean = params['scaler_mean']
        self.scaler_std = params['scaler_std']
        self.threshold = params['threshold']
        
        print(f"Model loaded from {path}")


def create_lstm_detector(config=None) -> LSTMAutoencoder:
    """
    Factory function to create LSTM autoencoder detector.
    
    Args:
        config (dict, optional): Model configuration
        
    Returns:
        LSTMAutoencoder instance
    """
    return LSTMAutoencoder(config=config)

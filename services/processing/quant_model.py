"""
Quantitative Model

Generates price and volatility forecasts using traditional machine learning models
with model versioning and performance tracking.
"""

import logging
import pickle
import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from pathlib import Path
import json

# ML imports
try:
    import sklearn
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class ModelConfig:
    """Configuration for a quantitative model."""
    name: str
    model_type: str  # 'random_forest', 'xgboost', 'linear', 'gradient_boosting'
    version: str
    file_path: str
    features: List[str]
    target: str
    parameters: Dict[str, Any]
    performance_metrics: Optional[Dict[str, float]] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class PredictionResult:
    """Result of a model prediction."""
    symbol: str
    prediction_type: str  # 'price', 'volatility', 'return'
    predicted_value: float
    confidence_interval: Optional[Tuple[float, float]]
    model_name: str
    model_version: str
    features_used: Dict[str, float]
    timestamp: datetime


class ModelManager:
    """Manages loading and versioning of quantitative models."""
    
    def __init__(self, models_directory: str = "models"):
        """
        Initialize model manager.
        
        Args:
            models_directory: Directory containing model files
        """
        self.models_directory = Path(models_directory)
        self.models_directory.mkdir(exist_ok=True)
        self.loaded_models: Dict[str, Any] = {}
        self.model_configs: Dict[str, ModelConfig] = {}
        self.logger = logging.getLogger(f"{__name__}.ModelManager")
    
    def load_model(self, config: ModelConfig) -> bool:
        """
        Load a model from file.
        
        Args:
            config: Model configuration
            
        Returns:
            True if model loaded successfully
        """
        try:
            model_path = self.models_directory / config.file_path
            
            if not model_path.exists():
                self.logger.error(f"Model file not found: {model_path}")
                return False
            
            # Load model based on file extension
            if model_path.suffix == '.pkl':
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
            elif model_path.suffix == '.joblib':
                model = joblib.load(model_path)
            else:
                self.logger.error(f"Unsupported model file format: {model_path.suffix}")
                return False
            
            model_key = f"{config.name}_{config.version}"
            self.loaded_models[model_key] = model
            self.model_configs[model_key] = config
            
            self.logger.info(f"Loaded model: {model_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading model {config.name}: {e}")
            return False
    
    def get_model(self, name: str, version: Optional[str] = None) -> Optional[Any]:
        """
        Get a loaded model.
        
        Args:
            name: Model name
            version: Model version (latest if None)
            
        Returns:
            Model object or None
        """
        if version:
            model_key = f"{name}_{version}"
            return self.loaded_models.get(model_key)
        else:
            # Find latest version
            matching_keys = [key for key in self.loaded_models.keys() if key.startswith(f"{name}_")]
            if matching_keys:
                # Sort by version (assuming semantic versioning)
                latest_key = sorted(matching_keys)[-1]
                return self.loaded_models.get(latest_key)
            return None
    
    def get_model_config(self, name: str, version: Optional[str] = None) -> Optional[ModelConfig]:
        """
        Get model configuration.
        
        Args:
            name: Model name
            version: Model version
            
        Returns:
            Model configuration or None
        """
        if version:
            model_key = f"{name}_{version}"
            return self.model_configs.get(model_key)
        else:
            # Find latest version
            matching_keys = [key for key in self.model_configs.keys() if key.startswith(f"{name}_")]
            if matching_keys:
                latest_key = sorted(matching_keys)[-1]
                return self.model_configs.get(latest_key)
            return None
    
    def save_model(self, model: Any, config: ModelConfig) -> bool:
        """
        Save a model to file.
        
        Args:
            model: Model object
            config: Model configuration
            
        Returns:
            True if saved successfully
        """
        try:
            model_path = self.models_directory / config.file_path
            model_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save model
            if config.file_path.endswith('.pkl'):
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)
            elif config.file_path.endswith('.joblib'):
                joblib.dump(model, model_path)
            else:
                self.logger.error(f"Unsupported file format: {config.file_path}")
                return False
            
            # Save configuration
            config_path = model_path.with_suffix('.json')
            with open(config_path, 'w') as f:
                config_dict = {
                    'name': config.name,
                    'model_type': config.model_type,
                    'version': config.version,
                    'file_path': config.file_path,
                    'features': config.features,
                    'target': config.target,
                    'parameters': config.parameters,
                    'performance_metrics': config.performance_metrics,
                    'created_at': config.created_at.isoformat() if config.created_at else None,
                    'last_updated': datetime.utcnow().isoformat()
                }
                json.dump(config_dict, f, indent=2)
            
            self.logger.info(f"Saved model: {config.name}_{config.version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving model {config.name}: {e}")
            return False


class FeatureProcessor:
    """Processes features for model input."""
    
    def __init__(self):
        """Initialize feature processor."""
        self.logger = logging.getLogger(f"{__name__}.FeatureProcessor")
        self.scalers: Dict[str, StandardScaler] = {}
    
    def prepare_features(self, feature_data: Dict[str, Any], fundamental_data: Dict[str, Any], 
                        required_features: List[str]) -> Optional[np.ndarray]:
        """
        Prepare features for model input.
        
        Args:
            feature_data: Technical features
            fundamental_data: Fundamental data
            required_features: List of required feature names
            
        Returns:
            Feature array or None
        """
        try:
            features = {}
            
            # Extract technical features
            if 'features' in feature_data:
                tech_features = feature_data['features']
                for key, value in tech_features.items():
                    if isinstance(value, (int, float)) and not np.isnan(value):
                        features[f"tech_{key}"] = value
            
            # Extract fundamental features
            if 'data' in fundamental_data:
                fund_data = fundamental_data['data']
                for key, value in fund_data.items():
                    if isinstance(value, (int, float)) and not np.isnan(value):
                        features[f"fund_{key}"] = value
            
            # Create feature vector
            feature_vector = []
            missing_features = []
            
            for feature_name in required_features:
                if feature_name in features:
                    feature_vector.append(features[feature_name])
                else:
                    missing_features.append(feature_name)
                    feature_vector.append(0.0)  # Default value
            
            if missing_features:
                self.logger.warning(f"Missing features: {missing_features}")
            
            return np.array(feature_vector).reshape(1, -1)
            
        except Exception as e:
            self.logger.error(f"Error preparing features: {e}")
            return None
    
    def scale_features(self, features: np.ndarray, scaler_name: str = 'default') -> np.ndarray:
        """
        Scale features using stored scaler.
        
        Args:
            features: Feature array
            scaler_name: Name of the scaler to use
            
        Returns:
            Scaled features
        """
        try:
            if scaler_name in self.scalers:
                return self.scalers[scaler_name].transform(features)
            else:
                self.logger.warning(f"Scaler {scaler_name} not found, returning unscaled features")
                return features
                
        except Exception as e:
            self.logger.error(f"Error scaling features: {e}")
            return features


class QuantitativeModel:
    """
    Quantitative model service for generating forecasts.
    
    Features:
    - Model versioning and management
    - Multiple model types support
    - Feature preprocessing
    - Performance tracking
    """
    
    def __init__(self, service_name: str = "quant_model"):
        """
        Initialize quantitative model service.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Initialize model management
        self.model_manager = ModelManager()
        self.feature_processor = FeatureProcessor()
        
        # Data storage for aggregating features
        self.feature_cache: Dict[str, Dict[str, Any]] = {}
        self.fundamental_cache: Dict[str, Dict[str, Any]] = {}
        
        # Topic names
        self.feature_events_topic = get_topic_name('processing', 'feature_events')
        self.fundamental_events_topic = get_topic_name('data_ingestion', 'fundamental_events')
        self.forecast_events_topic = get_topic_name('processing', 'forecast_events')
        
        # Subscribe to events
        self.event_bus.subscribe(
            topic=self.feature_events_topic,
            callback=self._handle_feature_event,
            consumer_group='quantitative_modeling'
        )
        
        self.event_bus.subscribe(
            topic=self.fundamental_events_topic,
            callback=self._handle_fundamental_event,
            consumer_group='quantitative_modeling'
        )
        
        # Load default models
        self._load_default_models()
        
        self.logger.info("Quantitative model service initialized")
    
    def _load_default_models(self) -> None:
        """Load default models."""
        try:
            # Create a simple default model if none exists
            default_config = ModelConfig(
                name='price_predictor',
                model_type='random_forest',
                version='1.0.0',
                file_path='price_predictor_v1.0.0.joblib',
                features=['tech_sma', 'tech_ema', 'tech_rsi', 'tech_macd_macd', 'fund_revenue'],
                target='price_change',
                parameters={'n_estimators': 100, 'random_state': 42},
                created_at=datetime.utcnow()
            )
            
            # Create and save a simple model if it doesn't exist
            model_path = self.model_manager.models_directory / default_config.file_path
            if not model_path.exists():
                self._create_default_model(default_config)
            
            # Load the model
            self.model_manager.load_model(default_config)
            
        except Exception as e:
            self.logger.error(f"Error loading default models: {e}")
    
    def _create_default_model(self, config: ModelConfig) -> None:
        """Create a default model for demonstration."""
        try:
            if not SKLEARN_AVAILABLE:
                self.logger.warning("scikit-learn not available, cannot create default model")
                return
            
            # Create a simple random forest model
            model = RandomForestRegressor(**config.parameters)
            
            # Create dummy training data
            X_dummy = np.random.randn(100, len(config.features))
            y_dummy = np.random.randn(100)
            
            # Train the model
            model.fit(X_dummy, y_dummy)
            
            # Save the model
            self.model_manager.save_model(model, config)
            
            self.logger.info(f"Created default model: {config.name}")
            
        except Exception as e:
            self.logger.error(f"Error creating default model: {e}")

    def _handle_feature_event(self, message: EventMessage) -> None:
        """
        Handle incoming feature event.

        Args:
            message: Feature event message
        """
        try:
            data = message.data
            symbol = data.get('symbol')

            if symbol:
                self.feature_cache[symbol] = data
                self._try_generate_forecast(symbol)

        except Exception as e:
            self.logger.error(f"Error handling feature event: {e}")

    def _handle_fundamental_event(self, message: EventMessage) -> None:
        """
        Handle incoming fundamental event.

        Args:
            message: Fundamental event message
        """
        try:
            data = message.data
            symbol = data.get('symbol')

            if symbol:
                self.fundamental_cache[symbol] = data
                self._try_generate_forecast(symbol)

        except Exception as e:
            self.logger.error(f"Error handling fundamental event: {e}")

    def _try_generate_forecast(self, symbol: str) -> None:
        """
        Try to generate forecast if sufficient data is available.

        Args:
            symbol: Stock symbol
        """
        try:
            # Check if we have both feature and fundamental data
            if symbol in self.feature_cache and symbol in self.fundamental_cache:
                feature_data = self.feature_cache[symbol]
                fundamental_data = self.fundamental_cache[symbol]

                # Generate forecast
                forecast = self.generate_forecast(symbol, feature_data, fundamental_data)

                if forecast:
                    self._publish_forecast(forecast)

        except Exception as e:
            self.logger.error(f"Error trying to generate forecast for {symbol}: {e}")

    def generate_forecast(self, symbol: str, feature_data: Dict[str, Any],
                         fundamental_data: Dict[str, Any], model_name: str = 'price_predictor') -> Optional[PredictionResult]:
        """
        Generate forecast using specified model.

        Args:
            symbol: Stock symbol
            feature_data: Technical features
            fundamental_data: Fundamental data
            model_name: Name of the model to use

        Returns:
            Prediction result or None
        """
        try:
            # Get model and configuration
            model = self.model_manager.get_model(model_name)
            config = self.model_manager.get_model_config(model_name)

            if not model or not config:
                self.logger.error(f"Model {model_name} not found")
                return None

            # Prepare features
            features = self.feature_processor.prepare_features(
                feature_data, fundamental_data, config.features
            )

            if features is None:
                self.logger.error(f"Failed to prepare features for {symbol}")
                return None

            # Scale features if needed
            scaled_features = self.feature_processor.scale_features(features, model_name)

            # Make prediction
            prediction = model.predict(scaled_features)[0]

            # Calculate confidence interval (simplified)
            confidence_interval = self._calculate_confidence_interval(model, scaled_features)

            # Create feature dictionary for logging
            features_used = {}
            for i, feature_name in enumerate(config.features):
                if i < len(scaled_features[0]):
                    features_used[feature_name] = float(scaled_features[0][i])

            result = PredictionResult(
                symbol=symbol,
                prediction_type=config.target,
                predicted_value=float(prediction),
                confidence_interval=confidence_interval,
                model_name=config.name,
                model_version=config.version,
                features_used=features_used,
                timestamp=datetime.utcnow()
            )

            self.logger.debug(f"Generated forecast for {symbol}: {prediction}")
            return result

        except Exception as e:
            self.logger.error(f"Error generating forecast for {symbol}: {e}")
            return None

    def _calculate_confidence_interval(self, model: Any, features: np.ndarray,
                                     confidence: float = 0.95) -> Optional[Tuple[float, float]]:
        """
        Calculate confidence interval for prediction.

        Args:
            model: Trained model
            features: Feature array
            confidence: Confidence level

        Returns:
            Confidence interval tuple or None
        """
        try:
            # For ensemble models, use prediction variance
            if hasattr(model, 'estimators_'):
                predictions = np.array([estimator.predict(features)[0] for estimator in model.estimators_])
                mean_pred = np.mean(predictions)
                std_pred = np.std(predictions)

                # Calculate confidence interval
                z_score = 1.96 if confidence == 0.95 else 2.576  # Simplified
                margin = z_score * std_pred

                return (mean_pred - margin, mean_pred + margin)

            # For other models, return None (would need more sophisticated approach)
            return None

        except Exception as e:
            self.logger.error(f"Error calculating confidence interval: {e}")
            return None

    def _publish_forecast(self, forecast: PredictionResult) -> None:
        """
        Publish forecast to event bus.

        Args:
            forecast: Prediction result
        """
        try:
            message = {
                'symbol': forecast.symbol,
                'prediction_type': forecast.prediction_type,
                'predicted_value': forecast.predicted_value,
                'confidence_interval': forecast.confidence_interval,
                'model_name': forecast.model_name,
                'model_version': forecast.model_version,
                'features_used': forecast.features_used,
                'timestamp': forecast.timestamp.isoformat()
            }

            self.event_bus.publish(
                topic=self.forecast_events_topic,
                message=message,
                event_type='forecast'
            )

            # Save to document database
            self.database_manager.save_document('forecasts', message)

            self.logger.debug(f"Published forecast for {forecast.symbol}")

        except Exception as e:
            self.logger.error(f"Error publishing forecast: {e}")

    def train_model(self, model_config: ModelConfig, training_data: pd.DataFrame) -> bool:
        """
        Train a new model with provided data.

        Args:
            model_config: Model configuration
            training_data: Training dataset

        Returns:
            True if training successful
        """
        try:
            if not SKLEARN_AVAILABLE:
                self.logger.error("scikit-learn not available for model training")
                return False

            # Prepare features and target
            X = training_data[model_config.features].values
            y = training_data[model_config.target].values

            # Create model based on type
            if model_config.model_type == 'random_forest':
                model = RandomForestRegressor(**model_config.parameters)
            elif model_config.model_type == 'gradient_boosting':
                model = GradientBoostingRegressor(**model_config.parameters)
            elif model_config.model_type == 'linear':
                model = LinearRegression(**model_config.parameters)
            elif model_config.model_type == 'xgboost' and XGBOOST_AVAILABLE:
                model = xgb.XGBRegressor(**model_config.parameters)
            else:
                self.logger.error(f"Unsupported model type: {model_config.model_type}")
                return False

            # Train model
            model.fit(X, y)

            # Calculate performance metrics
            y_pred = model.predict(X)
            metrics = {
                'mse': mean_squared_error(y, y_pred),
                'mae': mean_absolute_error(y, y_pred),
                'r2': r2_score(y, y_pred)
            }

            model_config.performance_metrics = metrics
            model_config.last_updated = datetime.utcnow()

            # Save model
            success = self.model_manager.save_model(model, model_config)

            if success:
                # Load the trained model
                self.model_manager.load_model(model_config)
                self.logger.info(f"Successfully trained model: {model_config.name}")

            return success

        except Exception as e:
            self.logger.error(f"Error training model {model_config.name}: {e}")
            return False

    def get_model_performance(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Get performance metrics for a model.

        Args:
            model_name: Name of the model

        Returns:
            Performance metrics or None
        """
        config = self.model_manager.get_model_config(model_name)
        if config and config.performance_metrics:
            return {
                'model_name': config.name,
                'model_version': config.version,
                'metrics': config.performance_metrics,
                'last_updated': config.last_updated.isoformat() if config.last_updated else None
            }
        return None

    def list_available_models(self) -> List[Dict[str, Any]]:
        """
        List all available models.

        Returns:
            List of model information
        """
        models = []

        for model_key, config in self.model_manager.model_configs.items():
            models.append({
                'name': config.name,
                'version': config.version,
                'model_type': config.model_type,
                'features': config.features,
                'target': config.target,
                'performance_metrics': config.performance_metrics,
                'created_at': config.created_at.isoformat() if config.created_at else None,
                'last_updated': config.last_updated.isoformat() if config.last_updated else None
            })

        return models

    def stop(self) -> None:
        """Stop the quantitative model service."""
        self.event_bus.close()
        self.logger.info("Quantitative model service stopped")


def main():
    """Main entry point for the quantitative model service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    quant_model = QuantitativeModel()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down quantitative model service...")
    finally:
        quant_model.stop()


if __name__ == "__main__":
    main()

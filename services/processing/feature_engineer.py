"""
Feature Engineer

Calculates quantitative indicators from raw market data with high-performance
in-memory data management and configurable feature sets.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Deque
from datetime import datetime, timezone
from dataclasses import dataclass
from collections import deque, defaultdict
import threading
import time

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class FeatureSet:
    """Configuration for a set of features to calculate."""
    name: str
    indicators: List[str]
    window_sizes: Dict[str, int]
    parameters: Dict[str, Any]


class TechnicalIndicators:
    """Collection of technical indicator calculations."""
    
    @staticmethod
    def sma(prices: np.ndarray, window: int) -> float:
        """Simple Moving Average."""
        if len(prices) < window:
            return np.nan
        return np.mean(prices[-window:])
    
    @staticmethod
    def ema(prices: np.ndarray, window: int, alpha: Optional[float] = None) -> float:
        """Exponential Moving Average."""
        if len(prices) < window:
            return np.nan
        
        if alpha is None:
            alpha = 2.0 / (window + 1)
        
        ema_value = prices[0]
        for price in prices[1:]:
            ema_value = alpha * price + (1 - alpha) * ema_value
        
        return ema_value
    
    @staticmethod
    def rsi(prices: np.ndarray, window: int = 14) -> float:
        """Relative Strength Index."""
        if len(prices) < window + 1:
            return np.nan
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-window:])
        avg_loss = np.mean(losses[-window:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, float]:
        """MACD (Moving Average Convergence Divergence)."""
        if len(prices) < slow:
            return {'macd': np.nan, 'signal': np.nan, 'histogram': np.nan}
        
        ema_fast = TechnicalIndicators.ema(prices, fast)
        ema_slow = TechnicalIndicators.ema(prices, slow)
        macd_line = ema_fast - ema_slow
        
        # For signal line, we'd need historical MACD values
        # Simplified implementation
        signal_line = macd_line  # In practice, this would be EMA of MACD
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    @staticmethod
    def bollinger_bands(prices: np.ndarray, window: int = 20, num_std: float = 2.0) -> Dict[str, float]:
        """Bollinger Bands."""
        if len(prices) < window:
            return {'upper': np.nan, 'middle': np.nan, 'lower': np.nan}
        
        sma = TechnicalIndicators.sma(prices, window)
        std = np.std(prices[-window:])
        
        upper = sma + (num_std * std)
        lower = sma - (num_std * std)
        
        return {
            'upper': upper,
            'middle': sma,
            'lower': lower
        }
    
    @staticmethod
    def vwap(prices: np.ndarray, volumes: np.ndarray) -> float:
        """Volume Weighted Average Price."""
        if len(prices) != len(volumes) or len(prices) == 0:
            return np.nan
        
        return np.sum(prices * volumes) / np.sum(volumes)
    
    @staticmethod
    def atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, window: int = 14) -> float:
        """Average True Range."""
        if len(highs) < window + 1 or len(lows) < window + 1 or len(closes) < window + 1:
            return np.nan
        
        # Calculate True Range
        tr_values = []
        for i in range(1, len(closes)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_values.append(max(tr1, tr2, tr3))
        
        if len(tr_values) < window:
            return np.nan
        
        return np.mean(tr_values[-window:])


class DataWindow:
    """Manages rolling time windows of market data for a symbol."""
    
    def __init__(self, symbol: str, max_size: int = 1000):
        """
        Initialize data window.
        
        Args:
            symbol: Stock symbol
            max_size: Maximum number of data points to keep
        """
        self.symbol = symbol
        self.max_size = max_size
        
        # Price data
        self.timestamps: Deque[datetime] = deque(maxlen=max_size)
        self.opens: Deque[float] = deque(maxlen=max_size)
        self.highs: Deque[float] = deque(maxlen=max_size)
        self.lows: Deque[float] = deque(maxlen=max_size)
        self.closes: Deque[float] = deque(maxlen=max_size)
        self.volumes: Deque[float] = deque(maxlen=max_size)
        
        # Current bar aggregation
        self.current_bar = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': 0,
            'timestamp': None
        }
        
        self.lock = threading.Lock()
    
    def add_tick(self, timestamp: datetime, price: float, volume: float) -> bool:
        """
        Add a new tick to the data window.
        
        Args:
            timestamp: Tick timestamp
            price: Tick price
            volume: Tick volume
            
        Returns:
            True if a new bar was completed
        """
        with self.lock:
            # Initialize current bar if needed
            if self.current_bar['timestamp'] is None:
                self.current_bar = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'timestamp': timestamp
                }
                return False
            
            # Check if we need to start a new bar (1-minute bars)
            current_minute = self.current_bar['timestamp'].replace(second=0, microsecond=0)
            tick_minute = timestamp.replace(second=0, microsecond=0)
            
            if tick_minute > current_minute:
                # Complete the current bar
                self._complete_current_bar()
                
                # Start new bar
                self.current_bar = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'timestamp': timestamp
                }
                return True
            else:
                # Update current bar
                self.current_bar['high'] = max(self.current_bar['high'], price)
                self.current_bar['low'] = min(self.current_bar['low'], price)
                self.current_bar['close'] = price
                self.current_bar['volume'] += volume
                return False
    
    def _complete_current_bar(self) -> None:
        """Complete the current bar and add it to the window."""
        self.timestamps.append(self.current_bar['timestamp'])
        self.opens.append(self.current_bar['open'])
        self.highs.append(self.current_bar['high'])
        self.lows.append(self.current_bar['low'])
        self.closes.append(self.current_bar['close'])
        self.volumes.append(self.current_bar['volume'])
    
    def get_arrays(self) -> Dict[str, np.ndarray]:
        """Get numpy arrays of the data."""
        with self.lock:
            return {
                'timestamps': np.array(list(self.timestamps)),
                'opens': np.array(list(self.opens)),
                'highs': np.array(list(self.highs)),
                'lows': np.array(list(self.lows)),
                'closes': np.array(list(self.closes)),
                'volumes': np.array(list(self.volumes))
            }
    
    def has_sufficient_data(self, min_periods: int) -> bool:
        """Check if window has sufficient data for calculations."""
        with self.lock:
            return len(self.closes) >= min_periods


class FeatureEngineer:
    """
    Feature engineer with high-performance data management.
    
    Features:
    - Efficient in-memory data windows
    - Configurable feature sets
    - Real-time indicator calculations
    - Multi-symbol support
    """
    
    def __init__(self, service_name: str = "feature_engineer"):
        """
        Initialize feature engineer.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Data management
        self.data_windows: Dict[str, DataWindow] = {}
        self.feature_sets = self._load_feature_sets()
        
        # Topic names
        self.raw_market_data_topic = get_topic_name('data_ingestion', 'raw_market_data')
        self.feature_events_topic = get_topic_name('processing', 'feature_events')
        
        # Subscribe to market data
        self.event_bus.subscribe(
            topic=self.raw_market_data_topic,
            callback=self._handle_market_data,
            consumer_group='feature_engineering'
        )
        
        self.logger.info("Feature engineer initialized")
    
    def _load_feature_sets(self) -> List[FeatureSet]:
        """Load feature set configurations."""
        # Default feature set - in production, this would come from configuration
        default_features = FeatureSet(
            name='default',
            indicators=['sma', 'ema', 'rsi', 'macd', 'bollinger_bands', 'vwap', 'atr'],
            window_sizes={
                'sma': 20,
                'ema': 12,
                'rsi': 14,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9,
                'bollinger': 20,
                'atr': 14
            },
            parameters={
                'bollinger_std': 2.0,
                'ema_alpha': None
            }
        )
        
        return [default_features]

    def _handle_market_data(self, message: EventMessage) -> None:
        """
        Handle incoming market data message.

        Args:
            message: Market data event message
        """
        try:
            data = message.data
            symbol = data.get('symbol')

            if not symbol:
                self.logger.warning("Market data message missing symbol")
                return

            # Parse timestamp
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.utcnow()

            price = float(data.get('price', 0))
            volume = float(data.get('volume', 0))

            # Get or create data window for symbol
            if symbol not in self.data_windows:
                self.data_windows[symbol] = DataWindow(symbol)

            data_window = self.data_windows[symbol]

            # Add tick to data window
            bar_completed = data_window.add_tick(timestamp, price, volume)

            # Calculate features if a bar was completed
            if bar_completed:
                self._calculate_features(symbol, data_window)

        except Exception as e:
            self.logger.error(f"Error handling market data: {e}")

    def _calculate_features(self, symbol: str, data_window: DataWindow) -> None:
        """
        Calculate features for a symbol.

        Args:
            symbol: Stock symbol
            data_window: Data window for the symbol
        """
        try:
            # Get data arrays
            arrays = data_window.get_arrays()

            if len(arrays['closes']) == 0:
                return

            # Calculate features for each feature set
            for feature_set in self.feature_sets:
                features = self.calculate_indicators(arrays, feature_set)

                if features:
                    self._publish_features(symbol, features, feature_set.name)

        except Exception as e:
            self.logger.error(f"Error calculating features for {symbol}: {e}")

    def calculate_indicators(self, arrays: Dict[str, np.ndarray], feature_set: FeatureSet) -> Optional[Dict[str, Any]]:
        """
        Calculate technical indicators for given data arrays.

        Args:
            arrays: Data arrays (opens, highs, lows, closes, volumes)
            feature_set: Feature set configuration

        Returns:
            Dictionary of calculated features or None
        """
        try:
            closes = arrays['closes']
            highs = arrays['highs']
            lows = arrays['lows']
            volumes = arrays['volumes']

            if len(closes) == 0:
                return None

            features = {}

            # Calculate each indicator
            for indicator in feature_set.indicators:
                try:
                    if indicator == 'sma':
                        window = feature_set.window_sizes.get('sma', 20)
                        if len(closes) >= window:
                            features['sma'] = TechnicalIndicators.sma(closes, window)

                    elif indicator == 'ema':
                        window = feature_set.window_sizes.get('ema', 12)
                        alpha = feature_set.parameters.get('ema_alpha')
                        if len(closes) >= window:
                            features['ema'] = TechnicalIndicators.ema(closes, window, alpha)

                    elif indicator == 'rsi':
                        window = feature_set.window_sizes.get('rsi', 14)
                        if len(closes) >= window + 1:
                            features['rsi'] = TechnicalIndicators.rsi(closes, window)

                    elif indicator == 'macd':
                        fast = feature_set.window_sizes.get('macd_fast', 12)
                        slow = feature_set.window_sizes.get('macd_slow', 26)
                        signal = feature_set.window_sizes.get('macd_signal', 9)
                        if len(closes) >= slow:
                            macd_result = TechnicalIndicators.macd(closes, fast, slow, signal)
                            features.update({f'macd_{k}': v for k, v in macd_result.items()})

                    elif indicator == 'bollinger_bands':
                        window = feature_set.window_sizes.get('bollinger', 20)
                        num_std = feature_set.parameters.get('bollinger_std', 2.0)
                        if len(closes) >= window:
                            bb_result = TechnicalIndicators.bollinger_bands(closes, window, num_std)
                            features.update({f'bb_{k}': v for k, v in bb_result.items()})

                    elif indicator == 'vwap':
                        if len(closes) > 0 and len(volumes) > 0:
                            features['vwap'] = TechnicalIndicators.vwap(closes, volumes)

                    elif indicator == 'atr':
                        window = feature_set.window_sizes.get('atr', 14)
                        if len(highs) >= window + 1:
                            features['atr'] = TechnicalIndicators.atr(highs, lows, closes, window)

                except Exception as e:
                    self.logger.error(f"Error calculating {indicator}: {e}")

            # Add metadata
            features['timestamp'] = datetime.utcnow().isoformat()
            features['data_points'] = len(closes)
            features['feature_set'] = feature_set.name

            return features

        except Exception as e:
            self.logger.error(f"Error in calculate_indicators: {e}")
            return None

    def _publish_features(self, symbol: str, features: Dict[str, Any], feature_set_name: str) -> None:
        """
        Publish calculated features to event bus.

        Args:
            symbol: Stock symbol
            features: Calculated features
            feature_set_name: Name of the feature set
        """
        try:
            message = {
                'symbol': symbol,
                'features': features,
                'feature_set': feature_set_name,
                'timestamp': datetime.utcnow().isoformat()
            }

            self.event_bus.publish(
                topic=self.feature_events_topic,
                message=message,
                event_type='feature_calculation'
            )

            self.logger.debug(f"Published features for {symbol} ({feature_set_name})")

        except Exception as e:
            self.logger.error(f"Error publishing features for {symbol}: {e}")

    def get_latest_features(self, symbol: str, feature_set_name: str = 'default') -> Optional[Dict[str, Any]]:
        """
        Get the latest calculated features for a symbol.

        Args:
            symbol: Stock symbol
            feature_set_name: Name of the feature set

        Returns:
            Latest features or None
        """
        try:
            if symbol not in self.data_windows:
                return None

            data_window = self.data_windows[symbol]
            arrays = data_window.get_arrays()

            # Find the feature set
            feature_set = None
            for fs in self.feature_sets:
                if fs.name == feature_set_name:
                    feature_set = fs
                    break

            if not feature_set:
                return None

            return self.calculate_indicators(arrays, feature_set)

        except Exception as e:
            self.logger.error(f"Error getting latest features for {symbol}: {e}")
            return None

    def add_feature_set(self, feature_set: FeatureSet) -> None:
        """
        Add a new feature set configuration.

        Args:
            feature_set: Feature set to add
        """
        self.feature_sets.append(feature_set)
        self.logger.info(f"Added feature set: {feature_set.name}")

    def get_data_window_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all data windows.

        Returns:
            Dictionary with data window information
        """
        info = {}

        for symbol, window in self.data_windows.items():
            arrays = window.get_arrays()
            info[symbol] = {
                'data_points': len(arrays['closes']),
                'latest_timestamp': arrays['timestamps'][-1] if len(arrays['timestamps']) > 0 else None,
                'latest_price': arrays['closes'][-1] if len(arrays['closes']) > 0 else None
            }

        return info

    def stop(self) -> None:
        """Stop the feature engineer."""
        self.event_bus.close()
        self.logger.info("Feature engineer stopped")


def main():
    """Main entry point for the feature engineer service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    engineer = FeatureEngineer()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down feature engineer...")
    finally:
        engineer.stop()


if __name__ == "__main__":
    main()

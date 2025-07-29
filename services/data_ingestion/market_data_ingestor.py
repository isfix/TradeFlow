"""
Market Data Ingestor

Provides continuous, real-time stream of market data with institutional-grade
resilience, reconnection logic, and data validation.
"""

import asyncio
import json
import logging
import time
import websocket
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone
from dataclasses import dataclass
import requests
from urllib.parse import urljoin

from ...core.event_bus import get_event_bus, initialize_event_bus
from ...core.database_clients import get_database_manager, MarketData
from ...configs import load_services_config, get_topic_name


@dataclass
class ConnectionConfig:
    """Configuration for data source connection."""
    name: str
    api_key: str
    base_url: str
    websocket_url: str
    rate_limit: int
    timeout: int
    symbols: List[str]


class HeartbeatManager:
    """Manages heartbeat mechanism for connection monitoring."""
    
    def __init__(self, interval: int = 30, timeout: int = 60):
        """
        Initialize heartbeat manager.
        
        Args:
            interval: Heartbeat interval in seconds
            timeout: Timeout for heartbeat response in seconds
        """
        self.interval = interval
        self.timeout = timeout
        self.last_heartbeat = time.time()
        self.is_alive = True
        self.logger = logging.getLogger(f"{__name__}.HeartbeatManager")
    
    def update_heartbeat(self) -> None:
        """Update the last heartbeat timestamp."""
        self.last_heartbeat = time.time()
        self.is_alive = True
    
    def check_heartbeat(self) -> bool:
        """
        Check if connection is alive based on heartbeat.
        
        Returns:
            True if connection is alive, False otherwise
        """
        current_time = time.time()
        if current_time - self.last_heartbeat > self.timeout:
            self.is_alive = False
            self.logger.warning("Heartbeat timeout detected")
            return False
        return True


class ReconnectionManager:
    """Manages reconnection logic with exponential backoff."""
    
    def __init__(self, max_retries: int = 10, base_delay: float = 1.0, max_delay: float = 300.0):
        """
        Initialize reconnection manager.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_count = 0
        self.logger = logging.getLogger(f"{__name__}.ReconnectionManager")
    
    def get_delay(self) -> float:
        """
        Get the delay for the current retry attempt using exponential backoff.
        
        Returns:
            Delay in seconds
        """
        delay = min(self.base_delay * (2 ** self.retry_count), self.max_delay)
        return delay
    
    def should_retry(self) -> bool:
        """
        Check if should attempt to retry.
        
        Returns:
            True if should retry, False otherwise
        """
        return self.retry_count < self.max_retries
    
    def increment_retry(self) -> None:
        """Increment retry count."""
        self.retry_count += 1
        self.logger.info(f"Retry attempt {self.retry_count}/{self.max_retries}")
    
    def reset(self) -> None:
        """Reset retry count after successful connection."""
        self.retry_count = 0
        self.logger.info("Reconnection manager reset after successful connection")


class DataValidator:
    """Validates incoming market data."""
    
    def __init__(self):
        """Initialize data validator."""
        self.logger = logging.getLogger(f"{__name__}.DataValidator")
    
    def validate_market_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate market data structure and values.
        
        Args:
            data: Raw market data
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            # Check required fields
            required_fields = ['symbol', 'price', 'timestamp']
            for field in required_fields:
                if field not in data:
                    self.logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate data types and ranges
            if not isinstance(data['symbol'], str) or len(data['symbol']) == 0:
                self.logger.warning("Invalid symbol")
                return False
            
            if not isinstance(data['price'], (int, float)) or data['price'] <= 0:
                self.logger.warning("Invalid price")
                return False
            
            if 'volume' in data and (not isinstance(data['volume'], (int, float)) or data['volume'] < 0):
                self.logger.warning("Invalid volume")
                return False
            
            # Validate timestamp
            if isinstance(data['timestamp'], str):
                try:
                    datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                except ValueError:
                    self.logger.warning("Invalid timestamp format")
                    return False
            elif isinstance(data['timestamp'], (int, float)):
                if data['timestamp'] <= 0:
                    self.logger.warning("Invalid timestamp value")
                    return False
            else:
                self.logger.warning("Invalid timestamp type")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating market data: {e}")
            return False


class MarketDataIngestor:
    """
    Market data ingestor with institutional-grade resilience.
    
    Features:
    - Persistent WebSocket connections with heartbeat monitoring
    - Exponential backoff reconnection strategy
    - Data validation and normalization
    - Multiple data source support
    """
    
    def __init__(self, service_name: str = "market_data_ingestor"):
        """
        Initialize market data ingestor.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Connection management
        self.connections: Dict[str, websocket.WebSocketApp] = {}
        self.heartbeat_managers: Dict[str, HeartbeatManager] = {}
        self.reconnection_managers: Dict[str, ReconnectionManager] = {}
        self.data_validator = DataValidator()
        
        # Control flags
        self.running = False
        self.connection_threads: Dict[str, threading.Thread] = {}
        
        # Topic names
        self.raw_market_data_topic = get_topic_name('data_ingestion', 'raw_market_data')
    
    def connect_and_stream(self, symbols: List[str]) -> None:
        """
        Connect to data sources and start streaming.
        
        Args:
            symbols: List of symbols to subscribe to
        """
        self.running = True
        
        # Connect to configured data sources
        market_data_config = self.config.get('market_data', {})
        
        for source_name, source_config in market_data_config.items():
            if 'websocket_url' in source_config:
                connection_config = ConnectionConfig(
                    name=source_name,
                    api_key=source_config['api_key'],
                    base_url=source_config['base_url'],
                    websocket_url=source_config['websocket_url'],
                    rate_limit=source_config.get('rate_limit', 60),
                    timeout=source_config.get('timeout', 30),
                    symbols=symbols
                )
                
                self._start_connection(connection_config)
    
    def _start_connection(self, config: ConnectionConfig) -> None:
        """
        Start connection to a data source.
        
        Args:
            config: Connection configuration
        """
        self.heartbeat_managers[config.name] = HeartbeatManager()
        self.reconnection_managers[config.name] = ReconnectionManager()
        
        # Start connection thread
        connection_thread = threading.Thread(
            target=self._connection_worker,
            args=(config,),
            daemon=True
        )
        connection_thread.start()
        self.connection_threads[config.name] = connection_thread
        
        self.logger.info(f"Started connection thread for {config.name}")
    
    def _connection_worker(self, config: ConnectionConfig) -> None:
        """
        Worker thread for managing connection to a data source.
        
        Args:
            config: Connection configuration
        """
        reconnection_manager = self.reconnection_managers[config.name]
        
        while self.running and reconnection_manager.should_retry():
            try:
                self._establish_websocket_connection(config)
                reconnection_manager.reset()
                
            except Exception as e:
                self.logger.error(f"Connection error for {config.name}: {e}")
                
                if reconnection_manager.should_retry():
                    delay = reconnection_manager.get_delay()
                    self.logger.info(f"Reconnecting to {config.name} in {delay} seconds")
                    time.sleep(delay)
                    reconnection_manager.increment_retry()
                else:
                    self.logger.error(f"Max retries exceeded for {config.name}")
                    break

    def _establish_websocket_connection(self, config: ConnectionConfig) -> None:
        """
        Establish WebSocket connection to data source.

        Args:
            config: Connection configuration
        """
        def on_message(ws, message):
            self._handle_message(config.name, message)

        def on_error(ws, error):
            self.logger.error(f"WebSocket error for {config.name}: {error}")

        def on_close(ws, close_status_code, close_msg):
            self.logger.warning(f"WebSocket closed for {config.name}: {close_status_code} - {close_msg}")

        def on_open(ws):
            self.logger.info(f"WebSocket connected to {config.name}")
            self.heartbeat_managers[config.name].update_heartbeat()

            # Subscribe to symbols
            self._subscribe_to_symbols(ws, config)

        # Create WebSocket connection
        ws_url = f"{config.websocket_url}?apikey={config.api_key}"

        ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.connections[config.name] = ws

        # Start WebSocket connection (blocking call)
        ws.run_forever()

    def _subscribe_to_symbols(self, ws: websocket.WebSocketApp, config: ConnectionConfig) -> None:
        """
        Subscribe to symbols on WebSocket connection.

        Args:
            ws: WebSocket connection
            config: Connection configuration
        """
        try:
            if config.name == 'polygon':
                # Polygon.io subscription format
                for symbol in config.symbols:
                    subscribe_msg = {
                        "action": "subscribe",
                        "params": f"T.{symbol}"  # Trade data
                    }
                    ws.send(json.dumps(subscribe_msg))

                    subscribe_msg = {
                        "action": "subscribe",
                        "params": f"Q.{symbol}"  # Quote data
                    }
                    ws.send(json.dumps(subscribe_msg))

            elif config.name == 'finnhub':
                # Finnhub subscription format
                for symbol in config.symbols:
                    subscribe_msg = {
                        "type": "subscribe",
                        "symbol": symbol
                    }
                    ws.send(json.dumps(subscribe_msg))

            self.logger.info(f"Subscribed to {len(config.symbols)} symbols on {config.name}")

        except Exception as e:
            self.logger.error(f"Error subscribing to symbols on {config.name}: {e}")

    def _handle_message(self, source_name: str, message: str) -> None:
        """
        Handle incoming WebSocket message.

        Args:
            source_name: Name of the data source
            message: Raw message string
        """
        try:
            # Update heartbeat
            self.heartbeat_managers[source_name].update_heartbeat()

            # Parse message
            data = json.loads(message)

            # Handle different message types
            if isinstance(data, list):
                # Multiple data points
                for item in data:
                    self._process_data_point(source_name, item)
            elif isinstance(data, dict):
                # Single data point
                self._process_data_point(source_name, data)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON message from {source_name}: {e}")
        except Exception as e:
            self.logger.error(f"Error handling message from {source_name}: {e}")

    def _process_data_point(self, source_name: str, data: Dict[str, Any]) -> None:
        """
        Process a single data point.

        Args:
            source_name: Name of the data source
            data: Data point
        """
        try:
            # Normalize data based on source
            normalized_data = self._normalize_data(source_name, data)

            if normalized_data and self.data_validator.validate_market_data(normalized_data):
                # Create MarketData object
                market_data = MarketData(
                    symbol=normalized_data['symbol'],
                    timestamp=self._parse_timestamp(normalized_data['timestamp']),
                    price=float(normalized_data['price']),
                    volume=float(normalized_data.get('volume', 0)),
                    bid=float(normalized_data['bid']) if normalized_data.get('bid') else None,
                    ask=float(normalized_data['ask']) if normalized_data.get('ask') else None,
                    bid_size=float(normalized_data['bid_size']) if normalized_data.get('bid_size') else None,
                    ask_size=float(normalized_data['ask_size']) if normalized_data.get('ask_size') else None,
                    metadata={'source': source_name}
                )

                # Publish to event bus
                self.event_bus.publish(
                    topic=self.raw_market_data_topic,
                    message={
                        'symbol': market_data.symbol,
                        'timestamp': market_data.timestamp.isoformat(),
                        'price': market_data.price,
                        'volume': market_data.volume,
                        'bid': market_data.bid,
                        'ask': market_data.ask,
                        'bid_size': market_data.bid_size,
                        'ask_size': market_data.ask_size,
                        'source': source_name
                    },
                    event_type='market_data'
                )

                # Save to time-series database
                self.database_manager.save_to_timeseries(market_data)

                self.logger.debug(f"Processed market data for {market_data.symbol} from {source_name}")

        except Exception as e:
            self.logger.error(f"Error processing data point from {source_name}: {e}")

    def _normalize_data(self, source_name: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize data from different sources to standard format.

        Args:
            source_name: Name of the data source
            data: Raw data

        Returns:
            Normalized data or None if not applicable
        """
        try:
            if source_name == 'polygon':
                # Polygon.io format
                if data.get('ev') == 'T':  # Trade data
                    return {
                        'symbol': data.get('sym'),
                        'price': data.get('p'),
                        'volume': data.get('s'),
                        'timestamp': data.get('t')
                    }
                elif data.get('ev') == 'Q':  # Quote data
                    return {
                        'symbol': data.get('sym'),
                        'price': (data.get('bp', 0) + data.get('ap', 0)) / 2,  # Mid price
                        'volume': 0,
                        'bid': data.get('bp'),
                        'ask': data.get('ap'),
                        'bid_size': data.get('bs'),
                        'ask_size': data.get('as'),
                        'timestamp': data.get('t')
                    }

            elif source_name == 'finnhub':
                # Finnhub format
                if 'data' in data:
                    for trade in data['data']:
                        return {
                            'symbol': trade.get('s'),
                            'price': trade.get('p'),
                            'volume': trade.get('v'),
                            'timestamp': trade.get('t')
                        }

            return None

        except Exception as e:
            self.logger.error(f"Error normalizing data from {source_name}: {e}")
            return None

    def _parse_timestamp(self, timestamp: Any) -> datetime:
        """
        Parse timestamp to datetime object.

        Args:
            timestamp: Timestamp in various formats

        Returns:
            Datetime object in UTC
        """
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif isinstance(timestamp, (int, float)):
            # Assume Unix timestamp in milliseconds
            return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        else:
            return datetime.utcnow()

    def stop(self) -> None:
        """Stop all connections and cleanup."""
        self.running = False

        # Close WebSocket connections
        for ws in self.connections.values():
            ws.close()

        # Wait for threads to finish
        for thread in self.connection_threads.values():
            thread.join(timeout=5.0)

        # Close event bus
        self.event_bus.close()

        self.logger.info("Market data ingestor stopped")


def main():
    """Main entry point for the market data ingestor service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    ingestor = MarketDataIngestor()

    try:
        # Example symbols - in production, this would come from configuration
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        ingestor.connect_and_stream(symbols)

        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down market data ingestor...")
    finally:
        ingestor.stop()


if __name__ == "__main__":
    main()

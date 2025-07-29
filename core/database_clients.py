"""
Database Clients

Manages all database connections and provides standardized functions
for reading from and writing to different types of databases.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
from dataclasses import dataclass
import json

# Database-specific imports
try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    from psycopg2.extras import RealDictCursor
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUXDB_AVAILABLE = True
except ImportError:
    INFLUXDB_AVAILABLE = False

try:
    import pymongo
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from ..configs import load_database_config


@dataclass
class TradeData:
    """Standard trade data structure."""
    trade_id: str
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: float
    price: float
    timestamp: datetime
    order_id: str
    execution_id: str
    commission: float
    source_service: str
    correlation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class MarketData:
    """Standard market data structure."""
    symbol: str
    timestamp: datetime
    price: float
    volume: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class DatabaseClientInterface(ABC):
    """Abstract interface for database clients."""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if database connection is healthy."""
        pass


class PostgreSQLClient(DatabaseClientInterface):
    """PostgreSQL client for trade ledger and relational data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL client.
        
        Args:
            config: Database configuration
        """
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2 is required for PostgreSQL client")
        
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.PostgreSQL")
        self.connection_pool: Optional[ThreadedConnectionPool] = None
    
    def connect(self) -> bool:
        """Establish connection pool to PostgreSQL."""
        try:
            self.connection_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=self.config.get('pool_size', 20),
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password'],
                sslmode=self.config.get('ssl_mode', 'prefer')
            )
            
            # Test connection
            conn = self.connection_pool.getconn()
            conn.close()
            self.connection_pool.putconn(conn)
            
            self.logger.info("PostgreSQL connection pool established")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.logger.info("PostgreSQL connection pool closed")
    
    def health_check(self) -> bool:
        """Check PostgreSQL connection health."""
        try:
            if not self.connection_pool:
                return False
            
            conn = self.connection_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            self.connection_pool.putconn(conn)
            return True
            
        except Exception as e:
            self.logger.error(f"PostgreSQL health check failed: {e}")
            return False
    
    def log_trade(self, trade_data: TradeData) -> bool:
        """
        Log a trade to the trade ledger.
        
        Args:
            trade_data: Trade information
            
        Returns:
            True if trade was logged successfully
        """
        try:
            conn = self.connection_pool.getconn()
            cursor = conn.cursor()
            
            # Create trades table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id VARCHAR(255) PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    quantity DECIMAL(20, 8) NOT NULL,
                    price DECIMAL(20, 8) NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    order_id VARCHAR(255) NOT NULL,
                    execution_id VARCHAR(255) NOT NULL,
                    commission DECIMAL(20, 8) NOT NULL,
                    source_service VARCHAR(100) NOT NULL,
                    correlation_id VARCHAR(255),
                    metadata JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Insert trade
            cursor.execute("""
                INSERT INTO trades (
                    trade_id, symbol, side, quantity, price, timestamp,
                    order_id, execution_id, commission, source_service,
                    correlation_id, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                trade_data.trade_id,
                trade_data.symbol,
                trade_data.side,
                trade_data.quantity,
                trade_data.price,
                trade_data.timestamp,
                trade_data.order_id,
                trade_data.execution_id,
                trade_data.commission,
                trade_data.source_service,
                trade_data.correlation_id,
                json.dumps(trade_data.metadata) if trade_data.metadata else None
            ))
            
            conn.commit()
            cursor.close()
            self.connection_pool.putconn(conn)
            
            self.logger.debug(f"Logged trade {trade_data.trade_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log trade {trade_data.trade_id}: {e}")
            if conn:
                conn.rollback()
                self.connection_pool.putconn(conn)
            return False


class InfluxDBClient(DatabaseClientInterface):
    """InfluxDB client for time-series data."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize InfluxDB client.

        Args:
            config: Database configuration
        """
        if not INFLUXDB_AVAILABLE:
            raise ImportError("influxdb-client is required for InfluxDB client")

        self.config = config
        self.logger = logging.getLogger(f"{__name__}.InfluxDB")
        self.client: Optional[InfluxDBClient] = None
        self.write_api = None
        self.query_api = None

    def connect(self) -> bool:
        """Establish connection to InfluxDB."""
        try:
            url = f"http://{self.config['host']}:{self.config['port']}"

            self.client = InfluxDBClient(
                url=url,
                token=self.config.get('token'),
                org=self.config.get('org'),
                timeout=self.config.get('timeout', 30) * 1000  # Convert to milliseconds
            )

            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()

            # Test connection
            self.client.ping()

            self.logger.info("InfluxDB connection established")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to InfluxDB: {e}")
            return False

    def disconnect(self) -> None:
        """Close InfluxDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("InfluxDB connection closed")

    def health_check(self) -> bool:
        """Check InfluxDB connection health."""
        try:
            if not self.client:
                return False

            self.client.ping()
            return True

        except Exception as e:
            self.logger.error(f"InfluxDB health check failed: {e}")
            return False

    def save_to_timeseries(self, market_data: MarketData) -> bool:
        """
        Save market data to time-series database.

        Args:
            market_data: Market data to save

        Returns:
            True if data was saved successfully
        """
        try:
            point = Point("market_data") \
                .tag("symbol", market_data.symbol) \
                .field("price", market_data.price) \
                .field("volume", market_data.volume) \
                .time(market_data.timestamp, WritePrecision.NS)

            # Add optional fields
            if market_data.bid is not None:
                point = point.field("bid", market_data.bid)
            if market_data.ask is not None:
                point = point.field("ask", market_data.ask)
            if market_data.bid_size is not None:
                point = point.field("bid_size", market_data.bid_size)
            if market_data.ask_size is not None:
                point = point.field("ask_size", market_data.ask_size)

            # Add metadata as tags
            if market_data.metadata:
                for key, value in market_data.metadata.items():
                    if isinstance(value, (str, int, float, bool)):
                        point = point.tag(key, str(value))

            self.write_api.write(
                bucket=self.config['database'],
                org=self.config['org'],
                record=point
            )

            self.logger.debug(f"Saved market data for {market_data.symbol}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save market data for {market_data.symbol}: {e}")
            return False


class MongoDBClient(DatabaseClientInterface):
    """MongoDB client for document storage."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MongoDB client.

        Args:
            config: Database configuration
        """
        if not MONGODB_AVAILABLE:
            raise ImportError("pymongo is required for MongoDB client")

        self.config = config
        self.logger = logging.getLogger(f"{__name__}.MongoDB")
        self.client: Optional[MongoClient] = None
        self.database = None

    def connect(self) -> bool:
        """Establish connection to MongoDB."""
        try:
            connection_string = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"

            self.client = MongoClient(
                connection_string,
                authSource=self.config.get('auth_source', 'admin'),
                serverSelectionTimeoutMS=self.config.get('timeout', 30) * 1000,
                maxPoolSize=self.config.get('max_pool_size', 100)
            )

            self.database = self.client[self.config['database']]

            # Test connection
            self.client.admin.command('ping')

            self.logger.info("MongoDB connection established")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")

    def health_check(self) -> bool:
        """Check MongoDB connection health."""
        try:
            if not self.client:
                return False

            self.client.admin.command('ping')
            return True

        except Exception as e:
            self.logger.error(f"MongoDB health check failed: {e}")
            return False

    def save_document(self, collection_name: str, document: Dict[str, Any]) -> bool:
        """
        Save a document to MongoDB.

        Args:
            collection_name: Name of the collection
            document: Document to save

        Returns:
            True if document was saved successfully
        """
        try:
            collection = self.database[collection_name]

            # Add timestamp if not present
            if 'timestamp' not in document:
                document['timestamp'] = datetime.utcnow()

            result = collection.insert_one(document)

            self.logger.debug(f"Saved document to {collection_name}: {result.inserted_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save document to {collection_name}: {e}")
            return False


class DatabaseManager:
    """
    Manages all database connections and provides unified access.
    """

    def __init__(self):
        """Initialize database manager."""
        self.logger = logging.getLogger(f"{__name__}.DatabaseManager")
        self.clients: Dict[str, DatabaseClientInterface] = {}
        self.config = load_database_config()

    def initialize_all_clients(self) -> bool:
        """
        Initialize all database clients based on configuration.

        Returns:
            True if all clients were initialized successfully
        """
        success = True

        # Initialize PostgreSQL client
        if 'trade_ledger_db' in self.config:
            try:
                postgres_client = PostgreSQLClient(self.config['trade_ledger_db'])
                if postgres_client.connect():
                    self.clients['postgresql'] = postgres_client
                    self.logger.info("PostgreSQL client initialized")
                else:
                    success = False
            except Exception as e:
                self.logger.error(f"Failed to initialize PostgreSQL client: {e}")
                success = False

        # Initialize InfluxDB client
        if 'timeseries_db' in self.config:
            try:
                influx_client = InfluxDBClient(self.config['timeseries_db'])
                if influx_client.connect():
                    self.clients['influxdb'] = influx_client
                    self.logger.info("InfluxDB client initialized")
                else:
                    success = False
            except Exception as e:
                self.logger.error(f"Failed to initialize InfluxDB client: {e}")
                success = False

        # Initialize MongoDB client
        if 'document_db' in self.config:
            try:
                mongo_client = MongoDBClient(self.config['document_db'])
                if mongo_client.connect():
                    self.clients['mongodb'] = mongo_client
                    self.logger.info("MongoDB client initialized")
                else:
                    success = False
            except Exception as e:
                self.logger.error(f"Failed to initialize MongoDB client: {e}")
                success = False

        return success

    def get_client(self, client_type: str) -> Optional[DatabaseClientInterface]:
        """
        Get a database client by type.

        Args:
            client_type: Type of client ('postgresql', 'influxdb', 'mongodb')

        Returns:
            Database client instance or None if not found
        """
        return self.clients.get(client_type)

    def health_check_all(self) -> Dict[str, bool]:
        """
        Perform health check on all database clients.

        Returns:
            Dictionary mapping client type to health status
        """
        health_status = {}

        for client_type, client in self.clients.items():
            health_status[client_type] = client.health_check()

        return health_status

    def disconnect_all(self) -> None:
        """Disconnect all database clients."""
        for client_type, client in self.clients.items():
            try:
                client.disconnect()
                self.logger.info(f"Disconnected {client_type} client")
            except Exception as e:
                self.logger.error(f"Error disconnecting {client_type} client: {e}")

    # Convenience methods for common operations
    def log_trade(self, trade_data: TradeData) -> bool:
        """Log a trade using the PostgreSQL client."""
        postgres_client = self.get_client('postgresql')
        if postgres_client and isinstance(postgres_client, PostgreSQLClient):
            return postgres_client.log_trade(trade_data)
        return False

    def save_to_timeseries(self, market_data: MarketData) -> bool:
        """Save market data using the InfluxDB client."""
        influx_client = self.get_client('influxdb')
        if influx_client and isinstance(influx_client, InfluxDBClient):
            return influx_client.save_to_timeseries(market_data)
        return False

    def save_document(self, collection_name: str, document: Dict[str, Any]) -> bool:
        """Save a document using the MongoDB client."""
        mongo_client = self.get_client('mongodb')
        if mongo_client and isinstance(mongo_client, MongoDBClient):
            return mongo_client.save_document(collection_name, document)
        return False


# Global database manager instance
_database_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _database_manager
    if _database_manager is None:
        _database_manager = DatabaseManager()
        _database_manager.initialize_all_clients()
    return _database_manager


def initialize_database_manager() -> DatabaseManager:
    """Initialize the global database manager instance."""
    global _database_manager
    _database_manager = DatabaseManager()
    _database_manager.initialize_all_clients()
    return _database_manager

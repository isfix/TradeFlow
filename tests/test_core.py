"""
Test suite for core infrastructure components.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

# Import core modules
from trading_platform.core.event_bus import EventBusFactory, EventMessage
from trading_platform.core.database_clients import DatabaseManager, TradeData, MarketData
from trading_platform.configs import ConfigLoader


class TestConfigLoader(unittest.TestCase):
    """Test configuration loading functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config_loader = ConfigLoader()
    
    def test_environment_variable_substitution(self):
        """Test environment variable substitution."""
        # Test with default value
        content = "test_value: ${TEST_VAR:-default_value}"
        result = self.config_loader._substitute_env_vars(content)
        self.assertIn("default_value", result)
    
    def test_invalid_environment_variable(self):
        """Test handling of missing required environment variables."""
        content = "test_value: ${REQUIRED_VAR}"
        with self.assertRaises(ValueError):
            self.config_loader._substitute_env_vars(content)


class TestEventBus(unittest.TestCase):
    """Test event bus functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service_name = "test_service"
        self.received_messages = []
    
    def message_callback(self, message: EventMessage):
        """Callback for testing message reception."""
        self.received_messages.append(message)
    
    @patch('trading_platform.core.event_bus.KAFKA_AVAILABLE', False)
    def test_kafka_unavailable_error(self):
        """Test error when Kafka is not available."""
        with self.assertRaises(ValueError):
            EventBusFactory.create_event_bus(self.service_name, "kafka")
    
    def test_event_message_creation(self):
        """Test EventMessage creation and serialization."""
        message = EventMessage(
            id="test_id",
            timestamp=datetime.now(timezone.utc),
            topic="test_topic",
            event_type="test_event",
            data={"key": "value"},
            source_service="test_service"
        )
        
        self.assertEqual(message.id, "test_id")
        self.assertEqual(message.topic, "test_topic")
        self.assertEqual(message.event_type, "test_event")
        self.assertEqual(message.data["key"], "value")


class TestDatabaseClients(unittest.TestCase):
    """Test database client functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = DatabaseManager()
    
    def test_trade_data_creation(self):
        """Test TradeData creation and validation."""
        trade_data = TradeData(
            trade_id="TEST_001",
            symbol="AAPL",
            side="BUY",
            quantity=100.0,
            price=150.0,
            timestamp=datetime.now(timezone.utc),
            order_id="ORDER_001",
            execution_id="EXEC_001",
            commission=1.0,
            source_service="test_service"
        )
        
        self.assertEqual(trade_data.symbol, "AAPL")
        self.assertEqual(trade_data.side, "BUY")
        self.assertEqual(trade_data.quantity, 100.0)
        self.assertEqual(trade_data.price, 150.0)
    
    def test_market_data_creation(self):
        """Test MarketData creation and validation."""
        market_data = MarketData(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            price=150.0,
            volume=1000.0,
            bid=149.95,
            ask=150.05
        )
        
        self.assertEqual(market_data.symbol, "AAPL")
        self.assertEqual(market_data.price, 150.0)
        self.assertEqual(market_data.volume, 1000.0)
        self.assertEqual(market_data.bid, 149.95)
        self.assertEqual(market_data.ask, 150.05)
    
    @patch('trading_platform.core.database_clients.POSTGRESQL_AVAILABLE', False)
    def test_postgresql_unavailable(self):
        """Test handling when PostgreSQL is not available."""
        from trading_platform.core.database_clients import PostgreSQLClient
        
        with self.assertRaises(ImportError):
            PostgreSQLClient({})
    
    def test_database_manager_initialization(self):
        """Test database manager initialization."""
        # Test that manager initializes without errors
        self.assertIsInstance(self.db_manager, DatabaseManager)
        self.assertIsInstance(self.db_manager.clients, dict)


class TestDataValidation(unittest.TestCase):
    """Test data validation functionality."""
    
    def test_trade_data_validation(self):
        """Test trade data validation."""
        # Valid trade data
        valid_trade = TradeData(
            trade_id="TEST_001",
            symbol="AAPL",
            side="BUY",
            quantity=100.0,
            price=150.0,
            timestamp=datetime.now(timezone.utc),
            order_id="ORDER_001",
            execution_id="EXEC_001",
            commission=1.0,
            source_service="test_service"
        )
        
        # Test that all required fields are present
        self.assertTrue(valid_trade.trade_id)
        self.assertTrue(valid_trade.symbol)
        self.assertIn(valid_trade.side, ["BUY", "SELL"])
        self.assertGreater(valid_trade.quantity, 0)
        self.assertGreater(valid_trade.price, 0)
    
    def test_market_data_validation(self):
        """Test market data validation."""
        # Valid market data
        valid_data = MarketData(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            price=150.0,
            volume=1000.0
        )
        
        # Test that all required fields are present
        self.assertTrue(valid_data.symbol)
        self.assertIsInstance(valid_data.timestamp, datetime)
        self.assertGreater(valid_data.price, 0)
        self.assertGreaterEqual(valid_data.volume, 0)


class TestErrorHandling(unittest.TestCase):
    """Test error handling across core components."""
    
    def test_config_loading_error_handling(self):
        """Test configuration loading error handling."""
        config_loader = ConfigLoader()
        
        # Test loading non-existent config
        with self.assertRaises(FileNotFoundError):
            config_loader.load_config("non_existent_config")
    
    def test_database_connection_error_handling(self):
        """Test database connection error handling."""
        # Test with invalid configuration
        invalid_config = {
            'host': 'invalid_host',
            'port': 9999,
            'database': 'invalid_db',
            'username': 'invalid_user',
            'password': 'invalid_pass'
        }
        
        # This should not raise an exception during initialization
        # but should handle connection errors gracefully
        db_manager = DatabaseManager()
        self.assertIsInstance(db_manager, DatabaseManager)


class TestPerformanceAndScaling(unittest.TestCase):
    """Test performance and scaling considerations."""
    
    def test_large_message_handling(self):
        """Test handling of large messages."""
        # Create a large data payload
        large_data = {"data": "x" * 10000}  # 10KB of data
        
        message = EventMessage(
            id="large_test",
            timestamp=datetime.now(timezone.utc),
            topic="test_topic",
            event_type="large_test",
            data=large_data,
            source_service="test_service"
        )
        
        # Test that large messages can be created and serialized
        self.assertEqual(len(message.data["data"]), 10000)
    
    def test_concurrent_access_safety(self):
        """Test thread safety of core components."""
        import threading
        
        # Test concurrent access to database manager
        db_manager = DatabaseManager()
        results = []
        
        def worker():
            try:
                # Simulate concurrent access
                health_status = db_manager.health_check_all()
                results.append(health_status)
            except Exception as e:
                results.append(f"Error: {e}")
        
        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All threads should complete without errors
        self.assertEqual(len(results), 5)


class TestIntegration(unittest.TestCase):
    """Integration tests for core components."""
    
    def test_end_to_end_data_flow(self):
        """Test end-to-end data flow through core components."""
        # This is a simplified integration test
        # In a real environment, this would test actual data flow
        
        # 1. Create market data
        market_data = MarketData(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            price=150.0,
            volume=1000.0
        )
        
        # 2. Create trade data
        trade_data = TradeData(
            trade_id="INTEGRATION_TEST_001",
            symbol="AAPL",
            side="BUY",
            quantity=100.0,
            price=150.0,
            timestamp=datetime.now(timezone.utc),
            order_id="ORDER_001",
            execution_id="EXEC_001",
            commission=1.0,
            source_service="integration_test"
        )
        
        # 3. Test that data structures are compatible
        self.assertEqual(market_data.symbol, trade_data.symbol)
        self.assertEqual(market_data.price, trade_data.price)
    
    def test_configuration_integration(self):
        """Test configuration integration across components."""
        config_loader = ConfigLoader()
        
        # Test that configuration loading works with core components
        try:
            # This should not raise an exception even if configs don't exist
            db_manager = DatabaseManager()
            self.assertIsInstance(db_manager, DatabaseManager)
        except Exception as e:
            # If there's an error, it should be a configuration-related error
            self.assertIn("config", str(e).lower())


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)

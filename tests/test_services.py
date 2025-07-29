"""
Test suite for trading platform services.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import numpy as np

# Import service modules
from trading_platform.services.data_ingestion.market_data_ingestor import DataValidator, HeartbeatManager
from trading_platform.services.processing.feature_engineer import TechnicalIndicators, DataWindow
from trading_platform.services.strategy.portfolio_manager import PositionSizer, RiskManager
from trading_platform.services.execution.risk_manager import TradingVelocityMonitor, PositionLimitMonitor


class TestDataIngestion(unittest.TestCase):
    """Test data ingestion services."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.data_validator = DataValidator()
        self.heartbeat_manager = HeartbeatManager()
    
    def test_data_validator_valid_data(self):
        """Test data validator with valid market data."""
        valid_data = {
            'symbol': 'AAPL',
            'price': 150.0,
            'volume': 1000,
            'timestamp': '2024-01-01T10:00:00Z'
        }
        
        result = self.data_validator.validate_market_data(valid_data)
        self.assertTrue(result)
    
    def test_data_validator_invalid_data(self):
        """Test data validator with invalid market data."""
        # Missing required fields
        invalid_data = {
            'symbol': 'AAPL',
            'price': 150.0
            # Missing timestamp
        }
        
        result = self.data_validator.validate_market_data(invalid_data)
        self.assertFalse(result)
        
        # Invalid price
        invalid_price_data = {
            'symbol': 'AAPL',
            'price': -150.0,  # Negative price
            'timestamp': '2024-01-01T10:00:00Z'
        }
        
        result = self.data_validator.validate_market_data(invalid_price_data)
        self.assertFalse(result)
    
    def test_heartbeat_manager(self):
        """Test heartbeat manager functionality."""
        # Initial state should be alive
        self.assertTrue(self.heartbeat_manager.is_alive)
        
        # Update heartbeat
        self.heartbeat_manager.update_heartbeat()
        self.assertTrue(self.heartbeat_manager.check_heartbeat())
        
        # Simulate timeout by setting old timestamp
        self.heartbeat_manager.last_heartbeat = time.time() - 120  # 2 minutes ago
        self.assertFalse(self.heartbeat_manager.check_heartbeat())


class TestFeatureEngineering(unittest.TestCase):
    """Test feature engineering services."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.data_window = DataWindow("AAPL", max_size=100)
        
        # Sample price data for testing
        self.sample_prices = np.array([100, 102, 101, 103, 105, 104, 106, 108, 107, 109])
        self.sample_volumes = np.array([1000, 1100, 900, 1200, 1300, 1000, 1400, 1500, 1100, 1600])
    
    def test_technical_indicators_sma(self):
        """Test Simple Moving Average calculation."""
        window = 5
        result = TechnicalIndicators.sma(self.sample_prices, window)
        
        # SMA of last 5 prices: (105, 104, 106, 108, 107) = 106
        expected = np.mean(self.sample_prices[-window:])
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_technical_indicators_ema(self):
        """Test Exponential Moving Average calculation."""
        window = 5
        result = TechnicalIndicators.ema(self.sample_prices, window)
        
        # EMA should be a valid number
        self.assertIsInstance(result, (int, float))
        self.assertFalse(np.isnan(result))
    
    def test_technical_indicators_rsi(self):
        """Test Relative Strength Index calculation."""
        result = TechnicalIndicators.rsi(self.sample_prices, window=5)
        
        # RSI should be between 0 and 100
        if not np.isnan(result):
            self.assertGreaterEqual(result, 0)
            self.assertLessEqual(result, 100)
    
    def test_technical_indicators_bollinger_bands(self):
        """Test Bollinger Bands calculation."""
        result = TechnicalIndicators.bollinger_bands(self.sample_prices, window=5)
        
        self.assertIn('upper', result)
        self.assertIn('middle', result)
        self.assertIn('lower', result)
        
        # Upper band should be higher than lower band
        if not (np.isnan(result['upper']) or np.isnan(result['lower'])):
            self.assertGreater(result['upper'], result['lower'])
    
    def test_technical_indicators_vwap(self):
        """Test Volume Weighted Average Price calculation."""
        result = TechnicalIndicators.vwap(self.sample_prices, self.sample_volumes)
        
        # VWAP should be a valid number
        self.assertIsInstance(result, (int, float))
        if not np.isnan(result):
            self.assertGreater(result, 0)
    
    def test_data_window_tick_aggregation(self):
        """Test data window tick aggregation."""
        timestamp = datetime.now(timezone.utc)
        
        # Add multiple ticks within the same minute
        self.data_window.add_tick(timestamp, 100.0, 1000)
        self.data_window.add_tick(timestamp, 101.0, 500)
        self.data_window.add_tick(timestamp, 99.0, 800)
        
        # Current bar should aggregate the ticks
        self.assertEqual(self.data_window.current_bar['open'], 100.0)
        self.assertEqual(self.data_window.current_bar['high'], 101.0)
        self.assertEqual(self.data_window.current_bar['low'], 99.0)
        self.assertEqual(self.data_window.current_bar['close'], 99.0)
        self.assertEqual(self.data_window.current_bar['volume'], 2300)
    
    def test_data_window_bar_completion(self):
        """Test data window bar completion."""
        timestamp1 = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        timestamp2 = timestamp1 + timedelta(minutes=1)
        
        # Add tick for first minute
        bar_completed = self.data_window.add_tick(timestamp1, 100.0, 1000)
        self.assertFalse(bar_completed)
        
        # Add tick for next minute - should complete the bar
        bar_completed = self.data_window.add_tick(timestamp2, 101.0, 1000)
        self.assertTrue(bar_completed)


class TestPortfolioManagement(unittest.TestCase):
    """Test portfolio management services."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.position_sizer = PositionSizer({
            'max_position_percentage': 0.05,
            'base_position_percentage': 0.02
        })
        
        self.risk_manager = RiskManager({
            'max_position_percentage': 0.10,
            'max_concentration_percentage': 0.20,
            'allow_short_selling': False
        })
    
    def test_position_sizing_strong_buy(self):
        """Test position sizing for STRONG_BUY signal."""
        from trading_platform.services.strategy.portfolio_manager import PortfolioState
        
        signal = {
            'signal': 'STRONG_BUY',
            'confidence': 0.8
        }
        
        portfolio_state = PortfolioState(
            cash_balance=Decimal('100000'),
            total_value=Decimal('100000'),
            positions={},
            buying_power=Decimal('100000'),
            margin_used=Decimal('0'),
            last_updated=datetime.utcnow()
        )
        
        current_price = Decimal('100')
        
        position_size, risk_metrics = self.position_sizer.calculate_position_size(
            signal, portfolio_state, current_price
        )
        
        # Should calculate a positive position size
        self.assertGreater(position_size, 0)
        self.assertIn('target_percentage', risk_metrics)
        self.assertIn('confidence_factor', risk_metrics)
    
    def test_position_sizing_hold_signal(self):
        """Test position sizing for HOLD signal."""
        from trading_platform.services.strategy.portfolio_manager import PortfolioState
        
        signal = {
            'signal': 'HOLD',
            'confidence': 0.5
        }
        
        portfolio_state = PortfolioState(
            cash_balance=Decimal('100000'),
            total_value=Decimal('100000'),
            positions={},
            buying_power=Decimal('100000'),
            margin_used=Decimal('0'),
            last_updated=datetime.utcnow()
        )
        
        current_price = Decimal('100')
        
        position_size, risk_metrics = self.position_sizer.calculate_position_size(
            signal, portfolio_state, current_price
        )
        
        # HOLD signal should result in zero position size
        self.assertEqual(position_size, 0)
    
    def test_kelly_criterion(self):
        """Test Kelly Criterion position sizing."""
        win_rate = 0.6
        avg_win = 0.1  # 10% average win
        avg_loss = 0.05  # 5% average loss
        base_size = Decimal('1000')
        
        kelly_size = self.position_sizer.apply_kelly_criterion(
            win_rate, avg_win, avg_loss, base_size
        )
        
        # Kelly size should be positive and reasonable
        self.assertGreater(kelly_size, 0)
        self.assertLessEqual(kelly_size, base_size * Decimal('0.25'))  # Capped at 25%


class TestRiskManagement(unittest.TestCase):
    """Test risk management services."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.velocity_monitor = TradingVelocityMonitor(
            max_trades_per_minute=5,
            max_trades_per_hour=50
        )
        
        self.position_monitor = PositionLimitMonitor({
            'max_position_percentage': 0.10,
            'max_position_quantity': 10000,
            'allow_short_selling': False
        })
    
    def test_trading_velocity_within_limits(self):
        """Test trading velocity within limits."""
        symbol = "AAPL"
        
        # Check velocity before any trades
        within_limits, message = self.velocity_monitor.check_velocity(symbol)
        self.assertTrue(within_limits)
        
        # Record a few trades
        for _ in range(3):
            self.velocity_monitor.record_trade(symbol)
        
        # Should still be within limits
        within_limits, message = self.velocity_monitor.check_velocity(symbol)
        self.assertTrue(within_limits)
    
    def test_trading_velocity_exceeds_limits(self):
        """Test trading velocity exceeding limits."""
        symbol = "AAPL"
        
        # Record trades up to the limit
        for _ in range(5):
            self.velocity_monitor.record_trade(symbol)
        
        # Next check should exceed the limit
        within_limits, message = self.velocity_monitor.check_velocity(symbol)
        self.assertFalse(within_limits)
        self.assertIn("Exceeded", message)
    
    def test_position_limits_within_bounds(self):
        """Test position limits within bounds."""
        symbol = "AAPL"
        side = "BUY"
        quantity = Decimal('100')
        price = Decimal('150')
        portfolio_value = Decimal('100000')
        
        within_limits, message = self.position_monitor.check_position_limits(
            symbol, side, quantity, price, portfolio_value
        )
        
        self.assertTrue(within_limits)
    
    def test_position_limits_exceeds_bounds(self):
        """Test position limits exceeding bounds."""
        symbol = "AAPL"
        side = "BUY"
        quantity = Decimal('1000')  # Large quantity
        price = Decimal('150')
        portfolio_value = Decimal('100000')
        
        within_limits, message = self.position_monitor.check_position_limits(
            symbol, side, quantity, price, portfolio_value
        )
        
        # This should exceed position limits
        self.assertFalse(within_limits)
        self.assertIn("exceed", message.lower())
    
    def test_short_selling_restriction(self):
        """Test short selling restriction."""
        symbol = "AAPL"
        side = "SELL"
        quantity = Decimal('200')  # More than current position (0)
        price = Decimal('150')
        portfolio_value = Decimal('100000')
        
        within_limits, message = self.position_monitor.check_position_limits(
            symbol, side, quantity, price, portfolio_value
        )
        
        # Should be rejected due to short selling restriction
        self.assertFalse(within_limits)


class TestPerformanceAnalysis(unittest.TestCase):
    """Test performance analysis services."""
    
    def test_trade_performance_calculation(self):
        """Test trade performance calculation."""
        from trading_platform.services.analysis.feedback_loop import PerformanceCalculator
        
        calculator = PerformanceCalculator()
        
        entry_trade = {
            'trade_id': 'TEST_001',
            'symbol': 'AAPL',
            'side': 'BUY',
            'quantity': 100,
            'price': 150.0,
            'timestamp': '2024-01-01T10:00:00+00:00',
            'signal_source': 'test_signal',
            'signal_confidence': 0.8
        }
        
        exit_trade = {
            'trade_id': 'TEST_002',
            'symbol': 'AAPL',
            'side': 'SELL',
            'quantity': 100,
            'price': 160.0,
            'timestamp': '2024-01-01T11:00:00+00:00'
        }
        
        performance = calculator.calculate_trade_performance(entry_trade, exit_trade)
        
        # Check calculated values
        self.assertEqual(performance.symbol, 'AAPL')
        self.assertEqual(performance.side, 'BUY')
        self.assertTrue(performance.is_closed)
        
        # P&L should be positive (bought at 150, sold at 160)
        expected_pnl = (160 - 150) * 100
        self.assertEqual(float(performance.pnl), expected_pnl)
        
        # P&L percentage should be about 6.67%
        expected_pnl_pct = (10 / 150) * 100
        self.assertAlmostEqual(performance.pnl_percentage, expected_pnl_pct, places=2)


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)

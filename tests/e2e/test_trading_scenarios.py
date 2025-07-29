"""
End-to-End Trading Scenarios Tests

These tests simulate complete trading workflows from data ingestion
through signal generation to order execution, testing the entire
system as a black box.
"""

import pytest
import asyncio
import time
import json
import requests
from typing import Dict, List, Any
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import docker
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import redis


class TradingSystemE2ETest:
    """
    End-to-end test framework for the trading system.
    
    Simulates real trading scenarios and validates system behavior.
    """
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """Initialize E2E test framework."""
        self.base_url = base_url
        self.kafka_bootstrap_servers = ["localhost:29092"]
        self.postgres_config = {
            "host": "localhost",
            "port": 5432,
            "database": "trading_platform_test",
            "user": "test_user",
            "password": "test_password"
        }
        self.redis_config = {
            "host": "localhost",
            "port": 6379,
            "decode_responses": True
        }
        
        # Test data
        self.test_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
        self.test_portfolio_value = Decimal("100000.00")
    
    async def setup_test_data(self):
        """Set up initial test data."""
        # Initialize database tables
        conn = psycopg2.connect(**self.postgres_config)
        
        with conn.cursor() as cursor:
            # Create test portfolio
            cursor.execute("""
                INSERT INTO portfolios (id, name, initial_value, current_value, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    current_value = EXCLUDED.current_value
            """, (
                "test_portfolio",
                "E2E Test Portfolio",
                self.test_portfolio_value,
                self.test_portfolio_value,
                datetime.utcnow()
            ))
            
            # Initialize positions
            for symbol in self.test_symbols:
                cursor.execute("""
                    INSERT INTO positions (portfolio_id, symbol, quantity, avg_price, market_value, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (portfolio_id, symbol) DO UPDATE SET
                        quantity = 0,
                        avg_price = 0,
                        market_value = 0,
                        updated_at = EXCLUDED.updated_at
                """, (
                    "test_portfolio",
                    symbol,
                    0,
                    0,
                    0,
                    datetime.utcnow()
                ))
            
            conn.commit()
        
        conn.close()
        
        # Clear Redis cache
        r = redis.Redis(**self.redis_config)
        r.flushdb()
    
    async def inject_market_data(self, symbol: str, price: float, volume: int, timestamp: datetime = None):
        """Inject market data into the system."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        
        market_data = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": timestamp.isoformat(),
            "source": "e2e_test"
        }
        
        producer.send("market_data", market_data)
        producer.flush()
        producer.close()
    
    async def inject_news_data(self, symbol: str, headline: str, sentiment: str = "neutral", timestamp: datetime = None):
        """Inject news data into the system."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        
        news_data = {
            "symbol": symbol,
            "headline": headline,
            "content": f"Test news content about {symbol}. This is {sentiment} news.",
            "sentiment": sentiment,
            "timestamp": timestamp.isoformat(),
            "source": "e2e_test"
        }
        
        producer.send("news_data", news_data)
        producer.flush()
        producer.close()
    
    async def wait_for_signals(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Wait for trading signals to be generated."""
        consumer = KafkaConsumer(
            "trading_signals",
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=timeout * 1000,
            auto_offset_reset="latest"
        )
        
        signals = []
        start_time = time.time()
        
        for message in consumer:
            signals.append(message.value)
            
            # Stop if we've been waiting too long
            if time.time() - start_time > timeout:
                break
        
        consumer.close()
        return signals
    
    async def wait_for_orders(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Wait for orders to be generated."""
        consumer = KafkaConsumer(
            "orders",
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=timeout * 1000,
            auto_offset_reset="latest"
        )
        
        orders = []
        start_time = time.time()
        
        for message in consumer:
            orders.append(message.value)
            
            if time.time() - start_time > timeout:
                break
        
        consumer.close()
        return orders
    
    async def get_portfolio_state(self) -> Dict[str, Any]:
        """Get current portfolio state."""
        conn = psycopg2.connect(**self.postgres_config)
        
        with conn.cursor() as cursor:
            # Get portfolio summary
            cursor.execute("""
                SELECT name, current_value, cash_balance, total_pnl
                FROM portfolios
                WHERE id = %s
            """, ("test_portfolio",))
            
            portfolio_row = cursor.fetchone()
            
            # Get positions
            cursor.execute("""
                SELECT symbol, quantity, avg_price, market_value, unrealized_pnl
                FROM positions
                WHERE portfolio_id = %s AND quantity != 0
            """, ("test_portfolio",))
            
            positions = []
            for row in cursor.fetchall():
                positions.append({
                    "symbol": row[0],
                    "quantity": float(row[1]),
                    "avg_price": float(row[2]),
                    "market_value": float(row[3]),
                    "unrealized_pnl": float(row[4]) if row[4] else 0.0
                })
        
        conn.close()
        
        return {
            "name": portfolio_row[0],
            "current_value": float(portfolio_row[1]),
            "cash_balance": float(portfolio_row[2]) if portfolio_row[2] else 0.0,
            "total_pnl": float(portfolio_row[3]) if portfolio_row[3] else 0.0,
            "positions": positions
        }
    
    async def verify_system_health(self) -> bool:
        """Verify that all system components are healthy."""
        try:
            # Check API health
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code != 200:
                return False
            
            # Check database connectivity
            conn = psycopg2.connect(**self.postgres_config)
            conn.close()
            
            # Check Redis connectivity
            r = redis.Redis(**self.redis_config)
            r.ping()
            
            # Check Kafka connectivity
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            producer.close()
            
            return True
            
        except Exception as e:
            print(f"Health check failed: {e}")
            return False


@pytest.fixture(scope="session")
async def e2e_test_framework():
    """Pytest fixture for E2E test framework."""
    framework = TradingSystemE2ETest()
    
    # Verify system is healthy before running tests
    assert await framework.verify_system_health(), "System health check failed"
    
    # Set up test data
    await framework.setup_test_data()
    
    yield framework


class TestTradingScenarios:
    """End-to-end trading scenario tests."""
    
    @pytest.mark.asyncio
    async def test_bullish_signal_generation(self, e2e_test_framework):
        """Test generation of bullish signals from positive market data and news."""
        framework = e2e_test_framework
        
        # Inject positive market data (price increase)
        await framework.inject_market_data("AAPL", 145.0, 1000)
        await asyncio.sleep(2)
        await framework.inject_market_data("AAPL", 148.0, 1200)
        await asyncio.sleep(2)
        await framework.inject_market_data("AAPL", 152.0, 1500)
        
        # Inject positive news
        await framework.inject_news_data(
            "AAPL",
            "Apple reports record quarterly earnings beating expectations",
            "positive"
        )
        
        # Wait for signals to be generated
        signals = await framework.wait_for_signals(timeout=30)
        
        # Verify bullish signal was generated
        assert len(signals) > 0, "No signals were generated"
        
        aapl_signals = [s for s in signals if s.get("symbol") == "AAPL"]
        assert len(aapl_signals) > 0, "No AAPL signals generated"
        
        # Should have at least one BUY or STRONG_BUY signal
        bullish_signals = [s for s in aapl_signals if s.get("signal") in ["BUY", "STRONG_BUY"]]
        assert len(bullish_signals) > 0, "No bullish signals generated"
        
        # Verify signal structure
        signal = bullish_signals[0]
        assert "confidence" in signal
        assert "reasoning" in signal
        assert signal["confidence"] > 0.5
    
    @pytest.mark.asyncio
    async def test_bearish_signal_generation(self, e2e_test_framework):
        """Test generation of bearish signals from negative market data and news."""
        framework = e2e_test_framework
        
        # Inject negative market data (price decrease)
        await framework.inject_market_data("GOOGL", 155.0, 1000)
        await asyncio.sleep(2)
        await framework.inject_market_data("GOOGL", 150.0, 1500)
        await asyncio.sleep(2)
        await framework.inject_market_data("GOOGL", 145.0, 2000)
        
        # Inject negative news
        await framework.inject_news_data(
            "GOOGL",
            "Google faces regulatory challenges and declining ad revenue",
            "negative"
        )
        
        # Wait for signals
        signals = await framework.wait_for_signals(timeout=30)
        
        # Verify bearish signal was generated
        assert len(signals) > 0, "No signals were generated"
        
        googl_signals = [s for s in signals if s.get("symbol") == "GOOGL"]
        assert len(googl_signals) > 0, "No GOOGL signals generated"
        
        # Should have at least one SELL or STRONG_SELL signal
        bearish_signals = [s for s in googl_signals if s.get("signal") in ["SELL", "STRONG_SELL"]]
        assert len(bearish_signals) > 0, "No bearish signals generated"
    
    @pytest.mark.asyncio
    async def test_order_generation_and_execution(self, e2e_test_framework):
        """Test complete order generation and execution workflow."""
        framework = e2e_test_framework
        
        # Get initial portfolio state
        initial_portfolio = await framework.get_portfolio_state()
        initial_cash = initial_portfolio["cash_balance"]
        
        # Generate strong bullish signal
        await framework.inject_market_data("MSFT", 300.0, 1000)
        await framework.inject_news_data(
            "MSFT",
            "Microsoft announces breakthrough in AI technology",
            "positive"
        )
        
        # Wait for signals and orders
        signals = await framework.wait_for_signals(timeout=30)
        orders = await framework.wait_for_orders(timeout=30)
        
        # Verify order was generated
        assert len(orders) > 0, "No orders were generated"
        
        msft_orders = [o for o in orders if o.get("symbol") == "MSFT"]
        assert len(msft_orders) > 0, "No MSFT orders generated"
        
        # Verify order structure
        order = msft_orders[0]
        assert "order_id" in order
        assert "symbol" in order
        assert "side" in order
        assert "quantity" in order
        assert "price" in order
        assert order["side"] in ["BUY", "SELL"]
        assert order["quantity"] > 0
        
        # Wait for order execution
        await asyncio.sleep(10)
        
        # Verify portfolio was updated
        final_portfolio = await framework.get_portfolio_state()
        
        if order["side"] == "BUY":
            # Should have new position or increased existing position
            msft_positions = [p for p in final_portfolio["positions"] if p["symbol"] == "MSFT"]
            assert len(msft_positions) > 0, "No MSFT position after buy order"
            assert msft_positions[0]["quantity"] > 0
    
    @pytest.mark.asyncio
    async def test_risk_management_limits(self, e2e_test_framework):
        """Test that risk management prevents excessive position sizes."""
        framework = e2e_test_framework
        
        # Try to generate a very large order by creating extreme signals
        for i in range(10):  # Multiple strong signals
            await framework.inject_market_data("TSLA", 200.0 + i * 10, 5000)
            await framework.inject_news_data(
                "TSLA",
                f"Tesla announces major breakthrough #{i}",
                "positive"
            )
            await asyncio.sleep(1)
        
        # Wait for orders
        orders = await framework.wait_for_orders(timeout=30)
        
        # Verify orders respect position limits
        tsla_orders = [o for o in orders if o.get("symbol") == "TSLA"]
        
        if tsla_orders:
            total_order_value = sum(
                float(o["quantity"]) * float(o["price"])
                for o in tsla_orders
                if o["side"] == "BUY"
            )
            
            # Should not exceed reasonable percentage of portfolio
            max_position_value = float(framework.test_portfolio_value) * 0.1  # 10% max
            assert total_order_value <= max_position_value, f"Order value {total_order_value} exceeds limit {max_position_value}"
    
    @pytest.mark.asyncio
    async def test_multi_asset_portfolio_management(self, e2e_test_framework):
        """Test portfolio management across multiple assets."""
        framework = e2e_test_framework
        
        # Generate signals for multiple symbols
        symbols_and_prices = [
            ("AAPL", 150.0),
            ("GOOGL", 140.0),
            ("MSFT", 310.0),
            ("TSLA", 220.0)
        ]
        
        for symbol, price in symbols_and_prices:
            await framework.inject_market_data(symbol, price, 1000)
            await framework.inject_news_data(
                symbol,
                f"{symbol} shows strong performance",
                "positive"
            )
            await asyncio.sleep(2)
        
        # Wait for signals and orders
        signals = await framework.wait_for_signals(timeout=45)
        orders = await framework.wait_for_orders(timeout=45)
        
        # Verify diversification
        symbols_with_signals = set(s["symbol"] for s in signals)
        symbols_with_orders = set(o["symbol"] for o in orders)
        
        # Should have signals for multiple symbols
        assert len(symbols_with_signals) >= 2, "Insufficient diversification in signals"
        
        # Portfolio allocation should be reasonable
        if orders:
            buy_orders = [o for o in orders if o["side"] == "BUY"]
            if buy_orders:
                total_order_value = sum(
                    float(o["quantity"]) * float(o["price"])
                    for o in buy_orders
                )
                
                # Total orders should not exceed portfolio value
                assert total_order_value <= float(framework.test_portfolio_value), "Orders exceed portfolio value"
    
    @pytest.mark.asyncio
    async def test_system_recovery_after_failure(self, e2e_test_framework):
        """Test system recovery after simulated component failure."""
        framework = e2e_test_framework
        
        # Inject data before "failure"
        await framework.inject_market_data("AAPL", 150.0, 1000)
        
        # Simulate Redis failure by flushing cache
        r = redis.Redis(**framework.redis_config)
        r.flushdb()
        
        # Continue injecting data
        await framework.inject_market_data("AAPL", 155.0, 1200)
        await framework.inject_news_data(
            "AAPL",
            "Apple continues strong performance despite technical issues",
            "positive"
        )
        
        # System should recover and continue processing
        signals = await framework.wait_for_signals(timeout=30)
        
        # Should still generate signals despite cache loss
        assert len(signals) > 0, "System did not recover after cache failure"
    
    @pytest.mark.asyncio
    async def test_performance_under_load(self, e2e_test_framework):
        """Test system performance under high data load."""
        framework = e2e_test_framework
        
        start_time = time.time()
        
        # Inject high volume of data
        for i in range(100):
            symbol = framework.test_symbols[i % len(framework.test_symbols)]
            price = 100.0 + (i % 50)
            
            await framework.inject_market_data(symbol, price, 1000 + i * 10)
            
            if i % 10 == 0:  # Inject news every 10 market data points
                await framework.inject_news_data(
                    symbol,
                    f"News update #{i} for {symbol}",
                    "neutral"
                )
        
        # Wait for processing
        signals = await framework.wait_for_signals(timeout=60)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify system handled the load
        assert len(signals) > 0, "No signals generated under load"
        assert processing_time < 120, f"Processing took too long: {processing_time}s"
        
        # Verify system is still healthy
        assert await framework.verify_system_health(), "System unhealthy after load test"


if __name__ == "__main__":
    # Run E2E tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])

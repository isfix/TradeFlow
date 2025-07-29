"""
Performance and Load Tests for Trading Platform

These tests verify system performance under various load conditions
and ensure the platform meets performance requirements.
"""

import pytest
import asyncio
import time
import statistics
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import psutil
import memory_profiler
from kafka import KafkaProducer, KafkaConsumer
import redis
import psycopg2
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class PerformanceMetrics:
    """Performance metrics container."""
    throughput: float  # operations per second
    latency_p50: float  # 50th percentile latency (ms)
    latency_p95: float  # 95th percentile latency (ms)
    latency_p99: float  # 99th percentile latency (ms)
    error_rate: float  # percentage of failed operations
    cpu_usage: float  # average CPU usage percentage
    memory_usage: float  # peak memory usage (MB)
    duration: float  # test duration (seconds)


class PerformanceTestFramework:
    """Framework for performance testing."""
    
    def __init__(self):
        """Initialize performance test framework."""
        self.kafka_bootstrap_servers = ["localhost:29092"]
        self.redis_config = {
            "host": "localhost",
            "port": 6379,
            "decode_responses": True
        }
        self.postgres_config = {
            "host": "localhost",
            "port": 5432,
            "database": "trading_platform_test",
            "user": "test_user",
            "password": "test_password"
        }
        
        # Performance tracking
        self.latencies: List[float] = []
        self.errors: List[str] = []
        self.start_time: float = 0
        self.end_time: float = 0
        
        # System monitoring
        self.cpu_samples: List[float] = []
        self.memory_samples: List[float] = []
        self.monitoring_active = False
    
    def start_monitoring(self):
        """Start system resource monitoring."""
        self.monitoring_active = True
        self.cpu_samples.clear()
        self.memory_samples.clear()
        
        def monitor():
            while self.monitoring_active:
                self.cpu_samples.append(psutil.cpu_percent(interval=1))
                self.memory_samples.append(psutil.virtual_memory().used / 1024 / 1024)  # MB
        
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop system resource monitoring."""
        self.monitoring_active = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2)
    
    def calculate_metrics(self, operation_count: int) -> PerformanceMetrics:
        """Calculate performance metrics from collected data."""
        duration = self.end_time - self.start_time
        throughput = operation_count / duration if duration > 0 else 0
        
        # Calculate latency percentiles
        if self.latencies:
            latency_p50 = statistics.median(self.latencies)
            latency_p95 = statistics.quantiles(self.latencies, n=20)[18]  # 95th percentile
            latency_p99 = statistics.quantiles(self.latencies, n=100)[98]  # 99th percentile
        else:
            latency_p50 = latency_p95 = latency_p99 = 0
        
        error_rate = (len(self.errors) / operation_count * 100) if operation_count > 0 else 0
        
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        peak_memory = max(self.memory_samples) if self.memory_samples else 0
        
        return PerformanceMetrics(
            throughput=throughput,
            latency_p50=latency_p50,
            latency_p95=latency_p95,
            latency_p99=latency_p99,
            error_rate=error_rate,
            cpu_usage=avg_cpu,
            memory_usage=peak_memory,
            duration=duration
        )
    
    async def test_market_data_ingestion_throughput(self, messages_per_second: int, duration_seconds: int) -> PerformanceMetrics:
        """Test market data ingestion throughput."""
        self.latencies.clear()
        self.errors.clear()
        
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            batch_size=16384,
            linger_ms=10
        )
        
        total_messages = messages_per_second * duration_seconds
        interval = 1.0 / messages_per_second
        
        self.start_monitoring()
        self.start_time = time.time()
        
        try:
            for i in range(total_messages):
                start_op = time.time()
                
                try:
                    market_data = {
                        "symbol": f"TEST{i % 100}",
                        "price": 100.0 + (i % 50),
                        "volume": 1000 + (i % 1000),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "performance_test"
                    }
                    
                    producer.send("market_data", market_data)
                    
                    if i % 100 == 0:  # Flush every 100 messages
                        producer.flush()
                    
                    end_op = time.time()
                    self.latencies.append((end_op - start_op) * 1000)  # Convert to ms
                    
                except Exception as e:
                    self.errors.append(str(e))
                
                # Rate limiting
                if i < total_messages - 1:
                    await asyncio.sleep(interval)
        
        finally:
            producer.flush()
            producer.close()
            self.end_time = time.time()
            self.stop_monitoring()
        
        return self.calculate_metrics(total_messages)
    
    async def test_database_write_performance(self, writes_per_second: int, duration_seconds: int) -> PerformanceMetrics:
        """Test database write performance."""
        self.latencies.clear()
        self.errors.clear()
        
        total_writes = writes_per_second * duration_seconds
        interval = 1.0 / writes_per_second
        
        self.start_monitoring()
        self.start_time = time.time()
        
        try:
            conn = psycopg2.connect(**self.postgres_config)
            
            with conn.cursor() as cursor:
                for i in range(total_writes):
                    start_op = time.time()
                    
                    try:
                        cursor.execute("""
                            INSERT INTO test_performance_trades 
                            (symbol, price, quantity, timestamp)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            f"TEST{i % 100}",
                            100.0 + (i % 50),
                            100 + (i % 900),
                            datetime.now(timezone.utc)
                        ))
                        
                        if i % 100 == 0:  # Commit every 100 writes
                            conn.commit()
                        
                        end_op = time.time()
                        self.latencies.append((end_op - start_op) * 1000)
                        
                    except Exception as e:
                        self.errors.append(str(e))
                        conn.rollback()
                    
                    if i < total_writes - 1:
                        await asyncio.sleep(interval)
                
                conn.commit()
        
        finally:
            conn.close()
            self.end_time = time.time()
            self.stop_monitoring()
        
        return self.calculate_metrics(total_writes)
    
    async def test_redis_cache_performance(self, operations_per_second: int, duration_seconds: int) -> PerformanceMetrics:
        """Test Redis cache performance."""
        self.latencies.clear()
        self.errors.clear()
        
        total_operations = operations_per_second * duration_seconds
        interval = 1.0 / operations_per_second
        
        r = redis.Redis(**self.redis_config)
        
        self.start_monitoring()
        self.start_time = time.time()
        
        try:
            for i in range(total_operations):
                start_op = time.time()
                
                try:
                    key = f"test_key_{i % 1000}"
                    value = json.dumps({
                        "symbol": f"TEST{i % 100}",
                        "price": 100.0 + (i % 50),
                        "timestamp": time.time()
                    })
                    
                    # Mix of read and write operations
                    if i % 3 == 0:  # Write operation
                        r.setex(key, 300, value)  # 5 minute expiry
                    else:  # Read operation
                        r.get(key)
                    
                    end_op = time.time()
                    self.latencies.append((end_op - start_op) * 1000)
                    
                except Exception as e:
                    self.errors.append(str(e))
                
                if i < total_operations - 1:
                    await asyncio.sleep(interval)
        
        finally:
            self.end_time = time.time()
            self.stop_monitoring()
        
        return self.calculate_metrics(total_operations)
    
    async def test_concurrent_signal_processing(self, concurrent_workers: int, signals_per_worker: int) -> PerformanceMetrics:
        """Test concurrent signal processing performance."""
        self.latencies.clear()
        self.errors.clear()
        
        total_signals = concurrent_workers * signals_per_worker
        
        def process_signals(worker_id: int) -> List[float]:
            """Process signals in a worker thread."""
            worker_latencies = []
            
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            
            try:
                for i in range(signals_per_worker):
                    start_op = time.time()
                    
                    signal_data = {
                        "symbol": f"TEST{(worker_id * signals_per_worker + i) % 100}",
                        "signal": "BUY",
                        "confidence": 0.8,
                        "reasoning": f"Test signal from worker {worker_id}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "performance_test"
                    }
                    
                    producer.send("trading_signals", signal_data)
                    
                    end_op = time.time()
                    worker_latencies.append((end_op - start_op) * 1000)
            
            except Exception as e:
                self.errors.append(f"Worker {worker_id}: {str(e)}")
            
            finally:
                producer.flush()
                producer.close()
            
            return worker_latencies
        
        self.start_monitoring()
        self.start_time = time.time()
        
        # Execute concurrent workers
        with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
            futures = [
                executor.submit(process_signals, worker_id)
                for worker_id in range(concurrent_workers)
            ]
            
            # Collect results
            for future in as_completed(futures):
                try:
                    worker_latencies = future.result()
                    self.latencies.extend(worker_latencies)
                except Exception as e:
                    self.errors.append(str(e))
        
        self.end_time = time.time()
        self.stop_monitoring()
        
        return self.calculate_metrics(total_signals)


@pytest.fixture(scope="session")
def performance_framework():
    """Pytest fixture for performance test framework."""
    framework = PerformanceTestFramework()
    
    # Set up test database table
    conn = psycopg2.connect(**framework.postgres_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_performance_trades (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                price DECIMAL(10,2),
                quantity INTEGER,
                timestamp TIMESTAMP
            )
        """)
        conn.commit()
    conn.close()
    
    yield framework
    
    # Cleanup
    conn = psycopg2.connect(**framework.postgres_config)
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_performance_trades")
        conn.commit()
    conn.close()


class TestSystemPerformance:
    """System performance tests."""
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_market_data_ingestion_baseline(self, performance_framework):
        """Test baseline market data ingestion performance."""
        metrics = await performance_framework.test_market_data_ingestion_throughput(
            messages_per_second=100,
            duration_seconds=30
        )
        
        # Performance assertions
        assert metrics.throughput >= 90, f"Throughput too low: {metrics.throughput} msg/s"
        assert metrics.latency_p95 <= 100, f"P95 latency too high: {metrics.latency_p95} ms"
        assert metrics.error_rate <= 1, f"Error rate too high: {metrics.error_rate}%"
        assert metrics.cpu_usage <= 80, f"CPU usage too high: {metrics.cpu_usage}%"
        
        print(f"Market Data Ingestion Performance:")
        print(f"  Throughput: {metrics.throughput:.2f} msg/s")
        print(f"  P50 Latency: {metrics.latency_p50:.2f} ms")
        print(f"  P95 Latency: {metrics.latency_p95:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
        print(f"  CPU Usage: {metrics.cpu_usage:.2f}%")
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_high_volume_market_data(self, performance_framework):
        """Test high volume market data processing."""
        metrics = await performance_framework.test_market_data_ingestion_throughput(
            messages_per_second=1000,
            duration_seconds=60
        )
        
        # High volume performance requirements
        assert metrics.throughput >= 800, f"High volume throughput too low: {metrics.throughput} msg/s"
        assert metrics.latency_p99 <= 500, f"P99 latency too high under load: {metrics.latency_p99} ms"
        assert metrics.error_rate <= 5, f"Error rate too high under load: {metrics.error_rate}%"
        
        print(f"High Volume Market Data Performance:")
        print(f"  Throughput: {metrics.throughput:.2f} msg/s")
        print(f"  P99 Latency: {metrics.latency_p99:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_database_write_performance(self, performance_framework):
        """Test database write performance."""
        metrics = await performance_framework.test_database_write_performance(
            writes_per_second=50,
            duration_seconds=30
        )
        
        # Database performance requirements
        assert metrics.throughput >= 45, f"Database write throughput too low: {metrics.throughput} writes/s"
        assert metrics.latency_p95 <= 200, f"Database write P95 latency too high: {metrics.latency_p95} ms"
        assert metrics.error_rate <= 1, f"Database error rate too high: {metrics.error_rate}%"
        
        print(f"Database Write Performance:")
        print(f"  Throughput: {metrics.throughput:.2f} writes/s")
        print(f"  P95 Latency: {metrics.latency_p95:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_redis_cache_performance(self, performance_framework):
        """Test Redis cache performance."""
        metrics = await performance_framework.test_redis_cache_performance(
            operations_per_second=500,
            duration_seconds=30
        )
        
        # Cache performance requirements
        assert metrics.throughput >= 450, f"Cache throughput too low: {metrics.throughput} ops/s"
        assert metrics.latency_p95 <= 50, f"Cache P95 latency too high: {metrics.latency_p95} ms"
        assert metrics.error_rate <= 0.5, f"Cache error rate too high: {metrics.error_rate}%"
        
        print(f"Redis Cache Performance:")
        print(f"  Throughput: {metrics.throughput:.2f} ops/s")
        print(f"  P95 Latency: {metrics.latency_p95:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_concurrent_signal_processing(self, performance_framework):
        """Test concurrent signal processing performance."""
        metrics = await performance_framework.test_concurrent_signal_processing(
            concurrent_workers=10,
            signals_per_worker=50
        )
        
        # Concurrent processing requirements
        assert metrics.throughput >= 400, f"Concurrent throughput too low: {metrics.throughput} signals/s"
        assert metrics.latency_p95 <= 150, f"Concurrent P95 latency too high: {metrics.latency_p95} ms"
        assert metrics.error_rate <= 2, f"Concurrent error rate too high: {metrics.error_rate}%"
        
        print(f"Concurrent Signal Processing Performance:")
        print(f"  Throughput: {metrics.throughput:.2f} signals/s")
        print(f"  P95 Latency: {metrics.latency_p95:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, performance_framework):
        """Test memory usage under sustained load."""
        # Run multiple performance tests to simulate sustained load
        initial_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
        
        # Market data load
        await performance_framework.test_market_data_ingestion_throughput(
            messages_per_second=500,
            duration_seconds=60
        )
        
        # Database load
        await performance_framework.test_database_write_performance(
            writes_per_second=25,
            duration_seconds=30
        )
        
        # Cache load
        await performance_framework.test_redis_cache_performance(
            operations_per_second=250,
            duration_seconds=30
        )
        
        final_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory usage requirements
        assert memory_increase <= 500, f"Memory increase too high: {memory_increase:.2f} MB"
        
        print(f"Memory Usage Under Load:")
        print(f"  Initial Memory: {initial_memory:.2f} MB")
        print(f"  Final Memory: {final_memory:.2f} MB")
        print(f"  Memory Increase: {memory_increase:.2f} MB")


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "-m", "performance", "--tb=short"])

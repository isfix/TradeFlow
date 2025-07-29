"""
Integration tests for microservices architecture.

These tests verify that services can communicate with each other
and with the infrastructure components (databases, event bus) correctly.
"""

import pytest
import asyncio
import docker
import time
import requests
import json
import os
from typing import Dict, List, Any
from pathlib import Path
import subprocess
import psycopg2
import redis
import pymongo
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


class MicroservicesTestEnvironment:
    """
    Test environment manager for microservices integration tests.
    
    Manages Docker containers for all services and infrastructure components.
    """
    
    def __init__(self):
        """Initialize the test environment."""
        self.docker_client = docker.from_env()
        self.containers: Dict[str, docker.models.containers.Container] = {}
        self.network_name = "trading_test_network"
        self.network = None
        
        # Service configurations
        self.services = {
            "postgres": {
                "image": "postgres:15-alpine",
                "environment": {
                    "POSTGRES_DB": "trading_platform_test",
                    "POSTGRES_USER": "test_user",
                    "POSTGRES_PASSWORD": "test_password"
                },
                "ports": {"5432/tcp": 5432},
                "healthcheck": self._postgres_healthcheck
            },
            "redis": {
                "image": "redis:7-alpine",
                "ports": {"6379/tcp": 6379},
                "healthcheck": self._redis_healthcheck
            },
            "mongodb": {
                "image": "mongo:7.0",
                "environment": {
                    "MONGO_INITDB_ROOT_USERNAME": "test_user",
                    "MONGO_INITDB_ROOT_PASSWORD": "test_password"
                },
                "ports": {"27017/tcp": 27017},
                "healthcheck": self._mongodb_healthcheck
            },
            "zookeeper": {
                "image": "confluentinc/cp-zookeeper:7.4.0",
                "environment": {
                    "ZOOKEEPER_CLIENT_PORT": "2181",
                    "ZOOKEEPER_TICK_TIME": "2000"
                },
                "ports": {"2181/tcp": 2181},
                "healthcheck": self._zookeeper_healthcheck
            },
            "kafka": {
                "image": "confluentinc/cp-kafka:7.4.0",
                "environment": {
                    "KAFKA_BROKER_ID": "1",
                    "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
                    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092",
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1"
                },
                "ports": {"29092/tcp": 29092},
                "depends_on": ["zookeeper"],
                "healthcheck": self._kafka_healthcheck
            }
        }
        
        # Microservices
        self.microservices = [
            "market_data_service",
            "news_service",
            "feature_engineering_service",
            "sentiment_analysis_service",
            "signal_aggregation_service",
            "risk_management_service"
        ]
    
    async def setup(self):
        """Set up the test environment."""
        try:
            # Create network
            self.network = self.docker_client.networks.create(
                self.network_name,
                driver="bridge"
            )
            
            # Start infrastructure services
            await self._start_infrastructure()
            
            # Wait for infrastructure to be ready
            await self._wait_for_infrastructure()
            
            # Build and start microservices
            await self._start_microservices()
            
            # Wait for microservices to be ready
            await self._wait_for_microservices()
            
        except Exception as e:
            await self.teardown()
            raise RuntimeError(f"Failed to set up test environment: {e}")
    
    async def teardown(self):
        """Tear down the test environment."""
        try:
            # Stop all containers
            for container in self.containers.values():
                try:
                    container.stop(timeout=10)
                    container.remove()
                except Exception as e:
                    print(f"Error stopping container: {e}")
            
            # Remove network
            if self.network:
                try:
                    self.network.remove()
                except Exception as e:
                    print(f"Error removing network: {e}")
            
            self.containers.clear()
            
        except Exception as e:
            print(f"Error during teardown: {e}")
    
    async def _start_infrastructure(self):
        """Start infrastructure services."""
        for service_name, config in self.services.items():
            if service_name in ["zookeeper", "kafka"]:
                continue  # Handle Kafka separately due to dependencies
            
            container = self.docker_client.containers.run(
                config["image"],
                environment=config.get("environment", {}),
                ports=config.get("ports", {}),
                network=self.network_name,
                name=f"test_{service_name}",
                detach=True,
                remove=True
            )
            
            self.containers[service_name] = container
        
        # Start Zookeeper first
        zk_config = self.services["zookeeper"]
        zk_container = self.docker_client.containers.run(
            zk_config["image"],
            environment=zk_config["environment"],
            ports=zk_config["ports"],
            network=self.network_name,
            name="test_zookeeper",
            detach=True,
            remove=True
        )
        self.containers["zookeeper"] = zk_container
        
        # Wait for Zookeeper
        await asyncio.sleep(10)
        
        # Start Kafka
        kafka_config = self.services["kafka"]
        kafka_container = self.docker_client.containers.run(
            kafka_config["image"],
            environment=kafka_config["environment"],
            ports=kafka_config["ports"],
            network=self.network_name,
            name="test_kafka",
            detach=True,
            remove=True
        )
        self.containers["kafka"] = kafka_container
    
    async def _start_microservices(self):
        """Build and start microservices."""
        # Build microservice images
        project_root = Path(__file__).parent.parent.parent
        
        for service_name in self.microservices:
            # Determine service path
            if service_name in ["market_data_service", "news_service"]:
                service_path = "services/data_ingestion"
            elif service_name in ["feature_engineering_service", "sentiment_analysis_service"]:
                service_path = "services/processing"
            elif service_name == "signal_aggregation_service":
                service_path = "services/strategy"
            elif service_name == "risk_management_service":
                service_path = "services/execution"
            
            # Build image
            image_tag = f"trading-platform-{service_name}:test"
            
            self.docker_client.images.build(
                path=str(project_root),
                dockerfile="docker/Dockerfile.microservice",
                tag=image_tag,
                buildargs={
                    "SERVICE_NAME": service_name,
                    "SERVICE_PATH": service_path
                },
                target="development"
            )
            
            # Start container
            container = self.docker_client.containers.run(
                image_tag,
                environment={
                    "SERVICE_NAME": service_name,
                    "POSTGRES_HOST": "test_postgres",
                    "POSTGRES_PORT": "5432",
                    "POSTGRES_DB": "trading_platform_test",
                    "POSTGRES_USER": "test_user",
                    "POSTGRES_PASSWORD": "test_password",
                    "REDIS_HOST": "test_redis",
                    "REDIS_PORT": "6379",
                    "MONGODB_HOST": "test_mongodb",
                    "MONGODB_PORT": "27017",
                    "MONGODB_USER": "test_user",
                    "MONGODB_PASSWORD": "test_password",
                    "KAFKA_BOOTSTRAP_SERVERS": "test_kafka:9092",
                    "ENVIRONMENT": "testing",
                    "LOG_LEVEL": "DEBUG"
                },
                network=self.network_name,
                name=f"test_{service_name}",
                detach=True,
                remove=True
            )
            
            self.containers[service_name] = container
    
    async def _wait_for_infrastructure(self):
        """Wait for infrastructure services to be ready."""
        max_wait = 120  # 2 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            all_ready = True
            
            for service_name, config in self.services.items():
                if not await config["healthcheck"]():
                    all_ready = False
                    break
            
            if all_ready:
                return
            
            await asyncio.sleep(5)
        
        raise TimeoutError("Infrastructure services did not become ready in time")
    
    async def _wait_for_microservices(self):
        """Wait for microservices to be ready."""
        max_wait = 180  # 3 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            all_ready = True
            
            for service_name in self.microservices:
                container = self.containers[service_name]
                
                # Check if container is running
                container.reload()
                if container.status != "running":
                    all_ready = False
                    break
                
                # Check logs for startup completion
                logs = container.logs().decode()
                if "started successfully" not in logs.lower():
                    all_ready = False
                    break
            
            if all_ready:
                return
            
            await asyncio.sleep(10)
        
        raise TimeoutError("Microservices did not become ready in time")
    
    async def _postgres_healthcheck(self) -> bool:
        """Check PostgreSQL health."""
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="trading_platform_test",
                user="test_user",
                password="test_password"
            )
            conn.close()
            return True
        except Exception:
            return False
    
    async def _redis_healthcheck(self) -> bool:
        """Check Redis health."""
        try:
            r = redis.Redis(host="localhost", port=6379, decode_responses=True)
            r.ping()
            return True
        except Exception:
            return False
    
    async def _mongodb_healthcheck(self) -> bool:
        """Check MongoDB health."""
        try:
            client = pymongo.MongoClient(
                "mongodb://test_user:test_password@localhost:27017/"
            )
            client.admin.command("ping")
            return True
        except Exception:
            return False
    
    async def _zookeeper_healthcheck(self) -> bool:
        """Check Zookeeper health."""
        try:
            # Simple TCP connection check
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(("localhost", 2181))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    async def _kafka_healthcheck(self) -> bool:
        """Check Kafka health."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=["localhost:29092"],
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            producer.close()
            return True
        except Exception:
            return False


@pytest.fixture(scope="session")
async def test_environment():
    """Pytest fixture for test environment."""
    env = MicroservicesTestEnvironment()
    await env.setup()
    
    yield env
    
    await env.teardown()


class TestMicroservicesIntegration:
    """Integration tests for microservices."""
    
    @pytest.mark.asyncio
    async def test_infrastructure_connectivity(self, test_environment):
        """Test that all infrastructure components are accessible."""
        # Test PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="trading_platform_test",
            user="test_user",
            password="test_password"
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
        
        conn.close()
        
        # Test Redis
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.set("test_key", "test_value")
        assert r.get("test_key") == "test_value"
        r.delete("test_key")
        
        # Test MongoDB
        client = pymongo.MongoClient(
            "mongodb://test_user:test_password@localhost:27017/"
        )
        db = client.test_db
        collection = db.test_collection
        
        doc_id = collection.insert_one({"test": "data"}).inserted_id
        doc = collection.find_one({"_id": doc_id})
        assert doc["test"] == "data"
        
        collection.delete_one({"_id": doc_id})
        client.close()
        
        # Test Kafka
        producer = KafkaProducer(
            bootstrap_servers=["localhost:29092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        
        producer.send("test_topic", {"test": "message"})
        producer.flush()
        producer.close()
        
        consumer = KafkaConsumer(
            "test_topic",
            bootstrap_servers=["localhost:29092"],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=5000
        )
        
        message = next(consumer)
        assert message.value["test"] == "message"
        consumer.close()
    
    @pytest.mark.asyncio
    async def test_microservices_startup(self, test_environment):
        """Test that all microservices start successfully."""
        for service_name in test_environment.microservices:
            container = test_environment.containers[service_name]
            
            # Check container status
            container.reload()
            assert container.status == "running"
            
            # Check logs for successful startup
            logs = container.logs().decode()
            assert "initialized successfully" in logs.lower()
    
    @pytest.mark.asyncio
    async def test_service_to_service_communication(self, test_environment):
        """Test communication between microservices via event bus."""
        # Send a test message from market data service
        producer = KafkaProducer(
            bootstrap_servers=["localhost:29092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        
        test_market_data = {
            "symbol": "AAPL",
            "price": 150.0,
            "volume": 1000,
            "timestamp": "2024-01-15T10:00:00Z",
            "source": "test"
        }
        
        producer.send("market_data", test_market_data)
        producer.flush()
        producer.close()
        
        # Verify that feature engineering service processes the message
        await asyncio.sleep(5)  # Allow time for processing
        
        # Check feature engineering service logs
        fe_container = test_environment.containers["feature_engineering_service"]
        fe_logs = fe_container.logs().decode()
        
        # Should contain evidence of processing market data
        assert "AAPL" in fe_logs or "market_data" in fe_logs
    
    @pytest.mark.asyncio
    async def test_database_integration(self, test_environment):
        """Test that services can read/write to databases correctly."""
        # Test PostgreSQL integration
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="trading_platform_test",
            user="test_user",
            password="test_password"
        )
        
        with conn.cursor() as cursor:
            # Create a test table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10),
                    price DECIMAL(10,2),
                    quantity INTEGER,
                    timestamp TIMESTAMP
                )
            """)
            
            # Insert test data
            cursor.execute("""
                INSERT INTO test_trades (symbol, price, quantity, timestamp)
                VALUES (%s, %s, %s, %s)
            """, ("AAPL", 150.0, 100, "2024-01-15 10:00:00"))
            
            conn.commit()
            
            # Verify data
            cursor.execute("SELECT * FROM test_trades WHERE symbol = %s", ("AAPL",))
            result = cursor.fetchone()
            
            assert result[1] == "AAPL"
            assert float(result[2]) == 150.0
            assert result[3] == 100
        
        conn.close()
    
    @pytest.mark.asyncio
    async def test_error_handling_and_resilience(self, test_environment):
        """Test error handling and service resilience."""
        # Stop one service temporarily
        redis_container = test_environment.containers["redis"]
        redis_container.stop()
        
        # Wait a bit
        await asyncio.sleep(10)
        
        # Check that other services are still running
        for service_name in test_environment.microservices:
            container = test_environment.containers[service_name]
            container.reload()
            
            # Services should still be running (graceful degradation)
            assert container.status == "running"
        
        # Restart Redis
        redis_container.start()
        await asyncio.sleep(10)
        
        # Verify Redis is accessible again
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.ping()
    
    @pytest.mark.asyncio
    async def test_end_to_end_trading_workflow(self, test_environment):
        """Test complete end-to-end trading workflow."""
        # 1. Inject market data
        producer = KafkaProducer(
            bootstrap_servers=["localhost:29092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        
        market_data = {
            "symbol": "AAPL",
            "price": 150.0,
            "volume": 1000,
            "timestamp": "2024-01-15T10:00:00Z"
        }
        
        producer.send("market_data", market_data)
        
        # 2. Inject news data
        news_data = {
            "symbol": "AAPL",
            "headline": "Apple reports strong quarterly earnings",
            "content": "Apple Inc. reported better than expected earnings...",
            "timestamp": "2024-01-15T09:30:00Z",
            "source": "test_news"
        }
        
        producer.send("news_data", news_data)
        producer.flush()
        producer.close()
        
        # 3. Wait for processing
        await asyncio.sleep(30)
        
        # 4. Check that signals were generated
        consumer = KafkaConsumer(
            "trading_signals",
            bootstrap_servers=["localhost:29092"],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=10000,
            auto_offset_reset="earliest"
        )
        
        signals_received = []
        for message in consumer:
            signals_received.append(message.value)
            if len(signals_received) >= 1:  # Expect at least one signal
                break
        
        consumer.close()
        
        # Verify signal structure
        assert len(signals_received) > 0
        signal = signals_received[0]
        assert "symbol" in signal
        assert "signal" in signal
        assert "confidence" in signal
        assert signal["symbol"] == "AAPL"


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short"])

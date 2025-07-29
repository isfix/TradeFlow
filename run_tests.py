#!/usr/bin/env python3
"""
Test Runner for Trading Platform

Comprehensive test runner that can execute different types of tests
with proper environment setup and reporting.
"""

import os
import sys
import argparse
import subprocess
import time
import docker
from pathlib import Path
from typing import List, Dict, Any
import yaml


class TestRunner:
    """
    Comprehensive test runner for the trading platform.
    
    Manages test environments, executes different test suites,
    and provides detailed reporting.
    """
    
    def __init__(self):
        """Initialize the test runner."""
        self.project_root = Path(__file__).parent
        self.docker_client = docker.from_env()
        self.test_containers: List[str] = []
        
        # Test suites
        self.test_suites = {
            "unit": {
                "path": "tests/",
                "markers": "unit",
                "description": "Fast unit tests",
                "requires_infrastructure": False
            },
            "integration": {
                "path": "tests/integration/",
                "markers": "integration",
                "description": "Integration tests with infrastructure",
                "requires_infrastructure": True
            },
            "e2e": {
                "path": "tests/e2e/",
                "markers": "e2e",
                "description": "End-to-end system tests",
                "requires_infrastructure": True
            },
            "performance": {
                "path": "tests/performance/",
                "markers": "performance",
                "description": "Performance and load tests",
                "requires_infrastructure": True
            },
            "security": {
                "path": "tests/",
                "markers": "security",
                "description": "Security-related tests",
                "requires_infrastructure": False
            },
            "smoke": {
                "path": "tests/",
                "markers": "smoke",
                "description": "Basic smoke tests",
                "requires_infrastructure": True
            }
        }
    
    def setup_test_environment(self, suite_name: str) -> bool:
        """Set up test environment for the specified suite."""
        suite = self.test_suites.get(suite_name)
        if not suite:
            print(f"Unknown test suite: {suite_name}")
            return False
        
        if not suite["requires_infrastructure"]:
            print(f"No infrastructure setup required for {suite_name} tests")
            return True
        
        print(f"Setting up infrastructure for {suite_name} tests...")
        
        try:
            # Start test infrastructure using Docker Compose
            compose_file = self.project_root / "docker" / "docker-compose.test.yml"
            
            if not compose_file.exists():
                # Create test-specific compose file
                self._create_test_compose_file(compose_file)
            
            # Start infrastructure services
            cmd = [
                "docker-compose",
                "-f", str(compose_file),
                "up", "-d",
                "--wait"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Failed to start test infrastructure: {result.stderr}")
                return False
            
            # Wait for services to be ready
            print("Waiting for services to be ready...")
            time.sleep(30)
            
            # Verify services are healthy
            if not self._verify_infrastructure_health():
                print("Infrastructure health check failed")
                return False
            
            print("Test infrastructure is ready")
            return True
            
        except Exception as e:
            print(f"Error setting up test environment: {e}")
            return False
    
    def _create_test_compose_file(self, compose_file: Path):
        """Create a test-specific Docker Compose file."""
        test_compose = {
            "version": "3.8",
            "services": {
                "postgres-test": {
                    "image": "postgres:15-alpine",
                    "environment": {
                        "POSTGRES_DB": "trading_platform_test",
                        "POSTGRES_USER": "test_user",
                        "POSTGRES_PASSWORD": "test_password"
                    },
                    "ports": ["5432:5432"],
                    "healthcheck": {
                        "test": ["CMD-SHELL", "pg_isready -U test_user -d trading_platform_test"],
                        "interval": "10s",
                        "timeout": "5s",
                        "retries": 5
                    }
                },
                "redis-test": {
                    "image": "redis:7-alpine",
                    "ports": ["6379:6379"],
                    "healthcheck": {
                        "test": ["CMD", "redis-cli", "ping"],
                        "interval": "10s",
                        "timeout": "5s",
                        "retries": 5
                    }
                },
                "mongodb-test": {
                    "image": "mongo:7.0",
                    "environment": {
                        "MONGO_INITDB_ROOT_USERNAME": "test_user",
                        "MONGO_INITDB_ROOT_PASSWORD": "test_password"
                    },
                    "ports": ["27017:27017"],
                    "healthcheck": {
                        "test": ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"],
                        "interval": "10s",
                        "timeout": "5s",
                        "retries": 5
                    }
                },
                "kafka-test": {
                    "image": "confluentinc/cp-kafka:7.4.0",
                    "environment": {
                        "KAFKA_BROKER_ID": "1",
                        "KAFKA_ZOOKEEPER_CONNECT": "zookeeper-test:2181",
                        "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:29092",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1"
                    },
                    "ports": ["29092:9092"],
                    "depends_on": ["zookeeper-test"]
                },
                "zookeeper-test": {
                    "image": "confluentinc/cp-zookeeper:7.4.0",
                    "environment": {
                        "ZOOKEEPER_CLIENT_PORT": "2181",
                        "ZOOKEEPER_TICK_TIME": "2000"
                    },
                    "ports": ["2181:2181"]
                }
            }
        }
        
        with open(compose_file, 'w') as f:
            yaml.dump(test_compose, f, default_flow_style=False)
    
    def _verify_infrastructure_health(self) -> bool:
        """Verify that all infrastructure services are healthy."""
        try:
            # Check PostgreSQL
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="trading_platform_test",
                user="test_user",
                password="test_password"
            )
            conn.close()
            
            # Check Redis
            import redis
            r = redis.Redis(host="localhost", port=6379)
            r.ping()
            
            # Check MongoDB
            import pymongo
            client = pymongo.MongoClient("mongodb://test_user:test_password@localhost:27017/")
            client.admin.command("ping")
            client.close()
            
            return True
            
        except Exception as e:
            print(f"Infrastructure health check failed: {e}")
            return False
    
    def run_test_suite(self, suite_name: str, **kwargs) -> bool:
        """Run a specific test suite."""
        suite = self.test_suites.get(suite_name)
        if not suite:
            print(f"Unknown test suite: {suite_name}")
            return False
        
        print(f"\n{'='*60}")
        print(f"Running {suite_name} tests: {suite['description']}")
        print(f"{'='*60}")
        
        # Build pytest command
        cmd = ["python", "-m", "pytest"]
        
        # Add test path
        cmd.append(suite["path"])
        
        # Add markers
        if suite["markers"]:
            cmd.extend(["-m", suite["markers"]])
        
        # Add additional options
        if kwargs.get("verbose"):
            cmd.append("-v")
        
        if kwargs.get("coverage"):
            cmd.extend(["--cov=trading_platform", "--cov-report=term-missing"])
        
        if kwargs.get("parallel"):
            cmd.extend(["-n", "auto"])
        
        if kwargs.get("maxfail"):
            cmd.extend(["--maxfail", str(kwargs["maxfail"])])
        
        # Set environment variables
        env = os.environ.copy()
        env.update({
            "ENVIRONMENT": "testing",
            "LOG_LEVEL": "DEBUG",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "trading_platform_test",
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
            "MONGODB_HOST": "localhost",
            "MONGODB_PORT": "27017",
            "MONGODB_USER": "test_user",
            "MONGODB_PASSWORD": "test_password",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092"
        })
        
        # Run tests
        print(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, env=env)
        
        success = result.returncode == 0
        
        if success:
            print(f"\n✅ {suite_name} tests PASSED")
        else:
            print(f"\n❌ {suite_name} tests FAILED")
        
        return success
    
    def cleanup_test_environment(self):
        """Clean up test environment."""
        print("Cleaning up test environment...")
        
        try:
            compose_file = self.project_root / "docker" / "docker-compose.test.yml"
            
            if compose_file.exists():
                cmd = [
                    "docker-compose",
                    "-f", str(compose_file),
                    "down", "-v", "--remove-orphans"
                ]
                
                subprocess.run(cmd, capture_output=True)
            
            print("Test environment cleaned up")
            
        except Exception as e:
            print(f"Error cleaning up test environment: {e}")
    
    def run_all_tests(self, **kwargs) -> Dict[str, bool]:
        """Run all test suites."""
        results = {}
        
        # Run tests in order of complexity
        test_order = ["unit", "integration", "smoke", "e2e", "performance", "security"]
        
        for suite_name in test_order:
            if suite_name not in self.test_suites:
                continue
            
            # Set up environment if needed
            if self.test_suites[suite_name]["requires_infrastructure"]:
                if not self.setup_test_environment(suite_name):
                    results[suite_name] = False
                    continue
            
            # Run tests
            results[suite_name] = self.run_test_suite(suite_name, **kwargs)
            
            # Stop on first failure if requested
            if not results[suite_name] and kwargs.get("fail_fast"):
                break
        
        return results
    
    def generate_report(self, results: Dict[str, bool]):
        """Generate test results report."""
        print(f"\n{'='*60}")
        print("TEST RESULTS SUMMARY")
        print(f"{'='*60}")
        
        total_suites = len(results)
        passed_suites = sum(1 for result in results.values() if result)
        failed_suites = total_suites - passed_suites
        
        for suite_name, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            description = self.test_suites[suite_name]["description"]
            print(f"{suite_name:12} | {status} | {description}")
        
        print(f"\n{'='*60}")
        print(f"Total: {total_suites} | Passed: {passed_suites} | Failed: {failed_suites}")
        
        if failed_suites == 0:
            print("🎉 All tests passed!")
            return True
        else:
            print(f"💥 {failed_suites} test suite(s) failed")
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Trading Platform Test Runner")
    
    parser.add_argument(
        "suite",
        nargs="?",
        choices=list(TestRunner().test_suites.keys()) + ["all"],
        default="all",
        help="Test suite to run"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report"
    )
    
    parser.add_argument(
        "--parallel", "-n",
        action="store_true",
        help="Run tests in parallel"
    )
    
    parser.add_argument(
        "--maxfail",
        type=int,
        default=5,
        help="Stop after N failures"
    )
    
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first test suite failure"
    )
    
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't cleanup test environment"
    )
    
    args = parser.parse_args()
    
    runner = TestRunner()
    
    try:
        if args.suite == "all":
            results = runner.run_all_tests(
                verbose=args.verbose,
                coverage=args.coverage,
                parallel=args.parallel,
                maxfail=args.maxfail,
                fail_fast=args.fail_fast
            )
            
            success = runner.generate_report(results)
        else:
            # Set up environment if needed
            suite = runner.test_suites[args.suite]
            if suite["requires_infrastructure"]:
                if not runner.setup_test_environment(args.suite):
                    sys.exit(1)
            
            # Run single test suite
            success = runner.run_test_suite(
                args.suite,
                verbose=args.verbose,
                coverage=args.coverage,
                parallel=args.parallel,
                maxfail=args.maxfail
            )
        
        sys.exit(0 if success else 1)
        
    finally:
        if not args.no_cleanup:
            runner.cleanup_test_environment()


if __name__ == "__main__":
    main()

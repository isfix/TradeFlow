# Institutional Trading Platform

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://docker.com)
[![Microservices](https://img.shields.io/badge/Architecture-Microservices-green.svg)](https://microservices.io)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blue.svg)](https://postgresql.org)
[![Kafka](https://img.shields.io/badge/Messaging-Apache%20Kafka-orange.svg)](https://kafka.apache.org)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**A production-ready, institutional-grade algorithmic trading platform built with true microservices architecture, persistent state management, and comprehensive risk controls.**

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Security](#security)
- [Performance](#performance)
- [Development](#development)
- [API Documentation](#api-documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The **Institutional Trading Platform** is a comprehensive, production-ready algorithmic trading system designed for institutional-grade operations. Built with a true microservices architecture, it provides real-time market data processing, sophisticated signal generation, risk management, and automated execution capabilities.

### What Makes This Platform Institutional-Grade

- **True Microservices Architecture** - Individual service containers with independent scaling
- **Persistent State Management** - Database-backed state that survives service restarts
- **Single Source of Truth** - Authoritative data management with no inconsistencies
- **Request-Response Communication** - Real-time inter-service queries
- **Sophisticated Trade Matching** - FIFO algorithms with accurate P&L calculation
- **Real Feedback Loops** - Continuous model improvement based on performance
- **Production-Grade Recovery** - Automatic failure handling and state restoration
- **Comprehensive Risk Management** - Pre-trade validation and circuit breakers
- **Real-Time Market Data** - Live price feeds with fallback mechanisms
- **Complete Audit Trail** - Full transaction history and compliance reporting

---

## Architecture

![Trading Platform Architecture](assets/flowchart.jpg)

The platform implements a **true microservices architecture** where each service runs independently in its own Docker container, communicating through Apache Kafka and direct request-response mechanisms.

### System Flow

```
Market Data → Data Ingestion → Processing → Strategy → Risk Management → Execution → Feedback Loop
                    ↓              ↓          ↓            ↓              ↓           ↓
              PostgreSQL ← State Management ← Portfolio ← Risk Engine ← Orders ← Performance
```

### Service Categories

#### Data Ingestion Layer
- **Market Data Service** - Real-time price feeds and market data
- **News Service** - Financial news processing and sentiment extraction
- **Fundamental Service** - Company fundamentals and financial metrics

#### Processing Layer
- **Feature Engineering Service** - Technical indicators and market features
- **Sentiment Analysis Service** - LLM-powered news and social sentiment
- **Quant Model Service** - Machine learning predictions and signals

#### Strategy Layer
- **Signal Aggregation Service** - Multi-modal signal synthesis and weighting
- **Portfolio Management Service** - Position sizing and portfolio optimization

#### Execution Layer
- **Risk Management Service** - Pre-trade validation and circuit breakers
- **Order Gateway Service** - Broker integration and trade execution

#### Analysis Layer
- **Feedback Loop Service** - Performance attribution and model improvement

---

## Key Features

### Risk Management
- **Pre-trade Risk Validation** - Comprehensive checks before order execution
- **Position Limits** - Configurable limits on position sizes and concentrations
- **Circuit Breakers** - Automatic trading halts on adverse conditions
- **Real-time Portfolio Monitoring** - Continuous risk assessment
- **Margin and Leverage Controls** - Sophisticated margin management

### Portfolio Management
- **Real-time Portfolio Valuation** - Live position and P&L tracking
- **Sophisticated Position Sizing** - Kelly Criterion and risk-based sizing
- **Multi-asset Support** - Stocks, ETFs, and other instruments
- **Performance Attribution** - Detailed analysis of returns by strategy
- **Tax-aware Trading** - FIFO matching for accurate cost basis

### Signal Generation
- **Multi-modal Signals** - Technical, fundamental, and sentiment analysis
- **LLM Integration** - Advanced language models for market analysis
- **Machine Learning Models** - Quantitative prediction algorithms
- **Signal Confidence Scoring** - Probabilistic signal strength assessment
- **Ensemble Methods** - Combining multiple signal sources

### Real-time Processing
- **Low-latency Execution** - Optimized for high-frequency trading
- **Event-driven Architecture** - Reactive processing pipeline
- **Streaming Data Processing** - Real-time market data handling
- **Asynchronous Operations** - Non-blocking service communication
- **Horizontal Scaling** - Independent service scaling

### State Management
- **Persistent State** - Database-backed state management
- **Automatic Recovery** - State restoration after failures
- **Consistency Validation** - Automated data integrity checks
- **Transaction Management** - ACID-compliant operations
- **State Checkpointing** - Regular state snapshots for recovery

---

## Quick Start

### Prerequisites

| Component | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.9+ | Core runtime environment |
| **Docker** | 20.0+ | Container orchestration |
| **Docker Compose** | 2.0+ | Multi-container management |
| **PostgreSQL** | 13+ | Primary database |
| **Apache Kafka** | 2.8+ | Message streaming |
| **Redis** | 6.0+ | Caching and sessions |

### 30-Second Setup

```bash
# 1. Clone the repository
git clone https://github.com/your-org/trading-platform.git
cd trading_platform

# 2. Start the platform (includes all dependencies)
python cli.py start --build

# 3. Check status
python cli.py status

# 4. View real-time logs
python cli.py logs --follow
```

### Verify Installation

```bash
# Check all services are running
python cli.py status --detailed

# Run health checks
python cli.py health-check

# View platform metrics
python cli.py metrics
```

---

## Installation

### Development Setup

```bash
# 1. Clone and setup environment
git clone https://github.com/your-org/trading-platform.git
cd trading_platform

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup environment variables
cp .env.example .env
# Edit .env with your configuration

# 5. Initialize database
python cli.py init-db

# 6. Start development mode
python cli.py start --dev
```

### Production Deployment

```bash
# 1. Build production images
docker-compose -f docker/docker-compose.yml build

# 2. Deploy with production configuration
docker-compose -f docker/docker-compose.yml up -d

# 3. Verify deployment
docker-compose ps
```

---

## Configuration

### Configuration Files

The platform uses YAML configuration files located in the `configs/` directory:

```
configs/
├── services.yaml      # Service-specific settings
├── database.yaml      # Database connections
├── kafka_topics.yaml  # Message topics
└── secrets.yaml       # API keys and secrets
```

### Environment Variables

Key environment variables for configuration:

```bash
# Database Configuration
DATABASE_URL=postgresql://user:pass@localhost:5432/trading_db
REDIS_URL=redis://localhost:6379

# Message Broker
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# API Keys
POLYGON_API_KEY=your_polygon_key
ALPACA_API_KEY=your_alpaca_key
ALPACA_SECRET_KEY=your_alpaca_secret

# Trading Configuration
INITIAL_CAPITAL=1000000
MAX_POSITION_SIZE=50000
RISK_TOLERANCE=0.02
```

### Service Configuration

Each service can be configured independently:

```yaml
# services.yaml
portfolio_manager:
  initial_capital: 1000000
  max_position_percentage: 0.05
  rebalance_frequency: "1h"

risk_manager:
  max_daily_loss: 10000
  max_portfolio_leverage: 2.0
  circuit_breaker_threshold: 0.05

market_data:
  providers:
    - polygon
    - alpaca
  update_frequency: "1s"
```

---

## Usage

### Command Line Interface

The platform includes a comprehensive CLI for management:

```bash
# Platform Management
python cli.py start [--build] [--dev]     # Start all services
python cli.py stop                        # Stop all services
python cli.py restart                     # Restart platform
python cli.py status [--detailed]         # Check service status

# Service Operations
python cli.py scale <service> <replicas>  # Scale specific service
python cli.py logs <service> [--follow]   # View service logs
python cli.py exec <service> <command>    # Execute command in service

# Database Operations
python cli.py init-db                     # Initialize database
python cli.py migrate                     # Run database migrations
python cli.py backup                      # Create database backup
python cli.py restore <backup_file>       # Restore from backup

# Monitoring
python cli.py health-check                # Run health checks
python cli.py metrics                     # View platform metrics
python cli.py performance                 # Performance analysis

# Development
python cli.py test [--coverage]           # Run test suite
python cli.py lint                        # Code quality checks
python cli.py format                      # Format code
```

### Web Dashboard

Access the web dashboard at `http://localhost:3000` for:

- **Real-time Portfolio Monitoring** - Live positions and P&L
- **Performance Analytics** - Strategy performance metrics
- **Risk Dashboard** - Current risk exposure and limits
- **Trade History** - Complete transaction log
- **System Health** - Service status and metrics

### API Endpoints

The platform exposes RESTful APIs for integration:

```bash
# Portfolio API
GET    /api/v1/portfolio/state           # Current portfolio state
GET    /api/v1/portfolio/positions       # All positions
GET    /api/v1/portfolio/performance     # Performance metrics

# Trading API
POST   /api/v1/orders                    # Submit new order
GET    /api/v1/orders/{id}               # Get order status
DELETE /api/v1/orders/{id}               # Cancel order

# Market Data API
GET    /api/v1/market/price/{symbol}     # Latest price
GET    /api/v1/market/history/{symbol}   # Historical data
WS     /api/v1/market/stream             # Real-time stream

# Risk API
GET    /api/v1/risk/limits               # Current risk limits
POST   /api/v1/risk/validate             # Validate trade
POST   /api/v1/risk/circuit-breaker      # Circuit breaker control
```

---

## Monitoring

### Grafana Dashboards

Access comprehensive monitoring at `http://localhost:3000`:

#### Portfolio Dashboard
- Real-time portfolio value and P&L
- Position breakdown by symbol and sector
- Daily/weekly/monthly performance charts
- Risk metrics and exposure analysis

#### Trading Dashboard
- Order flow and execution metrics
- Trade success rates and slippage analysis
- Signal performance attribution
- Market impact analysis

#### System Dashboard
- Service health and uptime metrics
- Database performance and query times
- Message queue throughput and latency
- Resource utilization (CPU, memory, disk)

### Alerting

Automated alerts for critical events:

```yaml
# Alert Configuration
alerts:
  portfolio:
    daily_loss_threshold: -5000
    position_concentration: 0.15
    margin_utilization: 0.8

  system:
    service_downtime: 30s
    database_latency: 100ms
    message_queue_lag: 1000

  trading:
    order_rejection_rate: 0.05
    execution_latency: 5s
    circuit_breaker_activation: immediate
```

### Logging

Structured logging with multiple levels:

```bash
# View logs by service
python cli.py logs portfolio-manager --level INFO
python cli.py logs risk-manager --level ERROR
python cli.py logs market-data --level DEBUG

# Search logs
python cli.py logs --search "order execution"
python cli.py logs --grep "ERROR|CRITICAL"

# Export logs
python cli.py logs --export --format json --output logs.json
```

---

## Testing

### Test Suite

Comprehensive testing strategy with multiple test types:

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python -m pytest tests/unit/           # Unit tests
python -m pytest tests/integration/    # Integration tests
python -m pytest tests/e2e/           # End-to-end tests
python -m pytest tests/performance/    # Performance tests

# Run with coverage
python -m pytest tests/ --cov=trading_platform --cov-report=html

# Run specific service tests
python -m pytest tests/test_portfolio_manager.py
python -m pytest tests/test_risk_manager.py
```

### Test Categories

#### Unit Tests (`tests/unit/`)
- Individual component testing
- Mock external dependencies
- Fast execution (< 1 second per test)
- 95%+ code coverage target

#### Integration Tests (`tests/integration/`)
- Service-to-service communication
- Database integration
- Message queue functionality
- Real component interactions

#### End-to-End Tests (`tests/e2e/`)
- Complete trading workflows
- Full system integration
- Real market data simulation
- Performance validation

#### Performance Tests (`tests/performance/`)
- Load testing with high message volumes
- Latency measurement under stress
- Memory usage and leak detection
- Scalability validation

### Test Environment

```bash
# Setup test environment
python cli.py test-env setup

# Run tests in isolated environment
python cli.py test-env run

# Cleanup test environment
python cli.py test-env cleanup
```

---

## Security

### Security Features

#### Authentication & Authorization
- JWT-based API authentication
- Role-based access control (RBAC)
- Service-to-service authentication
- API rate limiting and throttling

#### Data Protection
- Encryption at rest (database)
- Encryption in transit (TLS/SSL)
- Sensitive data masking in logs
- Secure secret management

#### Network Security
- Container network isolation
- Firewall rules and port restrictions
- VPN access for production environments
- DDoS protection and monitoring

### Secret Management

```bash
# Manage secrets securely
python cli.py secrets set POLYGON_API_KEY "your_key_here"
python cli.py secrets get POLYGON_API_KEY
python cli.py secrets list
python cli.py secrets rotate ALPACA_API_KEY
```

### Security Scanning

```bash
# Run security scans
python cli.py security scan              # Full security scan
python cli.py security vulnerabilities  # Check for vulnerabilities
python cli.py security compliance       # Compliance checks
python cli.py security audit           # Security audit report
```

---

## Performance

### Performance Characteristics

| Metric | Target | Typical |
|--------|--------|---------|
| **Order Latency** | < 10ms | 3-5ms |
| **Market Data Latency** | < 5ms | 1-2ms |
| **Portfolio Update** | < 50ms | 20-30ms |
| **Risk Check** | < 20ms | 5-10ms |
| **Database Query** | < 100ms | 10-50ms |
| **Message Throughput** | 10k/sec | 5k/sec |

### Optimization Features

#### Database Optimization
- Connection pooling (20 connections per service)
- Query optimization with proper indexing
- Read replicas for analytics queries
- Automatic vacuum and maintenance

#### Caching Strategy
- Redis caching for frequently accessed data
- In-memory caching for market data
- CDN for static assets
- Query result caching

#### Message Queue Optimization
- Kafka partitioning for parallel processing
- Batch processing for high-volume data
- Compression for large messages
- Dead letter queues for error handling

### Performance Monitoring

```bash
# Performance analysis
python cli.py performance analyze
python cli.py performance benchmark
python cli.py performance profile <service>
python cli.py performance optimize
```

---

## Development

### Development Workflow

```bash
# 1. Setup development environment
git clone https://github.com/your-org/trading-platform.git
cd trading_platform
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt

# 2. Start development services
python cli.py start --dev

# 3. Make changes and test
python cli.py test
python cli.py lint

# 4. Submit changes
git add .
git commit -m "feat: add new feature"
git push origin feature-branch
```

### Adding New Services

```bash
# 1. Create service structure
python cli.py create-service my_new_service

# 2. Implement service logic
# Edit services/my_category/my_new_service/

# 3. Add to Docker Compose
# Edit docker/docker-compose.yml

# 4. Update CLI management
# Edit cli.py

# 5. Add tests
# Create tests/test_my_new_service.py

# 6. Update documentation
# Edit README.md and service docs
```

### Code Standards

#### Code Quality
- **Black** for code formatting
- **Flake8** for linting
- **MyPy** for type checking
- **isort** for import sorting

#### Documentation
- Comprehensive docstrings for all functions
- Type hints for all parameters and returns
- README files for each service
- API documentation with examples

#### Testing Requirements
- Unit tests for all new functions
- Integration tests for service interactions
- Performance tests for critical paths
- Documentation tests for examples

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/ --cov
      - name: Security scan
        run: python cli.py security scan
      - name: Performance test
        run: python cli.py performance benchmark
```

---

## API Documentation

### REST API

Complete API documentation available at `http://localhost:8000/docs` when running.

#### Portfolio Management

```python
# Get portfolio state
GET /api/v1/portfolio/state
Response: {
    "portfolio_id": "default_portfolio",
    "total_value": 1050000.00,
    "cash_balance": 50000.00,
    "positions": [
        {
            "symbol": "AAPL",
            "quantity": 100,
            "avg_price": 150.00,
            "market_value": 15000.00,
            "unrealized_pnl": 500.00
        }
    ],
    "last_updated": "2024-01-15T10:30:00Z"
}

# Get position details
GET /api/v1/portfolio/positions/{symbol}
Response: {
    "symbol": "AAPL",
    "quantity": 100,
    "avg_price": 150.00,
    "current_price": 155.00,
    "market_value": 15500.00,
    "unrealized_pnl": 500.00,
    "cost_basis": 15000.00,
    "last_updated": "2024-01-15T10:30:00Z"
}
```

#### Order Management

```python
# Submit new order
POST /api/v1/orders
Request: {
    "symbol": "AAPL",
    "side": "BUY",
    "quantity": 100,
    "order_type": "MARKET",
    "time_in_force": "DAY"
}
Response: {
    "order_id": "ord_123456",
    "status": "PENDING",
    "submitted_at": "2024-01-15T10:30:00Z"
}

# Get order status
GET /api/v1/orders/{order_id}
Response: {
    "order_id": "ord_123456",
    "symbol": "AAPL",
    "side": "BUY",
    "quantity": 100,
    "filled_quantity": 100,
    "avg_fill_price": 150.25,
    "status": "FILLED",
    "submitted_at": "2024-01-15T10:30:00Z",
    "filled_at": "2024-01-15T10:30:15Z"
}
```

### WebSocket API

Real-time data streams:

```javascript
// Market data stream
const ws = new WebSocket('ws://localhost:8000/api/v1/market/stream');

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('Market update:', data);
    // {
    //     "symbol": "AAPL",
    //     "price": 150.25,
    //     "volume": 1000,
    //     "timestamp": "2024-01-15T10:30:00Z"
    // }
};

// Portfolio updates stream
const portfolioWs = new WebSocket('ws://localhost:8000/api/v1/portfolio/stream');

portfolioWs.onmessage = function(event) {
    const update = JSON.parse(event.data);
    console.log('Portfolio update:', update);
    // {
    //     "type": "position_update",
    //     "symbol": "AAPL",
    //     "new_quantity": 200,
    //     "new_value": 30050.00
    // }
};
```

---

## Contributing

We welcome contributions from the community! Please follow these guidelines:

### Contribution Process

1. **Fork the repository** and create a feature branch
2. **Make your changes** following our code standards
3. **Add tests** for new functionality
4. **Update documentation** as needed
5. **Submit a pull request** with a clear description

### Code Review Process

- All changes require review from at least 2 maintainers
- Automated tests must pass
- Security scan must pass
- Performance impact must be assessed
- Documentation must be updated

### Bug Reports

Please include:
- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details
- Relevant logs or error messages

### Feature Requests

Please include:
- Clear description of the feature
- Use case and business justification
- Proposed implementation approach
- Potential impact on existing functionality

---

### Acknowledgments

- Built with [Python](https://python.org) and [Docker](https://docker.com)
- Powered by [PostgreSQL](https://postgresql.org) and [Apache Kafka](https://kafka.apache.org)
- Monitoring with [Grafana](https://grafana.com) and [Prometheus](https://prometheus.io)
- Testing with [pytest](https://pytest.org) and [coverage.py](https://coverage.readthedocs.io)


"""
Database Schema Definitions

Comprehensive database schema for persistent state management
in the trading platform.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Decimal, DateTime, 
    Boolean, Text, ForeignKey, Index, UniqueConstraint, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB
from datetime import datetime
import uuid


Base = declarative_base()


class Portfolio(Base):
    """Portfolio table for persistent portfolio state."""
    
    __tablename__ = 'portfolios'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False)
    cash_balance = Column(Decimal(20, 8), nullable=False, default=0)
    total_value = Column(Decimal(20, 8), nullable=False, default=0)
    buying_power = Column(Decimal(20, 8), nullable=False, default=0)
    margin_used = Column(Decimal(20, 8), nullable=False, default=0)
    total_pnl = Column(Decimal(20, 8), nullable=False, default=0)
    daily_pnl = Column(Decimal(20, 8), nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    positions = relationship("Position", back_populates="portfolio", cascade="all, delete-orphan")
    trades = relationship("Trade", back_populates="portfolio")
    
    __table_args__ = (
        CheckConstraint('cash_balance >= 0', name='positive_cash'),
        CheckConstraint('total_value >= 0', name='positive_total_value'),
        CheckConstraint('buying_power >= 0', name='positive_buying_power'),
        CheckConstraint('margin_used >= 0', name='positive_margin'),
    )


class Position(Base):
    """Position table for individual stock positions."""
    
    __tablename__ = 'positions'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    portfolio_id = Column(String(50), ForeignKey('portfolios.id'), nullable=False)
    symbol = Column(String(20), nullable=False)
    quantity = Column(Decimal(20, 8), nullable=False, default=0)
    avg_price = Column(Decimal(20, 8), nullable=False, default=0)
    market_value = Column(Decimal(20, 8), nullable=False, default=0)
    unrealized_pnl = Column(Decimal(20, 8), nullable=False, default=0)
    realized_pnl = Column(Decimal(20, 8), nullable=False, default=0)
    cost_basis = Column(Decimal(20, 8), nullable=False, default=0)
    last_price = Column(Decimal(20, 8), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="positions")
    
    __table_args__ = (
        UniqueConstraint('portfolio_id', 'symbol', name='unique_portfolio_symbol'),
        Index('idx_portfolio_symbol', 'portfolio_id', 'symbol'),
        CheckConstraint('quantity >= 0', name='positive_quantity'),
        CheckConstraint('avg_price >= 0', name='positive_avg_price'),
    )


class Trade(Base):
    """Trade table for executed trades."""
    
    __tablename__ = 'trades'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    portfolio_id = Column(String(50), ForeignKey('portfolios.id'), nullable=False)
    order_id = Column(String(50), nullable=True)
    symbol = Column(String(20), nullable=False)
    side = Column(String(10), nullable=False)  # BUY, SELL
    quantity = Column(Decimal(20, 8), nullable=False)
    price = Column(Decimal(20, 8), nullable=False)
    total_value = Column(Decimal(20, 8), nullable=False)
    commission = Column(Decimal(20, 8), nullable=False, default=0)
    realized_pnl = Column(Decimal(20, 8), nullable=True)
    execution_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    settlement_date = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default='EXECUTED')
    broker = Column(String(50), nullable=True)
    metadata = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="trades")
    
    __table_args__ = (
        Index('idx_portfolio_symbol_time', 'portfolio_id', 'symbol', 'execution_time'),
        Index('idx_execution_time', 'execution_time'),
        CheckConstraint("side IN ('BUY', 'SELL')", name='valid_side'),
        CheckConstraint('quantity > 0', name='positive_quantity'),
        CheckConstraint('price > 0', name='positive_price'),
        CheckConstraint('total_value > 0', name='positive_total_value'),
    )


class TradingSignal(Base):
    """Trading signals table for signal persistence."""
    
    __tablename__ = 'trading_signals'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    symbol = Column(String(20), nullable=False)
    signal = Column(String(20), nullable=False)  # BUY, SELL, HOLD, etc.
    confidence = Column(Decimal(5, 4), nullable=False)  # 0.0000 to 1.0000
    reasoning = Column(Text, nullable=True)
    source_service = Column(String(50), nullable=False)
    signal_data = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default='PENDING')
    
    __table_args__ = (
        Index('idx_symbol_created', 'symbol', 'created_at'),
        Index('idx_status_created', 'status', 'created_at'),
        CheckConstraint("signal IN ('BUY', 'SELL', 'HOLD', 'STRONG_BUY', 'STRONG_SELL')", name='valid_signal'),
        CheckConstraint('confidence >= 0 AND confidence <= 1', name='valid_confidence'),
    )


class RiskMetrics(Base):
    """Risk metrics table for risk management state."""
    
    __tablename__ = 'risk_metrics'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    portfolio_id = Column(String(50), ForeignKey('portfolios.id'), nullable=False)
    metric_type = Column(String(50), nullable=False)  # VAR, SHARPE, BETA, etc.
    metric_value = Column(Decimal(20, 8), nullable=False)
    calculation_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    lookback_period = Column(Integer, nullable=True)
    confidence_level = Column(Decimal(5, 4), nullable=True)
    metadata = Column(JSONB, nullable=True)
    
    __table_args__ = (
        Index('idx_portfolio_metric_date', 'portfolio_id', 'metric_type', 'calculation_date'),
    )


class MarketData(Base):
    """Market data table for price history."""
    
    __tablename__ = 'market_data'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    symbol = Column(String(20), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open_price = Column(Decimal(20, 8), nullable=True)
    high_price = Column(Decimal(20, 8), nullable=True)
    low_price = Column(Decimal(20, 8), nullable=True)
    close_price = Column(Decimal(20, 8), nullable=False)
    volume = Column(Integer, nullable=False, default=0)
    source = Column(String(50), nullable=False)
    data_type = Column(String(20), nullable=False, default='TICK')  # TICK, BAR_1M, BAR_5M, etc.
    
    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', 'data_type', 'source', name='unique_market_data'),
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_timestamp', 'timestamp'),
        CheckConstraint('close_price > 0', name='positive_close_price'),
        CheckConstraint('volume >= 0', name='non_negative_volume'),
    )


class PerformanceMetrics(Base):
    """Performance metrics table for tracking strategy performance."""
    
    __tablename__ = 'performance_metrics'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    portfolio_id = Column(String(50), ForeignKey('portfolios.id'), nullable=False)
    calculation_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    total_return = Column(Decimal(10, 6), nullable=False)
    sharpe_ratio = Column(Decimal(10, 6), nullable=True)
    max_drawdown = Column(Decimal(10, 6), nullable=True)
    win_rate = Column(Decimal(5, 4), nullable=True)
    avg_win = Column(Decimal(10, 6), nullable=True)
    avg_loss = Column(Decimal(10, 6), nullable=True)
    total_trades = Column(Integer, nullable=False, default=0)
    winning_trades = Column(Integer, nullable=False, default=0)
    losing_trades = Column(Integer, nullable=False, default=0)
    
    __table_args__ = (
        Index('idx_portfolio_date', 'portfolio_id', 'calculation_date'),
    )


class SystemState(Base):
    """System state table for service state persistence."""
    
    __tablename__ = 'system_state'
    
    id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    service_name = Column(String(100), nullable=False)
    state_key = Column(String(100), nullable=False)
    state_value = Column(JSONB, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('service_name', 'state_key', name='unique_service_state'),
        Index('idx_service_key', 'service_name', 'state_key'),
    )


def create_database_engine(database_url: str):
    """Create database engine with proper configuration."""
    engine = create_engine(
        database_url,
        pool_size=20,
        max_overflow=30,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False  # Set to True for SQL debugging
    )
    return engine


def create_tables(engine):
    """Create all tables in the database."""
    Base.metadata.create_all(engine)


def get_session_factory(engine):
    """Get session factory for database operations."""
    return sessionmaker(bind=engine)


def init_database(database_url: str):
    """Initialize database with all tables."""
    engine = create_database_engine(database_url)
    create_tables(engine)
    return engine, get_session_factory(engine)

"""
Persistent State Manager

Database-backed state management for critical trading platform services.
Ensures all state is persisted and can survive service restarts.
"""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime, timezone
from contextlib import contextmanager
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, desc, func

from trading_platform.core.database_schema import (
    Portfolio, Position, Trade, TradingSignal, RiskMetrics,
    MarketData, PerformanceMetrics, SystemState, init_database
)
from trading_platform.core.secrets_manager import get_secrets_manager


class PersistentStateManager:
    """
    Database-backed state manager for trading platform services.
    
    Provides ACID-compliant state persistence with automatic recovery,
    ensuring no data loss on service restarts.
    """
    
    def __init__(self, service_name: str):
        """
        Initialize persistent state manager.
        
        Args:
            service_name: Name of the service using this state manager
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize database connection
        secrets_manager = get_secrets_manager()
        db_config = secrets_manager.get_database_config('postgres')
        
        database_url = (
            f"postgresql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        self.engine, self.session_factory = init_database(database_url)
        self.logger.info(f"Initialized persistent state manager for {service_name}")
    
    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def save_portfolio_state(self, portfolio_data: Dict[str, Any]) -> str:
        """
        Save portfolio state to database.
        
        Args:
            portfolio_data: Portfolio state data
            
        Returns:
            Portfolio ID
        """
        try:
            with self.get_session() as session:
                # Check if portfolio exists
                portfolio_id = portfolio_data.get('id')
                portfolio = None
                
                if portfolio_id:
                    portfolio = session.query(Portfolio).filter_by(id=portfolio_id).first()
                
                if portfolio:
                    # Update existing portfolio
                    portfolio.cash_balance = Decimal(str(portfolio_data['cash_balance']))
                    portfolio.total_value = Decimal(str(portfolio_data['total_value']))
                    portfolio.buying_power = Decimal(str(portfolio_data['buying_power']))
                    portfolio.margin_used = Decimal(str(portfolio_data['margin_used']))
                    portfolio.total_pnl = Decimal(str(portfolio_data.get('total_pnl', 0)))
                    portfolio.daily_pnl = Decimal(str(portfolio_data.get('daily_pnl', 0)))
                    portfolio.updated_at = datetime.utcnow()
                else:
                    # Create new portfolio
                    portfolio = Portfolio(
                        id=portfolio_id or f"portfolio_{self.service_name}_{int(datetime.utcnow().timestamp())}",
                        name=portfolio_data.get('name', f'Portfolio {self.service_name}'),
                        cash_balance=Decimal(str(portfolio_data['cash_balance'])),
                        total_value=Decimal(str(portfolio_data['total_value'])),
                        buying_power=Decimal(str(portfolio_data['buying_power'])),
                        margin_used=Decimal(str(portfolio_data['margin_used'])),
                        total_pnl=Decimal(str(portfolio_data.get('total_pnl', 0))),
                        daily_pnl=Decimal(str(portfolio_data.get('daily_pnl', 0)))
                    )
                    session.add(portfolio)
                    session.flush()  # Get the ID
                
                # Update positions
                positions_data = portfolio_data.get('positions', {})
                for symbol, position_data in positions_data.items():
                    self._save_position(session, portfolio.id, symbol, position_data)
                
                session.commit()
                self.logger.debug(f"Saved portfolio state for {portfolio.id}")
                return portfolio.id
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error saving portfolio state: {e}")
            raise
    
    def _save_position(self, session: Session, portfolio_id: str, symbol: str, position_data: Dict[str, Any]):
        """Save individual position to database."""
        position = session.query(Position).filter_by(
            portfolio_id=portfolio_id, 
            symbol=symbol
        ).first()
        
        if position:
            # Update existing position
            position.quantity = Decimal(str(position_data['quantity']))
            position.avg_price = Decimal(str(position_data['avg_price']))
            position.market_value = Decimal(str(position_data['market_value']))
            position.unrealized_pnl = Decimal(str(position_data.get('unrealized_pnl', 0)))
            position.realized_pnl = Decimal(str(position_data.get('realized_pnl', 0)))
            position.cost_basis = Decimal(str(position_data.get('cost_basis', 0)))
            position.last_price = Decimal(str(position_data.get('last_price', 0))) if position_data.get('last_price') else None
            position.updated_at = datetime.utcnow()
        else:
            # Create new position
            position = Position(
                portfolio_id=portfolio_id,
                symbol=symbol,
                quantity=Decimal(str(position_data['quantity'])),
                avg_price=Decimal(str(position_data['avg_price'])),
                market_value=Decimal(str(position_data['market_value'])),
                unrealized_pnl=Decimal(str(position_data.get('unrealized_pnl', 0))),
                realized_pnl=Decimal(str(position_data.get('realized_pnl', 0))),
                cost_basis=Decimal(str(position_data.get('cost_basis', 0))),
                last_price=Decimal(str(position_data.get('last_price', 0))) if position_data.get('last_price') else None
            )
            session.add(position)
    
    def load_portfolio_state(self, portfolio_id: str) -> Optional[Dict[str, Any]]:
        """
        Load portfolio state from database.
        
        Args:
            portfolio_id: Portfolio ID to load
            
        Returns:
            Portfolio state data or None if not found
        """
        try:
            with self.get_session() as session:
                portfolio = session.query(Portfolio).filter_by(id=portfolio_id).first()
                
                if not portfolio:
                    return None
                
                # Load positions
                positions = {}
                for position in portfolio.positions:
                    positions[position.symbol] = {
                        'quantity': float(position.quantity),
                        'avg_price': float(position.avg_price),
                        'market_value': float(position.market_value),
                        'unrealized_pnl': float(position.unrealized_pnl),
                        'realized_pnl': float(position.realized_pnl),
                        'cost_basis': float(position.cost_basis),
                        'last_price': float(position.last_price) if position.last_price else None,
                        'updated_at': position.updated_at.isoformat()
                    }
                
                return {
                    'id': portfolio.id,
                    'name': portfolio.name,
                    'cash_balance': float(portfolio.cash_balance),
                    'total_value': float(portfolio.total_value),
                    'buying_power': float(portfolio.buying_power),
                    'margin_used': float(portfolio.margin_used),
                    'total_pnl': float(portfolio.total_pnl),
                    'daily_pnl': float(portfolio.daily_pnl),
                    'positions': positions,
                    'created_at': portfolio.created_at.isoformat(),
                    'updated_at': portfolio.updated_at.isoformat()
                }
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error loading portfolio state: {e}")
            return None
    
    def save_trade(self, trade_data: Dict[str, Any]) -> str:
        """
        Save executed trade to database.
        
        Args:
            trade_data: Trade execution data
            
        Returns:
            Trade ID
        """
        try:
            with self.get_session() as session:
                trade = Trade(
                    portfolio_id=trade_data['portfolio_id'],
                    order_id=trade_data.get('order_id'),
                    symbol=trade_data['symbol'],
                    side=trade_data['side'],
                    quantity=Decimal(str(trade_data['quantity'])),
                    price=Decimal(str(trade_data['price'])),
                    total_value=Decimal(str(trade_data['total_value'])),
                    commission=Decimal(str(trade_data.get('commission', 0))),
                    realized_pnl=Decimal(str(trade_data.get('realized_pnl', 0))) if trade_data.get('realized_pnl') else None,
                    execution_time=datetime.fromisoformat(trade_data['execution_time']) if isinstance(trade_data['execution_time'], str) else trade_data['execution_time'],
                    status=trade_data.get('status', 'EXECUTED'),
                    broker=trade_data.get('broker'),
                    metadata=trade_data.get('metadata')
                )
                
                session.add(trade)
                session.flush()
                
                self.logger.debug(f"Saved trade {trade.id} for {trade.symbol}")
                return trade.id
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error saving trade: {e}")
            raise
    
    def get_portfolio_trades(self, portfolio_id: str, symbol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trades for a portfolio.
        
        Args:
            portfolio_id: Portfolio ID
            symbol: Optional symbol filter
            limit: Maximum number of trades to return
            
        Returns:
            List of trade data
        """
        try:
            with self.get_session() as session:
                query = session.query(Trade).filter_by(portfolio_id=portfolio_id)
                
                if symbol:
                    query = query.filter_by(symbol=symbol)
                
                trades = query.order_by(desc(Trade.execution_time)).limit(limit).all()
                
                return [
                    {
                        'id': trade.id,
                        'order_id': trade.order_id,
                        'symbol': trade.symbol,
                        'side': trade.side,
                        'quantity': float(trade.quantity),
                        'price': float(trade.price),
                        'total_value': float(trade.total_value),
                        'commission': float(trade.commission),
                        'realized_pnl': float(trade.realized_pnl) if trade.realized_pnl else None,
                        'execution_time': trade.execution_time.isoformat(),
                        'status': trade.status,
                        'broker': trade.broker,
                        'metadata': trade.metadata
                    }
                    for trade in trades
                ]
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting portfolio trades: {e}")
            return []
    
    def save_service_state(self, state_key: str, state_value: Any) -> None:
        """
        Save arbitrary service state.
        
        Args:
            state_key: Key for the state
            state_value: State value (will be JSON serialized)
        """
        try:
            with self.get_session() as session:
                state = session.query(SystemState).filter_by(
                    service_name=self.service_name,
                    state_key=state_key
                ).first()
                
                if state:
                    state.state_value = state_value
                    state.version += 1
                    state.updated_at = datetime.utcnow()
                else:
                    state = SystemState(
                        service_name=self.service_name,
                        state_key=state_key,
                        state_value=state_value
                    )
                    session.add(state)
                
                self.logger.debug(f"Saved service state {state_key}")
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error saving service state: {e}")
            raise
    
    def load_service_state(self, state_key: str) -> Optional[Any]:
        """
        Load service state.
        
        Args:
            state_key: Key for the state
            
        Returns:
            State value or None if not found
        """
        try:
            with self.get_session() as session:
                state = session.query(SystemState).filter_by(
                    service_name=self.service_name,
                    state_key=state_key
                ).first()
                
                return state.state_value if state else None
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error loading service state: {e}")
            return None
    
    def get_latest_market_price(self, symbol: str) -> Optional[Decimal]:
        """
        Get latest market price for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Latest price or None if not found
        """
        try:
            with self.get_session() as session:
                latest_data = session.query(MarketData).filter_by(
                    symbol=symbol
                ).order_by(desc(MarketData.timestamp)).first()
                
                return latest_data.close_price if latest_data else None
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting latest market price: {e}")
            return None

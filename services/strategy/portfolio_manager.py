"""
Portfolio Manager

Translates abstract signals into concrete, sized trade orders based on current
portfolio state with sophisticated position sizing and risk management.
"""

import logging
import threading
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_DOWN
import json

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage, RequestMessage, ResponseMessage
from ...core.database_clients import get_database_manager
from ...core.persistent_state_manager import PersistentStateManager
from ...configs import load_services_config, get_topic_name


@dataclass
class Position:
    """Represents a portfolio position."""
    symbol: str
    quantity: Decimal
    average_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    last_updated: datetime


@dataclass
class PortfolioState:
    """Current state of the portfolio."""
    cash_balance: Decimal
    total_value: Decimal
    positions: Dict[str, Position]
    buying_power: Decimal
    margin_used: Decimal
    last_updated: datetime


@dataclass
class ProposedTradeOrder:
    """Proposed trade order with full specifications."""
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: Decimal
    order_type: str  # 'MARKET', 'LIMIT', 'STOP'
    price: Optional[Decimal]  # For limit orders
    time_in_force: str  # 'DAY', 'GTC', 'IOC', 'FOK'
    signal_source: str
    signal_confidence: float
    reasoning: str
    risk_metrics: Dict[str, Any]
    correlation_id: str
    timestamp: datetime


class PositionSizer:
    """Implements position sizing algorithms."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize position sizer.
        
        Args:
            config: Position sizing configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.PositionSizer")
    
    def calculate_position_size(self, signal: Dict[str, Any], portfolio_state: PortfolioState,
                              current_price: Decimal) -> Tuple[Decimal, Dict[str, Any]]:
        """
        Calculate position size based on signal and portfolio state.
        
        Args:
            signal: Trading signal
            portfolio_state: Current portfolio state
            current_price: Current market price
            
        Returns:
            Tuple of (position_size, risk_metrics)
        """
        try:
            signal_strength = signal.get('signal', 'HOLD')
            confidence = float(signal.get('confidence', 0.0))
            
            # Get sizing parameters
            max_position_pct = self.config.get('max_position_percentage', 0.05)  # 5% max
            base_position_pct = self.config.get('base_position_percentage', 0.02)  # 2% base
            
            # Adjust position size based on signal strength and confidence
            if signal_strength == 'STRONG_BUY':
                target_pct = max_position_pct * confidence
            elif signal_strength == 'BUY':
                target_pct = base_position_pct * confidence
            elif signal_strength == 'STRONG_SELL':
                # For sell signals, calculate based on current position
                target_pct = -max_position_pct * confidence
            elif signal_strength == 'SELL':
                target_pct = -base_position_pct * confidence
            else:  # HOLD
                return Decimal('0'), {'reason': 'HOLD signal'}
            
            # Calculate dollar amount
            target_dollar_amount = portfolio_state.total_value * Decimal(str(target_pct))
            
            # Calculate shares
            if current_price > 0:
                target_shares = target_dollar_amount / current_price
                # Round down to avoid over-allocation
                target_shares = target_shares.quantize(Decimal('1'), rounding=ROUND_DOWN)
            else:
                target_shares = Decimal('0')
            
            # Risk metrics
            risk_metrics = {
                'target_percentage': target_pct,
                'target_dollar_amount': float(target_dollar_amount),
                'confidence_factor': confidence,
                'signal_strength': signal_strength,
                'current_price': float(current_price)
            }
            
            return target_shares, risk_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {e}")
            return Decimal('0'), {'error': str(e)}
    
    def apply_kelly_criterion(self, win_rate: float, avg_win: float, avg_loss: float,
                            base_size: Decimal) -> Decimal:
        """
        Apply Kelly Criterion for position sizing.
        
        Args:
            win_rate: Historical win rate (0.0 to 1.0)
            avg_win: Average winning trade percentage
            avg_loss: Average losing trade percentage
            base_size: Base position size
            
        Returns:
            Kelly-adjusted position size
        """
        try:
            if avg_loss <= 0 or win_rate <= 0:
                return base_size
            
            # Kelly formula: f = (bp - q) / b
            # where b = avg_win/avg_loss, p = win_rate, q = 1 - win_rate
            b = avg_win / abs(avg_loss)
            p = win_rate
            q = 1 - win_rate
            
            kelly_fraction = (b * p - q) / b
            
            # Cap Kelly fraction to avoid excessive leverage
            kelly_fraction = max(0, min(kelly_fraction, 0.25))  # Max 25%
            
            return base_size * Decimal(str(kelly_fraction))
            
        except Exception as e:
            self.logger.error(f"Error applying Kelly criterion: {e}")
            return base_size


class RiskManager:
    """Manages portfolio-level risk constraints."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize risk manager.
        
        Args:
            config: Risk management configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.RiskManager")
    
    def validate_trade(self, proposed_order: ProposedTradeOrder, 
                      portfolio_state: PortfolioState) -> Tuple[bool, str]:
        """
        Validate proposed trade against risk constraints.
        
        Args:
            proposed_order: Proposed trade order
            portfolio_state: Current portfolio state
            
        Returns:
            Tuple of (is_valid, reason)
        """
        try:
            # Check maximum position size
            max_position_value = portfolio_state.total_value * Decimal(str(self.config.get('max_position_percentage', 0.05)))
            order_value = proposed_order.quantity * (proposed_order.price or Decimal('0'))
            
            if order_value > max_position_value:
                return False, f"Order value {order_value} exceeds max position size {max_position_value}"
            
            # Check available cash for buy orders
            if proposed_order.side == 'BUY':
                if order_value > portfolio_state.cash_balance:
                    return False, f"Insufficient cash: need {order_value}, have {portfolio_state.cash_balance}"
            
            # Check position exists for sell orders
            if proposed_order.side == 'SELL':
                current_position = portfolio_state.positions.get(proposed_order.symbol)
                if not current_position or current_position.quantity < proposed_order.quantity:
                    return False, f"Insufficient position to sell: need {proposed_order.quantity}, have {current_position.quantity if current_position else 0}"
            
            # Check concentration limits
            if self._check_concentration_risk(proposed_order, portfolio_state):
                return False, "Trade would exceed concentration limits"
            
            return True, "Trade validated"
            
        except Exception as e:
            self.logger.error(f"Error validating trade: {e}")
            return False, f"Validation error: {e}"
    
    def _check_concentration_risk(self, proposed_order: ProposedTradeOrder,
                                portfolio_state: PortfolioState) -> bool:
        """
        Check if trade would create excessive concentration.
        
        Args:
            proposed_order: Proposed trade order
            portfolio_state: Current portfolio state
            
        Returns:
            True if concentration risk exists
        """
        try:
            max_concentration = self.config.get('max_concentration_percentage', 0.20)  # 20% max
            
            # Calculate new position value after trade
            current_position = portfolio_state.positions.get(proposed_order.symbol)
            current_quantity = current_position.quantity if current_position else Decimal('0')
            
            if proposed_order.side == 'BUY':
                new_quantity = current_quantity + proposed_order.quantity
            else:  # SELL
                new_quantity = current_quantity - proposed_order.quantity
            
            new_position_value = new_quantity * (proposed_order.price or Decimal('0'))
            concentration = new_position_value / portfolio_state.total_value
            
            return concentration > Decimal(str(max_concentration))
            
        except Exception as e:
            self.logger.error(f"Error checking concentration risk: {e}")
            return True  # Conservative approach


class PortfolioManager:
    """
    Portfolio manager with sophisticated state management and position sizing.
    
    Features:
    - Real-time portfolio state tracking
    - Multiple position sizing algorithms
    - Risk-based order generation
    - Trade execution monitoring
    """
    
    def __init__(self, service_name: str = "portfolio_manager"):
        """
        Initialize portfolio manager.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        self.state_manager = PersistentStateManager(service_name)

        # Portfolio state management with persistence
        self.portfolio_id = self.config.get('portfolio', {}).get('default_portfolio_id', 'default_portfolio')
        self.portfolio_state = self._load_or_create_portfolio_state()
        self.state_lock = threading.Lock()
        
        # Initialize position sizer and risk manager
        sizing_config = self.config.get('position_sizing', {})
        risk_config = self.config.get('risk_management', {})
        
        self.position_sizer = PositionSizer(sizing_config)
        self.risk_manager = RiskManager(risk_config)
        
        # Topic names
        self.enriched_signals_topic = get_topic_name('strategy', 'enriched_trade_signals')
        self.executed_trades_topic = get_topic_name('execution', 'executed_trade_events')
        self.proposed_orders_topic = get_topic_name('strategy', 'proposed_trade_orders')
        
        # Subscribe to events
        self.event_bus.subscribe(
            topic=self.enriched_signals_topic,
            callback=self._handle_enriched_signal,
            consumer_group='portfolio_management'
        )
        
        self.event_bus.subscribe(
            topic=self.executed_trades_topic,
            callback=self._handle_executed_trade,
            consumer_group='portfolio_management'
        )

        # Register request handlers for other services to query portfolio state
        self.event_bus.register_request_handler('get_portfolio_state', self._handle_portfolio_state_request)
        self.event_bus.register_request_handler('get_position_info', self._handle_position_info_request)
        self.event_bus.register_request_handler('get_portfolio_summary', self._handle_portfolio_summary_request)

        self.logger.info(f"Portfolio manager initialized with portfolio {self.portfolio_id}")
    
    def _load_or_create_portfolio_state(self) -> PortfolioState:
        """Load portfolio state from database or create new one."""
        try:
            # Try to load existing portfolio state
            portfolio_data = self.state_manager.load_portfolio_state(self.portfolio_id)

            if portfolio_data:
                # Convert database data to PortfolioState
                positions = {}
                for symbol, pos_data in portfolio_data.get('positions', {}).items():
                    positions[symbol] = Position(
                        symbol=symbol,
                        quantity=Decimal(str(pos_data['quantity'])),
                        avg_price=Decimal(str(pos_data['avg_price'])),
                        market_value=Decimal(str(pos_data['market_value'])),
                        unrealized_pnl=Decimal(str(pos_data.get('unrealized_pnl', 0))),
                        last_price=Decimal(str(pos_data['last_price'])) if pos_data.get('last_price') else None
                    )

                portfolio_state = PortfolioState(
                    cash_balance=Decimal(str(portfolio_data['cash_balance'])),
                    total_value=Decimal(str(portfolio_data['total_value'])),
                    positions=positions,
                    buying_power=Decimal(str(portfolio_data['buying_power'])),
                    margin_used=Decimal(str(portfolio_data['margin_used'])),
                    last_updated=datetime.utcnow()
                )

                self.logger.info(f"Loaded existing portfolio state: ${portfolio_data['total_value']:,.2f}")
                return portfolio_state

            else:
                # Create new portfolio with initial capital
                initial_capital = Decimal(str(self.config.get('portfolio', {}).get('initial_capital', 100000)))

                portfolio_state = PortfolioState(
                    cash_balance=initial_capital,
                    total_value=initial_capital,
                    positions={},
                    buying_power=initial_capital,
                    margin_used=Decimal('0.00'),
                    last_updated=datetime.utcnow()
                )

                # Save initial state to database
                self._save_portfolio_state()

                self.logger.info(f"Created new portfolio with ${initial_capital:,.2f} initial capital")
                return portfolio_state

        except Exception as e:
            self.logger.error(f"Error loading portfolio state: {e}")
            # Fallback to default state
            return PortfolioState(
                cash_balance=Decimal('100000.00'),
                total_value=Decimal('100000.00'),
                positions={},
                buying_power=Decimal('100000.00'),
                margin_used=Decimal('0.00'),
                last_updated=datetime.utcnow()
            )

    def _save_portfolio_state(self) -> None:
        """Save current portfolio state to database."""
        try:
            with self.state_lock:
                # Convert positions to serializable format
                positions_data = {}
                for symbol, position in self.portfolio_state.positions.items():
                    positions_data[symbol] = {
                        'quantity': float(position.quantity),
                        'avg_price': float(position.avg_price),
                        'market_value': float(position.market_value),
                        'unrealized_pnl': float(position.unrealized_pnl),
                        'last_price': float(position.last_price) if position.last_price else None
                    }

                portfolio_data = {
                    'id': self.portfolio_id,
                    'cash_balance': float(self.portfolio_state.cash_balance),
                    'total_value': float(self.portfolio_state.total_value),
                    'buying_power': float(self.portfolio_state.buying_power),
                    'margin_used': float(self.portfolio_state.margin_used),
                    'positions': positions_data
                }

                self.state_manager.save_portfolio_state(portfolio_data)

        except Exception as e:
            self.logger.error(f"Error saving portfolio state: {e}")

    def _handle_portfolio_state_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for portfolio state."""
        try:
            portfolio_id = request.data.get('portfolio_id', self.portfolio_id)

            if portfolio_id == self.portfolio_id:
                # Return current portfolio state
                with self.state_lock:
                    positions_data = {}
                    for symbol, position in self.portfolio_state.positions.items():
                        positions_data[symbol] = {
                            'quantity': float(position.quantity),
                            'avg_price': float(position.avg_price),
                            'market_value': float(position.market_value),
                            'unrealized_pnl': float(position.unrealized_pnl),
                            'last_price': float(position.last_price) if position.last_price else None
                        }

                    response_data = {
                        'id': self.portfolio_id,
                        'cash_balance': float(self.portfolio_state.cash_balance),
                        'total_value': float(self.portfolio_state.total_value),
                        'buying_power': float(self.portfolio_state.buying_power),
                        'margin_used': float(self.portfolio_state.margin_used),
                        'positions': positions_data,
                        'last_updated': self.portfolio_state.last_updated.isoformat()
                    }
            else:
                # Load from database
                response_data = self.state_manager.load_portfolio_state(portfolio_id)
                if not response_data:
                    raise ValueError(f"Portfolio {portfolio_id} not found")

            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_portfolio_state_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=response_data,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_portfolio_state_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_position_info_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for specific position information."""
        try:
            portfolio_id = request.data.get('portfolio_id', self.portfolio_id)
            symbol = request.data.get('symbol')

            if not symbol:
                raise ValueError("Symbol is required for position info request")

            if portfolio_id == self.portfolio_id:
                # Get from current state
                with self.state_lock:
                    position = self.portfolio_state.positions.get(symbol)
                    if position:
                        response_data = {
                            'symbol': symbol,
                            'quantity': float(position.quantity),
                            'avg_price': float(position.avg_price),
                            'market_value': float(position.market_value),
                            'unrealized_pnl': float(position.unrealized_pnl),
                            'last_price': float(position.last_price) if position.last_price else None
                        }
                    else:
                        response_data = {
                            'symbol': symbol,
                            'quantity': 0.0,
                            'avg_price': 0.0,
                            'market_value': 0.0,
                            'unrealized_pnl': 0.0,
                            'last_price': None
                        }
            else:
                # Load from database
                portfolio_data = self.state_manager.load_portfolio_state(portfolio_id)
                if portfolio_data:
                    position_data = portfolio_data.get('positions', {}).get(symbol, {})
                    response_data = {
                        'symbol': symbol,
                        'quantity': position_data.get('quantity', 0.0),
                        'avg_price': position_data.get('avg_price', 0.0),
                        'market_value': position_data.get('market_value', 0.0),
                        'unrealized_pnl': position_data.get('unrealized_pnl', 0.0),
                        'last_price': position_data.get('last_price')
                    }
                else:
                    raise ValueError(f"Portfolio {portfolio_id} not found")

            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_position_info_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=response_data,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_position_info_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_portfolio_summary_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for portfolio summary."""
        try:
            portfolio_id = request.data.get('portfolio_id', self.portfolio_id)

            if portfolio_id == self.portfolio_id:
                with self.state_lock:
                    response_data = {
                        'portfolio_id': self.portfolio_id,
                        'total_value': float(self.portfolio_state.total_value),
                        'cash_balance': float(self.portfolio_state.cash_balance),
                        'buying_power': float(self.portfolio_state.buying_power),
                        'margin_used': float(self.portfolio_state.margin_used),
                        'position_count': len(self.portfolio_state.positions),
                        'last_updated': self.portfolio_state.last_updated.isoformat()
                    }
            else:
                portfolio_data = self.state_manager.load_portfolio_state(portfolio_id)
                if portfolio_data:
                    response_data = {
                        'portfolio_id': portfolio_id,
                        'total_value': portfolio_data['total_value'],
                        'cash_balance': portfolio_data['cash_balance'],
                        'buying_power': portfolio_data['buying_power'],
                        'margin_used': portfolio_data['margin_used'],
                        'position_count': len(portfolio_data.get('positions', {})),
                        'last_updated': portfolio_data['updated_at']
                    }
                else:
                    raise ValueError(f"Portfolio {portfolio_id} not found")

            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_portfolio_summary_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=response_data,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_portfolio_summary_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_enriched_signal(self, message: EventMessage) -> None:
        """
        Handle incoming enriched signal.
        
        Args:
            message: Enriched signal message
        """
        try:
            signal_data = message.data
            symbol = signal_data.get('symbol')
            
            if symbol:
                trade_order = self.generate_trade_order(signal_data)
                if trade_order:
                    self._publish_proposed_order(trade_order)
            
        except Exception as e:
            self.logger.error(f"Error handling enriched signal: {e}")
    
    def _handle_executed_trade(self, message: EventMessage) -> None:
        """
        Handle executed trade to update portfolio state.
        
        Args:
            message: Executed trade message
        """
        try:
            trade_data = message.data
            self._update_portfolio_state(trade_data)
            
        except Exception as e:
            self.logger.error(f"Error handling executed trade: {e}")

    def generate_trade_order(self, signal_data: Dict[str, Any]) -> Optional[ProposedTradeOrder]:
        """
        Generate a concrete trade order from an enriched signal.

        Args:
            signal_data: Enriched signal data

        Returns:
            Proposed trade order or None
        """
        try:
            symbol = signal_data.get('symbol')
            signal = signal_data.get('signal')
            confidence = float(signal_data.get('confidence', 0.0))

            if not symbol or not signal:
                self.logger.warning("Signal missing required fields")
                return None

            # Skip HOLD signals
            if signal == 'HOLD':
                self.logger.debug(f"Skipping HOLD signal for {symbol}")
                return None

            # Get current market price (simplified - in production, get from market data)
            current_price = self._get_current_price(symbol)
            if not current_price:
                self.logger.error(f"Could not get current price for {symbol}")
                return None

            # Calculate position size
            with self.state_lock:
                position_size, risk_metrics = self.position_sizer.calculate_position_size(
                    signal_data, self.portfolio_state, current_price
                )

            if position_size == 0:
                self.logger.debug(f"Position size is zero for {symbol}")
                return None

            # Determine order side
            if signal in ['STRONG_BUY', 'BUY']:
                side = 'BUY'
                quantity = abs(position_size)
            elif signal in ['STRONG_SELL', 'SELL']:
                side = 'SELL'
                quantity = abs(position_size)
            else:
                self.logger.warning(f"Unknown signal: {signal}")
                return None

            # Create proposed order
            proposed_order = ProposedTradeOrder(
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type='MARKET',  # Default to market orders
                price=current_price,
                time_in_force='DAY',
                signal_source='llm_signal_aggregator',
                signal_confidence=confidence,
                reasoning=signal_data.get('reasoning', ''),
                risk_metrics=risk_metrics,
                correlation_id=signal_data.get('correlation_id', ''),
                timestamp=datetime.utcnow()
            )

            # Validate the order
            with self.state_lock:
                is_valid, reason = self.risk_manager.validate_trade(proposed_order, self.portfolio_state)

            if not is_valid:
                self.logger.warning(f"Trade validation failed for {symbol}: {reason}")
                return None

            self.logger.info(f"Generated trade order: {side} {quantity} {symbol} at {current_price}")
            return proposed_order

        except Exception as e:
            self.logger.error(f"Error generating trade order: {e}")
            return None

    def _get_current_price(self, symbol: str) -> Optional[Decimal]:
        """
        Get current market price for symbol from database or market data service.

        Args:
            symbol: Stock symbol

        Returns:
            Current price or None
        """
        try:
            # First try to get from persistent state manager (latest market data)
            latest_price = self.state_manager.get_latest_market_price(symbol)
            if latest_price:
                return latest_price

            # Fallback: try to request from market data service
            try:
                response = self.event_bus.request(
                    target_service='market_data_service',
                    request_type='get_latest_price',
                    data={'symbol': symbol},
                    timeout_seconds=2
                )

                if response and 'price' in response:
                    return Decimal(str(response['price']))

            except Exception as e:
                self.logger.warning(f"Could not get real-time price for {symbol}: {e}")

            # Final fallback: use simulated prices for development
            simulated_prices = {
                'AAPL': Decimal('150.00'),
                'GOOGL': Decimal('2500.00'),
                'MSFT': Decimal('300.00'),
                'TSLA': Decimal('200.00'),
                'AMZN': Decimal('3000.00'),
                'SPY': Decimal('400.00'),
                'QQQ': Decimal('350.00'),
                'IWM': Decimal('180.00')
            }

            price = simulated_prices.get(symbol, Decimal('100.00'))
            self.logger.warning(f"Using simulated price ${price} for {symbol}")
            return price

        except Exception as e:
            self.logger.error(f"Error getting current price for {symbol}: {e}")
            return None

    def _update_portfolio_state(self, trade_data: Dict[str, Any]) -> None:
        """
        Update portfolio state based on executed trade.

        Args:
            trade_data: Executed trade data
        """
        try:
            with self.state_lock:
                symbol = trade_data.get('symbol')
                side = trade_data.get('side')
                quantity = Decimal(str(trade_data.get('quantity', 0)))
                price = Decimal(str(trade_data.get('price', 0)))

                if not symbol or not side:
                    return

                # Update cash balance
                trade_value = quantity * price
                if side == 'BUY':
                    self.portfolio_state.cash_balance -= trade_value
                else:  # SELL
                    self.portfolio_state.cash_balance += trade_value

                # Update position
                current_position = self.portfolio_state.positions.get(symbol)

                if current_position:
                    if side == 'BUY':
                        # Calculate new average price
                        total_cost = (current_position.quantity * current_position.average_price) + trade_value
                        new_quantity = current_position.quantity + quantity
                        new_avg_price = total_cost / new_quantity if new_quantity > 0 else Decimal('0')

                        current_position.quantity = new_quantity
                        current_position.average_price = new_avg_price
                    else:  # SELL
                        current_position.quantity -= quantity

                        # Remove position if quantity is zero
                        if current_position.quantity <= 0:
                            del self.portfolio_state.positions[symbol]

                    if symbol in self.portfolio_state.positions:
                        current_position.market_value = current_position.quantity * price
                        current_position.unrealized_pnl = (price - current_position.average_price) * current_position.quantity
                        current_position.last_updated = datetime.utcnow()

                else:
                    # New position (only for BUY orders)
                    if side == 'BUY':
                        self.portfolio_state.positions[symbol] = Position(
                            symbol=symbol,
                            quantity=quantity,
                            average_price=price,
                            market_value=trade_value,
                            unrealized_pnl=Decimal('0'),
                            last_updated=datetime.utcnow()
                        )

                # Update total portfolio value
                self._recalculate_portfolio_value()

                # Save trade to database
                trade_record = {
                    'portfolio_id': self.portfolio_id,
                    'symbol': symbol,
                    'side': side,
                    'quantity': float(quantity),
                    'price': float(price),
                    'total_value': float(trade_value),
                    'execution_time': trade_data.get('execution_time', datetime.utcnow().isoformat()),
                    'order_id': trade_data.get('order_id'),
                    'broker': trade_data.get('broker'),
                    'commission': float(trade_data.get('commission', 0))
                }

                self.state_manager.save_trade(trade_record)

                # Save updated portfolio state
                self._save_portfolio_state()

                self.logger.info(f"Updated portfolio state after {side} {quantity} {symbol} at {price}")

        except Exception as e:
            self.logger.error(f"Error updating portfolio state: {e}")

    def _recalculate_portfolio_value(self) -> None:
        """Recalculate total portfolio value."""
        try:
            total_position_value = sum(pos.market_value for pos in self.portfolio_state.positions.values())
            self.portfolio_state.total_value = self.portfolio_state.cash_balance + total_position_value
            self.portfolio_state.buying_power = self.portfolio_state.cash_balance  # Simplified
            self.portfolio_state.last_updated = datetime.utcnow()

        except Exception as e:
            self.logger.error(f"Error recalculating portfolio value: {e}")

    def _publish_proposed_order(self, order: ProposedTradeOrder) -> None:
        """
        Publish proposed trade order to event bus.

        Args:
            order: Proposed trade order
        """
        try:
            message = asdict(order)
            # Convert Decimal and datetime to serializable formats
            message['quantity'] = float(order.quantity)
            message['price'] = float(order.price) if order.price else None
            message['timestamp'] = order.timestamp.isoformat()

            self.event_bus.publish(
                topic=self.proposed_orders_topic,
                message=message,
                event_type='proposed_order',
                correlation_id=order.correlation_id
            )

            # Save to document database
            self.database_manager.save_document('proposed_orders', message)

            self.logger.info(f"Published proposed order: {order.side} {order.quantity} {order.symbol}")

        except Exception as e:
            self.logger.error(f"Error publishing proposed order: {e}")

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """
        Get current portfolio summary.

        Returns:
            Portfolio summary
        """
        try:
            with self.state_lock:
                positions_summary = {}
                for symbol, position in self.portfolio_state.positions.items():
                    positions_summary[symbol] = {
                        'quantity': float(position.quantity),
                        'average_price': float(position.average_price),
                        'market_value': float(position.market_value),
                        'unrealized_pnl': float(position.unrealized_pnl),
                        'last_updated': position.last_updated.isoformat()
                    }

                return {
                    'cash_balance': float(self.portfolio_state.cash_balance),
                    'total_value': float(self.portfolio_state.total_value),
                    'buying_power': float(self.portfolio_state.buying_power),
                    'margin_used': float(self.portfolio_state.margin_used),
                    'positions': positions_summary,
                    'position_count': len(self.portfolio_state.positions),
                    'last_updated': self.portfolio_state.last_updated.isoformat()
                }

        except Exception as e:
            self.logger.error(f"Error getting portfolio summary: {e}")
            return {}

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get position information for a specific symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Position information or None
        """
        try:
            with self.state_lock:
                position = self.portfolio_state.positions.get(symbol)
                if position:
                    return {
                        'symbol': position.symbol,
                        'quantity': float(position.quantity),
                        'average_price': float(position.average_price),
                        'market_value': float(position.market_value),
                        'unrealized_pnl': float(position.unrealized_pnl),
                        'last_updated': position.last_updated.isoformat()
                    }
                return None

        except Exception as e:
            self.logger.error(f"Error getting position for {symbol}: {e}")
            return None

    def stop(self) -> None:
        """Stop the portfolio manager."""
        self.event_bus.close()
        self.logger.info("Portfolio manager stopped")


def main():
    """Main entry point for the portfolio manager service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    manager = PortfolioManager()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down portfolio manager...")
    finally:
        manager.stop()


if __name__ == "__main__":
    main()

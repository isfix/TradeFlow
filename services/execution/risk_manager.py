"""
Risk Manager

Acts as the final automated pre-trade safety check with comprehensive risk rules
and circuit breaker functionality to prevent catastrophic errors.
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from decimal import Decimal
from collections import defaultdict, deque
import threading

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage, RequestMessage, ResponseMessage
from ...core.database_clients import get_database_manager
from ...core.persistent_state_manager import PersistentStateManager
from ...configs import load_services_config, get_topic_name


@dataclass
class RiskCheck:
    """Individual risk check result."""
    check_name: str
    passed: bool
    message: str
    severity: str  # 'INFO', 'WARNING', 'ERROR', 'CRITICAL'


@dataclass
class RiskAssessment:
    """Complete risk assessment for an order."""
    order_id: str
    symbol: str
    checks: List[RiskCheck]
    overall_result: bool
    risk_score: float  # 0.0 to 1.0
    timestamp: datetime


class TradingVelocityMonitor:
    """Monitors trading velocity to detect runaway algorithms."""
    
    def __init__(self, max_trades_per_minute: int = 10, max_trades_per_hour: int = 100):
        """
        Initialize velocity monitor.
        
        Args:
            max_trades_per_minute: Maximum trades per minute per symbol
            max_trades_per_hour: Maximum trades per hour per symbol
        """
        self.max_trades_per_minute = max_trades_per_minute
        self.max_trades_per_hour = max_trades_per_hour
        
        # Track trades by symbol
        self.trades_per_minute: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_trades_per_minute))
        self.trades_per_hour: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_trades_per_hour))
        
        self.lock = threading.Lock()
        self.logger = logging.getLogger(f"{__name__}.TradingVelocityMonitor")
    
    def check_velocity(self, symbol: str) -> Tuple[bool, str]:
        """
        Check if trading velocity is within limits.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Tuple of (is_within_limits, message)
        """
        with self.lock:
            current_time = time.time()
            
            # Clean old entries
            minute_ago = current_time - 60
            hour_ago = current_time - 3600
            
            # Remove old minute entries
            minute_trades = self.trades_per_minute[symbol]
            while minute_trades and minute_trades[0] < minute_ago:
                minute_trades.popleft()
            
            # Remove old hour entries
            hour_trades = self.trades_per_hour[symbol]
            while hour_trades and hour_trades[0] < hour_ago:
                hour_trades.popleft()
            
            # Check limits
            if len(minute_trades) >= self.max_trades_per_minute:
                return False, f"Exceeded {self.max_trades_per_minute} trades per minute for {symbol}"
            
            if len(hour_trades) >= self.max_trades_per_hour:
                return False, f"Exceeded {self.max_trades_per_hour} trades per hour for {symbol}"
            
            return True, "Velocity check passed"
    
    def record_trade(self, symbol: str) -> None:
        """
        Record a trade for velocity tracking.
        
        Args:
            symbol: Stock symbol
        """
        with self.lock:
            current_time = time.time()
            self.trades_per_minute[symbol].append(current_time)
            self.trades_per_hour[symbol].append(current_time)


class PositionLimitMonitor:
    """Monitors position limits and concentration risk."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize position limit monitor.
        
        Args:
            config: Position limit configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.PositionLimitMonitor")
        
        # Current positions (simplified - in production, sync with portfolio manager)
        self.positions: Dict[str, Decimal] = {}
        self.lock = threading.Lock()
    
    def check_position_limits(self, symbol: str, side: str, quantity: Decimal, 
                            price: Decimal, portfolio_value: Decimal) -> Tuple[bool, str]:
        """
        Check if order would violate position limits.
        
        Args:
            symbol: Stock symbol
            side: Order side ('BUY' or 'SELL')
            quantity: Order quantity
            price: Order price
            portfolio_value: Total portfolio value
            
        Returns:
            Tuple of (is_within_limits, message)
        """
        try:
            with self.lock:
                current_position = self.positions.get(symbol, Decimal('0'))
                
                # Calculate new position after trade
                if side == 'BUY':
                    new_position = current_position + quantity
                else:  # SELL
                    new_position = current_position - quantity
                
                # Check maximum position size
                max_position_value = portfolio_value * Decimal(str(self.config.get('max_position_percentage', 0.10)))
                new_position_value = abs(new_position) * price
                
                if new_position_value > max_position_value:
                    return False, f"Position value {new_position_value} would exceed limit {max_position_value}"
                
                # Check maximum position quantity
                max_position_quantity = Decimal(str(self.config.get('max_position_quantity', 10000)))
                if abs(new_position) > max_position_quantity:
                    return False, f"Position quantity {abs(new_position)} would exceed limit {max_position_quantity}"
                
                # Check for short selling limits (if applicable)
                if new_position < 0 and not self.config.get('allow_short_selling', False):
                    return False, "Short selling not allowed"
                
                return True, "Position limits check passed"
                
        except Exception as e:
            self.logger.error(f"Error checking position limits: {e}")
            return False, f"Position limits check error: {e}"
    
    def update_position(self, symbol: str, side: str, quantity: Decimal) -> None:
        """
        Update position after trade execution.
        
        Args:
            symbol: Stock symbol
            side: Trade side
            quantity: Trade quantity
        """
        with self.lock:
            current_position = self.positions.get(symbol, Decimal('0'))
            
            if side == 'BUY':
                self.positions[symbol] = current_position + quantity
            else:  # SELL
                self.positions[symbol] = current_position - quantity
                
                # Remove position if zero
                if self.positions[symbol] == 0:
                    del self.positions[symbol]


class MarketConditionMonitor:
    """Monitors market conditions for risk assessment."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize market condition monitor.
        
        Args:
            config: Market condition configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.MarketConditionMonitor")
        
        # Market hours (simplified)
        self.market_open_hour = 9  # 9 AM
        self.market_close_hour = 16  # 4 PM
    
    def check_market_conditions(self, symbol: str) -> Tuple[bool, str]:
        """
        Check if market conditions are suitable for trading.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Tuple of (is_suitable, message)
        """
        try:
            current_time = datetime.now()
            
            # Check market hours (simplified)
            if not self._is_market_hours(current_time):
                return False, "Market is closed"
            
            # Check if symbol is on restricted list
            restricted_symbols = self.config.get('restricted_symbols', [])
            if symbol in restricted_symbols:
                return False, f"Symbol {symbol} is on restricted list"
            
            # Check for market volatility (simplified)
            if self._is_high_volatility_period():
                return False, "High volatility period detected"
            
            return True, "Market conditions check passed"
            
        except Exception as e:
            self.logger.error(f"Error checking market conditions: {e}")
            return False, f"Market conditions check error: {e}"
    
    def _is_market_hours(self, current_time: datetime) -> bool:
        """Check if current time is within market hours."""
        # Simplified check - in production, consider holidays, time zones, etc.
        weekday = current_time.weekday()
        hour = current_time.hour
        
        # Monday = 0, Sunday = 6
        if weekday >= 5:  # Weekend
            return False
        
        return self.market_open_hour <= hour < self.market_close_hour
    
    def _is_high_volatility_period(self) -> bool:
        """Check if current period has high volatility."""
        # Simplified implementation - in production, use VIX or other volatility measures
        return False


class RiskManager:
    """
    Comprehensive risk manager with multiple safety checks.
    
    Features:
    - Pre-trade risk validation
    - Trading velocity monitoring
    - Position limit enforcement
    - Market condition checks
    - Circuit breaker functionality
    """
    
    def __init__(self, service_name: str = "risk_manager"):
        """
        Initialize stateless risk manager.

        This risk manager does NOT maintain its own state. Instead, it queries
        the PortfolioManager for authoritative portfolio data when needed.

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

        # Initialize risk monitors (stateless)
        risk_config = self.config.get('risk_management', {})

        self.velocity_monitor = TradingVelocityMonitor(
            max_trades_per_minute=risk_config.get('max_trades_per_minute', 10),
            max_trades_per_hour=risk_config.get('max_trades_per_hour', 100)
        )

        # Market monitor (stateless)
        self.market_monitor = MarketConditionMonitor(risk_config)

        # Risk limits from config
        self.max_position_value = Decimal(str(risk_config.get('max_position_value', 50000)))
        self.max_portfolio_leverage = Decimal(str(risk_config.get('max_portfolio_leverage', 2.0)))
        self.max_daily_loss = Decimal(str(risk_config.get('max_daily_loss', 10000)))
        self.max_symbol_concentration = Decimal(str(risk_config.get('max_symbol_concentration', 0.1)))  # 10%

        # Circuit breaker state (persistent)
        circuit_breaker_state = self.state_manager.load_service_state('circuit_breaker')
        if circuit_breaker_state:
            self.circuit_breaker_active = circuit_breaker_state.get('active', False)
            self.circuit_breaker_reason = circuit_breaker_state.get('reason', '')
            self.circuit_breaker_timestamp = circuit_breaker_state.get('timestamp')
        else:
            self.circuit_breaker_active = False
            self.circuit_breaker_reason = ""
            self.circuit_breaker_timestamp = None

        # Topic names
        self.proposed_orders_topic = get_topic_name('strategy', 'proposed_trade_orders')
        self.validated_orders_topic = get_topic_name('execution', 'validated_trade_orders')
        self.rejected_orders_topic = get_topic_name('execution', 'rejected_trade_orders')

        # Subscribe to proposed orders
        self.event_bus.subscribe(
            topic=self.proposed_orders_topic,
            callback=self._handle_proposed_order,
            consumer_group='risk_management'
        )

        # Register request handlers for portfolio queries
        self.event_bus.register_request_handler('get_portfolio_state', self._handle_portfolio_request)
        self.event_bus.register_request_handler('get_position_info', self._handle_position_request)

        self.logger.info("Stateless risk manager initialized")

    def _get_portfolio_state(self, portfolio_id: str) -> Optional[Dict[str, Any]]:
        """
        Get authoritative portfolio state from PortfolioManager.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Portfolio state or None if not available
        """
        try:
            response = self.event_bus.request(
                target_service='portfolio_manager',
                request_type='get_portfolio_state',
                data={'portfolio_id': portfolio_id},
                timeout_seconds=5
            )
            return response

        except Exception as e:
            self.logger.error(f"Error getting portfolio state: {e}")
            return None

    def _get_position_info(self, portfolio_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get authoritative position info from PortfolioManager.

        Args:
            portfolio_id: Portfolio ID
            symbol: Stock symbol

        Returns:
            Position info or None if not available
        """
        try:
            response = self.event_bus.request(
                target_service='portfolio_manager',
                request_type='get_position_info',
                data={'portfolio_id': portfolio_id, 'symbol': symbol},
                timeout_seconds=5
            )
            return response

        except Exception as e:
            self.logger.error(f"Error getting position info: {e}")
            return None

    def _save_circuit_breaker_state(self):
        """Save circuit breaker state to persistent storage."""
        try:
            self.state_manager.save_service_state('circuit_breaker', {
                'active': self.circuit_breaker_active,
                'reason': self.circuit_breaker_reason,
                'timestamp': self.circuit_breaker_timestamp
            })
        except Exception as e:
            self.logger.error(f"Error saving circuit breaker state: {e}")

    def _handle_portfolio_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for portfolio data (proxy to PortfolioManager)."""
        try:
            # This is a proxy - forward to actual PortfolioManager
            portfolio_response = self.event_bus.request(
                target_service='portfolio_manager',
                request_type=request.request_type,
                data=request.data,
                timeout_seconds=10
            )

            return ResponseMessage(
                request_id=request.request_id,
                response_type=f"{request.request_type}_response",
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=portfolio_response,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type=f"{request.request_type}_response",
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_position_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for position data (proxy to PortfolioManager)."""
        try:
            # This is a proxy - forward to actual PortfolioManager
            position_response = self.event_bus.request(
                target_service='portfolio_manager',
                request_type=request.request_type,
                data=request.data,
                timeout_seconds=10
            )

            return ResponseMessage(
                request_id=request.request_id,
                response_type=f"{request.request_type}_response",
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=position_response,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type=f"{request.request_type}_response",
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_proposed_order(self, message: EventMessage) -> None:
        """
        Handle incoming proposed order for risk validation.
        
        Args:
            message: Proposed order message
        """
        try:
            order_data = message.data
            
            # Perform risk assessment
            assessment = self.validate_order(order_data)
            
            # Publish result
            if assessment.overall_result:
                self._publish_validated_order(order_data, assessment)
            else:
                self._publish_rejected_order(order_data, assessment)
            
        except Exception as e:
            self.logger.error(f"Error handling proposed order: {e}")
            # Reject order on error
            self._publish_rejected_order(
                message.data, 
                RiskAssessment(
                    order_id=message.data.get('correlation_id', 'unknown'),
                    symbol=message.data.get('symbol', 'unknown'),
                    checks=[RiskCheck('error_check', False, f"Processing error: {e}", 'CRITICAL')],
                    overall_result=False,
                    risk_score=1.0,
                    timestamp=datetime.utcnow()
                )
            )

    def validate_order(self, order_data: Dict[str, Any]) -> RiskAssessment:
        """
        Perform comprehensive risk validation on an order.

        Args:
            order_data: Order data to validate

        Returns:
            Risk assessment result
        """
        try:
            symbol = order_data.get('symbol', '')
            side = order_data.get('side', '')
            quantity = Decimal(str(order_data.get('quantity', 0)))
            price = Decimal(str(order_data.get('price', 0)))
            order_id = order_data.get('correlation_id', 'unknown')

            checks = []

            # 1. Circuit breaker check
            if self.circuit_breaker_active:
                checks.append(RiskCheck(
                    'circuit_breaker',
                    False,
                    f"Circuit breaker active: {self.circuit_breaker_reason}",
                    'CRITICAL'
                ))
            else:
                checks.append(RiskCheck('circuit_breaker', True, 'Circuit breaker inactive', 'INFO'))

            # 2. Sanity checks
            sanity_passed, sanity_msg = self._perform_sanity_checks(symbol, side, quantity, price)
            checks.append(RiskCheck('sanity_check', sanity_passed, sanity_msg, 'ERROR' if not sanity_passed else 'INFO'))

            # 3. Trading velocity check
            velocity_passed, velocity_msg = self.velocity_monitor.check_velocity(symbol)
            checks.append(RiskCheck('velocity_check', velocity_passed, velocity_msg, 'WARNING' if not velocity_passed else 'INFO'))

            # 4. Position limits check using authoritative portfolio data
            portfolio_id = order_data.get('portfolio_id', 'default')
            portfolio_state = self._get_portfolio_state(portfolio_id)

            if portfolio_state:
                portfolio_value = Decimal(str(portfolio_state.get('total_value', 0)))
                current_position = portfolio_state.get('positions', {}).get(symbol, {})
                current_quantity = Decimal(str(current_position.get('quantity', 0)))

                # Check position limits with real data
                position_passed, position_msg = self._check_position_limits_with_real_data(
                    symbol, side, quantity, price, portfolio_value, current_quantity, portfolio_state
                )
            else:
                position_passed = False
                position_msg = "Unable to retrieve portfolio state for position validation"

            checks.append(RiskCheck('position_limits', position_passed, position_msg, 'ERROR' if not position_passed else 'INFO'))

            # 5. Market conditions check
            market_passed, market_msg = self.market_monitor.check_market_conditions(symbol)
            checks.append(RiskCheck('market_conditions', market_passed, market_msg, 'WARNING' if not market_passed else 'INFO'))

            # 6. Fat finger check
            fat_finger_passed, fat_finger_msg = self._perform_fat_finger_check(symbol, quantity, price)
            checks.append(RiskCheck('fat_finger', fat_finger_passed, fat_finger_msg, 'ERROR' if not fat_finger_passed else 'INFO'))

            # Calculate overall result and risk score
            critical_failures = [c for c in checks if not c.passed and c.severity == 'CRITICAL']
            error_failures = [c for c in checks if not c.passed and c.severity == 'ERROR']

            overall_result = len(critical_failures) == 0 and len(error_failures) == 0

            # Calculate risk score (0.0 = no risk, 1.0 = maximum risk)
            risk_score = self._calculate_risk_score(checks)

            assessment = RiskAssessment(
                order_id=order_id,
                symbol=symbol,
                checks=checks,
                overall_result=overall_result,
                risk_score=risk_score,
                timestamp=datetime.utcnow()
            )

            # Record trade if passed (for velocity monitoring)
            if overall_result:
                self.velocity_monitor.record_trade(symbol)

            return assessment

        except Exception as e:
            self.logger.error(f"Error in risk validation: {e}")
            return RiskAssessment(
                order_id=order_data.get('correlation_id', 'unknown'),
                symbol=order_data.get('symbol', 'unknown'),
                checks=[RiskCheck('validation_error', False, f"Validation error: {e}", 'CRITICAL')],
                overall_result=False,
                risk_score=1.0,
                timestamp=datetime.utcnow()
            )

    def _check_position_limits_with_real_data(self, symbol: str, side: str, quantity: Decimal,
                                            price: Decimal, portfolio_value: Decimal,
                                            current_quantity: Decimal, portfolio_state: Dict[str, Any]) -> tuple[bool, str]:
        """
        Check position limits using real portfolio data.

        Args:
            symbol: Stock symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price
            portfolio_value: Current portfolio value
            current_quantity: Current position quantity
            portfolio_state: Full portfolio state

        Returns:
            Tuple of (passed, message)
        """
        try:
            order_value = quantity * price

            # Calculate new position after this order
            if side.upper() == 'BUY':
                new_quantity = current_quantity + quantity
            else:  # SELL
                new_quantity = current_quantity - quantity

            new_position_value = new_quantity * price

            # Check 1: Maximum position value
            if new_position_value > self.max_position_value:
                return False, f"Position value ${new_position_value:,.2f} exceeds limit ${self.max_position_value:,.2f}"

            # Check 2: Portfolio concentration (single symbol shouldn't exceed X% of portfolio)
            if portfolio_value > 0:
                concentration = new_position_value / portfolio_value
                if concentration > self.max_symbol_concentration:
                    return False, f"Symbol concentration {concentration:.1%} exceeds limit {self.max_symbol_concentration:.1%}"

            # Check 3: Daily loss limit
            daily_pnl = Decimal(str(portfolio_state.get('daily_pnl', 0)))
            if daily_pnl < -self.max_daily_loss:
                return False, f"Daily loss ${abs(daily_pnl):,.2f} exceeds limit ${self.max_daily_loss:,.2f}"

            # Check 4: Portfolio leverage
            cash_balance = Decimal(str(portfolio_state.get('cash_balance', 0)))
            margin_used = Decimal(str(portfolio_state.get('margin_used', 0)))

            # Calculate leverage after this order
            if side.upper() == 'BUY':
                new_margin_used = margin_used + order_value
            else:
                new_margin_used = max(Decimal('0'), margin_used - order_value)

            if cash_balance > 0:
                leverage = new_margin_used / cash_balance
                if leverage > self.max_portfolio_leverage:
                    return False, f"Portfolio leverage {leverage:.2f}x exceeds limit {self.max_portfolio_leverage:.2f}x"

            # Check 5: Sufficient buying power
            buying_power = Decimal(str(portfolio_state.get('buying_power', 0)))
            if side.upper() == 'BUY' and order_value > buying_power:
                return False, f"Insufficient buying power: ${buying_power:,.2f} < ${order_value:,.2f}"

            return True, "Position limits check passed"

        except Exception as e:
            self.logger.error(f"Error checking position limits: {e}")
            return False, f"Position limits check error: {str(e)}"

    def _perform_sanity_checks(self, symbol: str, side: str, quantity: Decimal, price: Decimal) -> Tuple[bool, str]:
        """Perform basic sanity checks on order parameters."""
        try:
            # Check symbol
            if not symbol or len(symbol) < 1:
                return False, "Invalid symbol"

            # Check side
            if side not in ['BUY', 'SELL']:
                return False, f"Invalid side: {side}"

            # Check quantity
            if quantity <= 0:
                return False, f"Invalid quantity: {quantity}"

            # Check price
            if price <= 0:
                return False, f"Invalid price: {price}"

            # Check for reasonable values
            if quantity > Decimal('1000000'):  # 1M shares
                return False, f"Quantity too large: {quantity}"

            if price > Decimal('100000'):  # $100k per share
                return False, f"Price too high: {price}"

            return True, "Sanity checks passed"

        except Exception as e:
            return False, f"Sanity check error: {e}"

    def _perform_fat_finger_check(self, symbol: str, quantity: Decimal, price: Decimal) -> Tuple[bool, str]:
        """Check for unusually large orders that might be fat finger errors."""
        try:
            # Get recent trade history for comparison (simplified)
            # In production, this would query actual trade history

            # Check order value
            order_value = quantity * price
            max_order_value = Decimal('1000000')  # $1M max order

            if order_value > max_order_value:
                return False, f"Order value {order_value} exceeds fat finger limit {max_order_value}"

            # Check quantity vs typical volumes (simplified)
            typical_volume = Decimal('10000')  # Simplified typical volume
            if quantity > typical_volume * Decimal('10'):  # 10x typical volume
                return False, f"Quantity {quantity} is unusually large (>10x typical volume)"

            return True, "Fat finger check passed"

        except Exception as e:
            return False, f"Fat finger check error: {e}"

    def _calculate_risk_score(self, checks: List[RiskCheck]) -> float:
        """Calculate overall risk score based on check results."""
        try:
            total_weight = 0
            risk_weight = 0

            for check in checks:
                if check.severity == 'CRITICAL':
                    weight = 10
                elif check.severity == 'ERROR':
                    weight = 5
                elif check.severity == 'WARNING':
                    weight = 2
                else:  # INFO
                    weight = 1

                total_weight += weight
                if not check.passed:
                    risk_weight += weight

            return risk_weight / total_weight if total_weight > 0 else 0.0

        except Exception:
            return 1.0  # Maximum risk on error

    def _publish_validated_order(self, order_data: Dict[str, Any], assessment: RiskAssessment) -> None:
        """Publish validated order to execution queue."""
        try:
            # Add risk assessment to order data
            validated_order = order_data.copy()
            validated_order['risk_assessment'] = {
                'risk_score': assessment.risk_score,
                'checks_passed': len([c for c in assessment.checks if c.passed]),
                'total_checks': len(assessment.checks),
                'validation_timestamp': assessment.timestamp.isoformat()
            }

            self.event_bus.publish(
                topic=self.validated_orders_topic,
                message=validated_order,
                event_type='validated_order',
                correlation_id=order_data.get('correlation_id')
            )

            # Save to database
            self.database_manager.save_document('validated_orders', validated_order)

            self.logger.info(f"Validated order for {order_data.get('symbol')} (risk score: {assessment.risk_score:.3f})")

        except Exception as e:
            self.logger.error(f"Error publishing validated order: {e}")

    def _publish_rejected_order(self, order_data: Dict[str, Any], assessment: RiskAssessment) -> None:
        """Publish rejected order with reasons."""
        try:
            rejected_order = order_data.copy()
            rejected_order['rejection_reasons'] = [
                {'check': c.check_name, 'message': c.message, 'severity': c.severity}
                for c in assessment.checks if not c.passed
            ]
            rejected_order['risk_score'] = assessment.risk_score
            rejected_order['rejection_timestamp'] = assessment.timestamp.isoformat()

            self.event_bus.publish(
                topic=self.rejected_orders_topic,
                message=rejected_order,
                event_type='rejected_order',
                correlation_id=order_data.get('correlation_id')
            )

            # Save to database
            self.database_manager.save_document('rejected_orders', rejected_order)

            # Log rejection
            reasons = [c.message for c in assessment.checks if not c.passed]
            self.logger.warning(f"Rejected order for {order_data.get('symbol')}: {'; '.join(reasons)}")

            # Check if circuit breaker should be activated
            self._check_circuit_breaker_activation(assessment)

        except Exception as e:
            self.logger.error(f"Error publishing rejected order: {e}")

    def _check_circuit_breaker_activation(self, assessment: RiskAssessment) -> None:
        """Check if circuit breaker should be activated based on rejection patterns."""
        try:
            # Simplified circuit breaker logic
            # In production, this would be more sophisticated

            critical_failures = [c for c in assessment.checks if not c.passed and c.severity == 'CRITICAL']

            if critical_failures and not self.circuit_breaker_active:
                self.activate_circuit_breaker(f"Critical risk check failure: {critical_failures[0].message}")

        except Exception as e:
            self.logger.error(f"Error checking circuit breaker activation: {e}")

    def activate_circuit_breaker(self, reason: str) -> None:
        """Activate circuit breaker to halt all trading."""
        self.circuit_breaker_active = True
        self.circuit_breaker_reason = reason
        self.circuit_breaker_timestamp = datetime.utcnow()

        # Save state persistently
        self._save_circuit_breaker_state()

        self.logger.critical(f"CIRCUIT BREAKER ACTIVATED: {reason}")

        # Publish circuit breaker event
        self.event_bus.publish(
            topic=get_topic_name('system', 'error_events'),
            message={
                'event_type': 'circuit_breaker_activated',
                'reason': reason,
                'timestamp': self.circuit_breaker_timestamp.isoformat()
            },
            event_type='circuit_breaker'
        )

    def deactivate_circuit_breaker(self) -> None:
        """Deactivate circuit breaker to resume trading."""
        if self.circuit_breaker_active:
            self.circuit_breaker_active = False
            self.circuit_breaker_reason = ""
            self.circuit_breaker_timestamp = None

            # Save state persistently
            self._save_circuit_breaker_state()

            self.logger.warning("Circuit breaker deactivated - trading resumed")

            # Publish circuit breaker deactivation event
            self.event_bus.publish(
                topic=get_topic_name('system', 'error_events'),
                message={
                    'event_type': 'circuit_breaker_deactivated',
                    'timestamp': datetime.utcnow().isoformat()
                },
                event_type='circuit_breaker'
            )

    def get_risk_status(self) -> Dict[str, Any]:
        """Get current risk management status."""
        return {
            'circuit_breaker_active': self.circuit_breaker_active,
            'circuit_breaker_reason': self.circuit_breaker_reason,
            'circuit_breaker_timestamp': self.circuit_breaker_timestamp.isoformat() if self.circuit_breaker_timestamp else None,
            'active_positions': len(self.position_monitor.positions),
            'status_timestamp': datetime.utcnow().isoformat()
        }

    def stop(self) -> None:
        """Stop the risk manager."""
        self.event_bus.close()
        self.logger.info("Risk manager stopped")


def main():
    """Main entry point for the risk manager service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    risk_manager = RiskManager()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down risk manager...")
    finally:
        risk_manager.stop()


if __name__ == "__main__":
    main()

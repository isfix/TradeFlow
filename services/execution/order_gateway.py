"""
Order Gateway

Manages direct communication with broker APIs with idempotency, execution reporting,
and comprehensive error handling for trade execution.
"""

import logging
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from decimal import Decimal
import json
import requests
from enum import Enum

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage
from ...core.database_clients import get_database_manager, TradeData
from ...configs import load_services_config, get_topic_name


class OrderStatus(Enum):
    """Order status enumeration."""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


@dataclass
class BrokerOrder:
    """Broker order representation."""
    internal_id: str
    broker_id: Optional[str]
    symbol: str
    side: str
    quantity: Decimal
    order_type: str
    price: Optional[Decimal]
    time_in_force: str
    status: OrderStatus
    filled_quantity: Decimal
    average_fill_price: Optional[Decimal]
    commission: Decimal
    submitted_at: Optional[datetime]
    last_updated: datetime
    broker_response: Optional[Dict[str, Any]] = None


@dataclass
class ExecutionReport:
    """Execution report from broker."""
    execution_id: str
    order_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    commission: Decimal
    timestamp: datetime
    broker_data: Dict[str, Any]


class BrokerInterface:
    """Abstract interface for broker communication."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize broker interface.
        
        Args:
            config: Broker configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.BrokerInterface")
    
    def submit_order(self, order: BrokerOrder) -> Tuple[bool, str, Optional[str]]:
        """
        Submit order to broker.
        
        Args:
            order: Order to submit
            
        Returns:
            Tuple of (success, message, broker_order_id)
        """
        raise NotImplementedError
    
    def get_order_status(self, broker_order_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Get order status from broker.
        
        Args:
            broker_order_id: Broker order ID
            
        Returns:
            Tuple of (success, order_data)
        """
        raise NotImplementedError
    
    def cancel_order(self, broker_order_id: str) -> Tuple[bool, str]:
        """
        Cancel order with broker.
        
        Args:
            broker_order_id: Broker order ID
            
        Returns:
            Tuple of (success, message)
        """
        raise NotImplementedError


class AlpacaBrokerInterface(BrokerInterface):
    """Alpaca broker interface implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Alpaca broker interface."""
        super().__init__(config)
        self.base_url = config['base_url']
        self.api_key = config['api_key']
        self.secret_key = config['secret_key']
        self.timeout = config.get('timeout', 30)
        
        self.headers = {
            'APCA-API-KEY-ID': self.api_key,
            'APCA-API-SECRET-KEY': self.secret_key,
            'Content-Type': 'application/json'
        }
    
    def submit_order(self, order: BrokerOrder) -> Tuple[bool, str, Optional[str]]:
        """Submit order to Alpaca."""
        try:
            # Prepare order data for Alpaca API
            order_data = {
                'symbol': order.symbol,
                'qty': str(order.quantity),
                'side': order.side.lower(),
                'type': order.order_type.lower(),
                'time_in_force': order.time_in_force.lower(),
                'client_order_id': order.internal_id
            }
            
            # Add price for limit orders
            if order.order_type == 'LIMIT' and order.price:
                order_data['limit_price'] = str(order.price)
            
            # Submit order
            response = requests.post(
                f"{self.base_url}/v2/orders",
                headers=self.headers,
                json=order_data,
                timeout=self.timeout
            )
            
            if response.status_code == 201:
                result = response.json()
                broker_order_id = result.get('id')
                return True, "Order submitted successfully", broker_order_id
            else:
                error_msg = f"Alpaca API error: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return False, error_msg, None
                
        except Exception as e:
            error_msg = f"Error submitting order to Alpaca: {e}"
            self.logger.error(error_msg)
            return False, error_msg, None
    
    def get_order_status(self, broker_order_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Get order status from Alpaca."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/orders/{broker_order_id}",
                headers=self.headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return True, response.json()
            else:
                self.logger.error(f"Error getting order status: {response.status_code} - {response.text}")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error getting order status from Alpaca: {e}")
            return False, None
    
    def cancel_order(self, broker_order_id: str) -> Tuple[bool, str]:
        """Cancel order with Alpaca."""
        try:
            response = requests.delete(
                f"{self.base_url}/v2/orders/{broker_order_id}",
                headers=self.headers,
                timeout=self.timeout
            )
            
            if response.status_code == 204:
                return True, "Order cancelled successfully"
            else:
                error_msg = f"Error cancelling order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error cancelling order with Alpaca: {e}"
            self.logger.error(error_msg)
            return False, error_msg


class SimulatedBrokerInterface(BrokerInterface):
    """Simulated broker interface for testing."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize simulated broker interface."""
        super().__init__(config)
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.execution_delay = config.get('execution_delay', 1)  # seconds
    
    def submit_order(self, order: BrokerOrder) -> Tuple[bool, str, Optional[str]]:
        """Submit order to simulated broker."""
        try:
            broker_order_id = f"SIM_{uuid.uuid4().hex[:8]}"
            
            # Simulate order acceptance
            self.orders[broker_order_id] = {
                'id': broker_order_id,
                'symbol': order.symbol,
                'side': order.side,
                'qty': str(order.quantity),
                'type': order.order_type,
                'status': 'accepted',
                'filled_qty': '0',
                'avg_fill_price': None,
                'submitted_at': datetime.utcnow().isoformat(),
                'client_order_id': order.internal_id
            }
            
            # Simulate execution after delay
            self._schedule_execution(broker_order_id, order)
            
            return True, "Order submitted to simulator", broker_order_id
            
        except Exception as e:
            error_msg = f"Error in simulated broker: {e}"
            self.logger.error(error_msg)
            return False, error_msg, None
    
    def get_order_status(self, broker_order_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Get order status from simulated broker."""
        if broker_order_id in self.orders:
            return True, self.orders[broker_order_id]
        else:
            return False, None
    
    def cancel_order(self, broker_order_id: str) -> Tuple[bool, str]:
        """Cancel order with simulated broker."""
        if broker_order_id in self.orders:
            self.orders[broker_order_id]['status'] = 'cancelled'
            return True, "Order cancelled in simulator"
        else:
            return False, "Order not found in simulator"
    
    def _schedule_execution(self, broker_order_id: str, order: BrokerOrder) -> None:
        """Schedule simulated execution."""
        import threading
        
        def execute_order():
            time.sleep(self.execution_delay)
            
            if broker_order_id in self.orders and self.orders[broker_order_id]['status'] == 'accepted':
                # Simulate execution
                simulated_price = float(order.price) if order.price else 100.0  # Default price
                
                self.orders[broker_order_id].update({
                    'status': 'filled',
                    'filled_qty': str(order.quantity),
                    'avg_fill_price': str(simulated_price),
                    'filled_at': datetime.utcnow().isoformat()
                })
                
                self.logger.info(f"Simulated execution: {broker_order_id}")
        
        thread = threading.Thread(target=execute_order, daemon=True)
        thread.start()


class OrderTracker:
    """Tracks orders and their execution status."""
    
    def __init__(self):
        """Initialize order tracker."""
        self.orders: Dict[str, BrokerOrder] = {}
        self.logger = logging.getLogger(f"{__name__}.OrderTracker")
    
    def add_order(self, order: BrokerOrder) -> None:
        """Add order to tracker."""
        self.orders[order.internal_id] = order
        self.logger.debug(f"Added order to tracker: {order.internal_id}")
    
    def update_order(self, internal_id: str, **kwargs) -> None:
        """Update order in tracker."""
        if internal_id in self.orders:
            order = self.orders[internal_id]
            for key, value in kwargs.items():
                if hasattr(order, key):
                    setattr(order, key, value)
            order.last_updated = datetime.utcnow()
    
    def get_order(self, internal_id: str) -> Optional[BrokerOrder]:
        """Get order from tracker."""
        return self.orders.get(internal_id)
    
    def get_pending_orders(self) -> List[BrokerOrder]:
        """Get all pending orders."""
        return [order for order in self.orders.values() 
                if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]]


class OrderGateway:
    """
    Order gateway with broker integration and execution reporting.
    
    Features:
    - Idempotent order submission
    - Multiple broker support
    - Execution monitoring and reporting
    - Order status tracking
    """
    
    def __init__(self, service_name: str = "order_gateway"):
        """
        Initialize order gateway.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Initialize broker interface
        self.broker = self._initialize_broker()
        self.order_tracker = OrderTracker()
        
        # Topic names
        self.validated_orders_topic = get_topic_name('execution', 'validated_trade_orders')
        self.executed_trades_topic = get_topic_name('execution', 'executed_trade_events')
        
        # Subscribe to validated orders
        self.event_bus.subscribe(
            topic=self.validated_orders_topic,
            callback=self._handle_validated_order,
            consumer_group='order_execution'
        )
        
        # Start order monitoring
        self._start_order_monitoring()
        
        self.logger.info("Order gateway initialized")
    
    def _initialize_broker(self) -> BrokerInterface:
        """Initialize broker interface based on configuration."""
        brokers_config = self.config.get('brokers', {})
        
        # Default to Alpaca if available, otherwise use simulator
        if 'alpaca' in brokers_config:
            self.logger.info("Initializing Alpaca broker interface")
            return AlpacaBrokerInterface(brokers_config['alpaca'])
        else:
            self.logger.info("Initializing simulated broker interface")
            return SimulatedBrokerInterface({'execution_delay': 2})
    
    def _handle_validated_order(self, message: EventMessage) -> None:
        """
        Handle incoming validated order for execution.
        
        Args:
            message: Validated order message
        """
        try:
            order_data = message.data
            
            # Create broker order
            broker_order = self._create_broker_order(order_data)
            
            # Submit to broker
            self.send_to_broker(broker_order)
            
        except Exception as e:
            self.logger.error(f"Error handling validated order: {e}")
    
    def _create_broker_order(self, order_data: Dict[str, Any]) -> BrokerOrder:
        """Create broker order from order data."""
        return BrokerOrder(
            internal_id=order_data.get('correlation_id', str(uuid.uuid4())),
            broker_id=None,
            symbol=order_data.get('symbol', ''),
            side=order_data.get('side', ''),
            quantity=Decimal(str(order_data.get('quantity', 0))),
            order_type=order_data.get('order_type', 'MARKET'),
            price=Decimal(str(order_data.get('price'))) if order_data.get('price') else None,
            time_in_force=order_data.get('time_in_force', 'DAY'),
            status=OrderStatus.PENDING,
            filled_quantity=Decimal('0'),
            average_fill_price=None,
            commission=Decimal('0'),
            submitted_at=None,
            last_updated=datetime.utcnow()
        )

    def send_to_broker(self, order: BrokerOrder) -> bool:
        """
        Send order to broker with idempotency checks.

        Args:
            order: Broker order to send

        Returns:
            True if order was sent successfully
        """
        try:
            # Check for duplicate submission
            existing_order = self.order_tracker.get_order(order.internal_id)
            if existing_order and existing_order.status != OrderStatus.PENDING:
                self.logger.warning(f"Order {order.internal_id} already submitted")
                return False

            # Add to tracker
            self.order_tracker.add_order(order)

            # Submit to broker
            success, message, broker_order_id = self.broker.submit_order(order)

            if success and broker_order_id:
                # Update order with broker ID
                self.order_tracker.update_order(
                    order.internal_id,
                    broker_id=broker_order_id,
                    status=OrderStatus.SUBMITTED,
                    submitted_at=datetime.utcnow()
                )

                self.logger.info(f"Order submitted to broker: {order.internal_id} -> {broker_order_id}")
                return True
            else:
                # Update order status to rejected
                self.order_tracker.update_order(
                    order.internal_id,
                    status=OrderStatus.REJECTED
                )

                self.logger.error(f"Order rejected by broker: {message}")
                return False

        except Exception as e:
            self.logger.error(f"Error sending order to broker: {e}")

            # Update order status to rejected
            self.order_tracker.update_order(
                order.internal_id,
                status=OrderStatus.REJECTED
            )
            return False

    def _start_order_monitoring(self) -> None:
        """Start background thread for monitoring order status."""
        import threading

        def monitor_orders():
            while True:
                try:
                    self._check_order_status()
                    time.sleep(5)  # Check every 5 seconds

                except Exception as e:
                    self.logger.error(f"Error in order monitoring: {e}")
                    time.sleep(10)  # Wait longer on error

        monitor_thread = threading.Thread(target=monitor_orders, daemon=True)
        monitor_thread.start()
        self.logger.info("Started order monitoring thread")

    def _check_order_status(self) -> None:
        """Check status of pending orders."""
        try:
            pending_orders = self.order_tracker.get_pending_orders()

            for order in pending_orders:
                if order.broker_id:
                    success, broker_data = self.broker.get_order_status(order.broker_id)

                    if success and broker_data:
                        self._process_broker_status_update(order, broker_data)

        except Exception as e:
            self.logger.error(f"Error checking order status: {e}")

    def _process_broker_status_update(self, order: BrokerOrder, broker_data: Dict[str, Any]) -> None:
        """Process status update from broker."""
        try:
            broker_status = broker_data.get('status', '').lower()
            filled_qty = Decimal(str(broker_data.get('filled_qty', 0)))
            avg_fill_price = broker_data.get('avg_fill_price')

            # Map broker status to internal status
            if broker_status == 'filled':
                new_status = OrderStatus.FILLED
            elif broker_status == 'partially_filled':
                new_status = OrderStatus.PARTIALLY_FILLED
            elif broker_status == 'cancelled':
                new_status = OrderStatus.CANCELLED
            elif broker_status == 'rejected':
                new_status = OrderStatus.REJECTED
            elif broker_status == 'expired':
                new_status = OrderStatus.EXPIRED
            else:
                new_status = OrderStatus.SUBMITTED

            # Check if status changed
            if order.status != new_status or order.filled_quantity != filled_qty:
                # Update order
                updates = {
                    'status': new_status,
                    'filled_quantity': filled_qty,
                    'broker_response': broker_data
                }

                if avg_fill_price:
                    updates['average_fill_price'] = Decimal(str(avg_fill_price))

                self.order_tracker.update_order(order.internal_id, **updates)

                # Generate execution report if filled
                if new_status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
                    self._generate_execution_report(order, broker_data)

                self.logger.info(f"Order status updated: {order.internal_id} -> {new_status}")

        except Exception as e:
            self.logger.error(f"Error processing broker status update: {e}")

    def _generate_execution_report(self, order: BrokerOrder, broker_data: Dict[str, Any]) -> None:
        """Generate execution report for filled orders."""
        try:
            # Calculate execution details
            filled_qty = order.filled_quantity
            avg_price = order.average_fill_price or Decimal('0')
            commission = Decimal(str(broker_data.get('commission', 0)))

            # Create execution report
            execution_report = ExecutionReport(
                execution_id=f"EXEC_{uuid.uuid4().hex[:8]}",
                order_id=order.internal_id,
                symbol=order.symbol,
                side=order.side,
                quantity=filled_qty,
                price=avg_price,
                commission=commission,
                timestamp=datetime.utcnow(),
                broker_data=broker_data
            )

            # Publish execution event
            self._publish_execution_event(execution_report)

            # Create trade data for database
            trade_data = TradeData(
                trade_id=execution_report.execution_id,
                symbol=order.symbol,
                side=order.side,
                quantity=float(filled_qty),
                price=float(avg_price),
                timestamp=execution_report.timestamp,
                order_id=order.internal_id,
                execution_id=execution_report.execution_id,
                commission=float(commission),
                source_service=self.service_name,
                correlation_id=order.internal_id,
                metadata={'broker_data': broker_data}
            )

            # Log trade to database
            self.database_manager.log_trade(trade_data)

            self.logger.info(f"Generated execution report: {execution_report.execution_id}")

        except Exception as e:
            self.logger.error(f"Error generating execution report: {e}")

    def _publish_execution_event(self, execution_report: ExecutionReport) -> None:
        """Publish execution event to event bus."""
        try:
            message = {
                'execution_id': execution_report.execution_id,
                'order_id': execution_report.order_id,
                'symbol': execution_report.symbol,
                'side': execution_report.side,
                'quantity': float(execution_report.quantity),
                'price': float(execution_report.price),
                'commission': float(execution_report.commission),
                'timestamp': execution_report.timestamp.isoformat(),
                'broker_data': execution_report.broker_data
            }

            self.event_bus.publish(
                topic=self.executed_trades_topic,
                message=message,
                event_type='trade_execution',
                correlation_id=execution_report.order_id
            )

            # Save to document database
            self.database_manager.save_document('executions', message)

            self.logger.debug(f"Published execution event: {execution_report.execution_id}")

        except Exception as e:
            self.logger.error(f"Error publishing execution event: {e}")

    def cancel_order(self, internal_id: str) -> bool:
        """
        Cancel an order.

        Args:
            internal_id: Internal order ID

        Returns:
            True if cancellation was successful
        """
        try:
            order = self.order_tracker.get_order(internal_id)
            if not order:
                self.logger.error(f"Order not found: {internal_id}")
                return False

            if not order.broker_id:
                self.logger.error(f"Order not submitted to broker: {internal_id}")
                return False

            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]:
                self.logger.warning(f"Order cannot be cancelled (status: {order.status}): {internal_id}")
                return False

            # Cancel with broker
            success, message = self.broker.cancel_order(order.broker_id)

            if success:
                self.order_tracker.update_order(internal_id, status=OrderStatus.CANCELLED)
                self.logger.info(f"Order cancelled: {internal_id}")
                return True
            else:
                self.logger.error(f"Failed to cancel order: {message}")
                return False

        except Exception as e:
            self.logger.error(f"Error cancelling order {internal_id}: {e}")
            return False

    def get_order_status(self, internal_id: str) -> Optional[Dict[str, Any]]:
        """
        Get order status.

        Args:
            internal_id: Internal order ID

        Returns:
            Order status information or None
        """
        try:
            order = self.order_tracker.get_order(internal_id)
            if order:
                return {
                    'internal_id': order.internal_id,
                    'broker_id': order.broker_id,
                    'symbol': order.symbol,
                    'side': order.side,
                    'quantity': float(order.quantity),
                    'order_type': order.order_type,
                    'price': float(order.price) if order.price else None,
                    'status': order.status.value,
                    'filled_quantity': float(order.filled_quantity),
                    'average_fill_price': float(order.average_fill_price) if order.average_fill_price else None,
                    'commission': float(order.commission),
                    'submitted_at': order.submitted_at.isoformat() if order.submitted_at else None,
                    'last_updated': order.last_updated.isoformat()
                }
            return None

        except Exception as e:
            self.logger.error(f"Error getting order status for {internal_id}: {e}")
            return None

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get execution summary statistics."""
        try:
            orders = list(self.order_tracker.orders.values())

            total_orders = len(orders)
            filled_orders = len([o for o in orders if o.status == OrderStatus.FILLED])
            pending_orders = len([o for o in orders if o.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]])
            rejected_orders = len([o for o in orders if o.status == OrderStatus.REJECTED])

            return {
                'total_orders': total_orders,
                'filled_orders': filled_orders,
                'pending_orders': pending_orders,
                'rejected_orders': rejected_orders,
                'fill_rate': filled_orders / total_orders if total_orders > 0 else 0.0,
                'last_updated': datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Error getting execution summary: {e}")
            return {}

    def stop(self) -> None:
        """Stop the order gateway."""
        self.event_bus.close()
        self.logger.info("Order gateway stopped")


def main():
    """Main entry point for the order gateway service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    gateway = OrderGateway()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down order gateway...")
    finally:
        gateway.stop()


if __name__ == "__main__":
    main()

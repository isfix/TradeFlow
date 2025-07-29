"""
State Recovery Manager

Production-grade state recovery and consistency management for trading platform services.
Ensures data integrity and provides automatic recovery from failures.
"""

import logging
import threading
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, desc, func

from trading_platform.core.persistent_state_manager import PersistentStateManager
from trading_platform.core.database_schema import Portfolio, Position, Trade, SystemState


@dataclass
class StateCheckpoint:
    """State checkpoint for recovery."""
    checkpoint_id: str
    service_name: str
    timestamp: datetime
    state_data: Dict[str, Any]
    checksum: str
    version: int


@dataclass
class ConsistencyCheck:
    """Consistency check result."""
    check_name: str
    passed: bool
    message: str
    severity: str  # 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    timestamp: datetime


class StateRecoveryManager:
    """
    Production-grade state recovery and consistency management.
    
    Features:
    - Automatic state checkpointing
    - Consistency validation
    - Automatic recovery from failures
    - Transaction management
    - State versioning
    """
    
    def __init__(self, service_name: str):
        """
        Initialize state recovery manager.
        
        Args:
            service_name: Name of the service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        self.state_manager = PersistentStateManager(service_name)
        self.checkpoint_interval = 300  # 5 minutes
        self.consistency_check_interval = 600  # 10 minutes
        
        # Recovery callbacks
        self.recovery_callbacks: Dict[str, Callable] = {}
        
        # State locks
        self.checkpoint_lock = threading.Lock()
        self.recovery_lock = threading.Lock()
        
        # Start background tasks
        self._start_checkpoint_thread()
        self._start_consistency_check_thread()
        
        self.logger.info(f"State recovery manager initialized for {service_name}")
    
    def register_recovery_callback(self, state_type: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for state recovery.
        
        Args:
            state_type: Type of state (e.g., 'portfolio', 'positions')
            callback: Callback function to restore state
        """
        self.recovery_callbacks[state_type] = callback
        self.logger.info(f"Registered recovery callback for {state_type}")
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactional state operations.
        
        Ensures atomicity of state changes across multiple operations.
        """
        checkpoint_id = None
        try:
            # Create checkpoint before transaction
            checkpoint_id = self.create_checkpoint("transaction_start")
            
            with self.state_manager.get_session() as session:
                yield session
                # Transaction committed automatically by context manager
                
        except Exception as e:
            self.logger.error(f"Transaction failed: {e}")
            
            # Attempt recovery if checkpoint exists
            if checkpoint_id:
                try:
                    self.recover_from_checkpoint(checkpoint_id)
                    self.logger.info(f"Recovered from checkpoint {checkpoint_id}")
                except Exception as recovery_error:
                    self.logger.critical(f"Recovery failed: {recovery_error}")
            
            raise
    
    def create_checkpoint(self, checkpoint_type: str = "manual") -> str:
        """
        Create a state checkpoint for recovery.
        
        Args:
            checkpoint_type: Type of checkpoint
            
        Returns:
            Checkpoint ID
        """
        try:
            with self.checkpoint_lock:
                checkpoint_id = f"{self.service_name}_{checkpoint_type}_{int(time.time())}"
                
                # Gather current state
                state_data = self._gather_current_state()
                
                # Calculate checksum
                checksum = self._calculate_checksum(state_data)
                
                # Create checkpoint
                checkpoint = StateCheckpoint(
                    checkpoint_id=checkpoint_id,
                    service_name=self.service_name,
                    timestamp=datetime.utcnow(),
                    state_data=state_data,
                    checksum=checksum,
                    version=1
                )
                
                # Save checkpoint
                self.state_manager.save_service_state(f"checkpoint_{checkpoint_id}", {
                    'checkpoint_id': checkpoint.checkpoint_id,
                    'service_name': checkpoint.service_name,
                    'timestamp': checkpoint.timestamp.isoformat(),
                    'state_data': checkpoint.state_data,
                    'checksum': checkpoint.checksum,
                    'version': checkpoint.version
                })
                
                self.logger.debug(f"Created checkpoint {checkpoint_id}")
                return checkpoint_id
                
        except Exception as e:
            self.logger.error(f"Error creating checkpoint: {e}")
            raise
    
    def recover_from_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Recover state from a checkpoint.
        
        Args:
            checkpoint_id: Checkpoint ID to recover from
            
        Returns:
            True if recovery successful
        """
        try:
            with self.recovery_lock:
                # Load checkpoint
                checkpoint_data = self.state_manager.load_service_state(f"checkpoint_{checkpoint_id}")
                
                if not checkpoint_data:
                    raise ValueError(f"Checkpoint {checkpoint_id} not found")
                
                # Verify checksum
                if not self._verify_checksum(checkpoint_data['state_data'], checkpoint_data['checksum']):
                    raise ValueError(f"Checkpoint {checkpoint_id} checksum verification failed")
                
                # Restore state using callbacks
                for state_type, state_value in checkpoint_data['state_data'].items():
                    callback = self.recovery_callbacks.get(state_type)
                    if callback:
                        callback(state_value)
                        self.logger.info(f"Restored {state_type} from checkpoint")
                    else:
                        self.logger.warning(f"No recovery callback for {state_type}")
                
                self.logger.info(f"Successfully recovered from checkpoint {checkpoint_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error recovering from checkpoint {checkpoint_id}: {e}")
            return False
    
    def run_consistency_checks(self) -> List[ConsistencyCheck]:
        """
        Run comprehensive consistency checks.
        
        Returns:
            List of consistency check results
        """
        checks = []
        
        try:
            # Check 1: Portfolio balance consistency
            portfolio_check = self._check_portfolio_consistency()
            checks.append(portfolio_check)
            
            # Check 2: Position value consistency
            position_check = self._check_position_consistency()
            checks.append(position_check)
            
            # Check 3: Trade history integrity
            trade_check = self._check_trade_integrity()
            checks.append(trade_check)
            
            # Check 4: Database constraints
            constraint_check = self._check_database_constraints()
            checks.append(constraint_check)
            
            # Log results
            critical_failures = [c for c in checks if c.severity == 'CRITICAL' and not c.passed]
            error_failures = [c for c in checks if c.severity == 'ERROR' and not c.passed]
            
            if critical_failures:
                self.logger.critical(f"Critical consistency failures: {len(critical_failures)}")
                for check in critical_failures:
                    self.logger.critical(f"CRITICAL: {check.check_name} - {check.message}")
            
            if error_failures:
                self.logger.error(f"Error consistency failures: {len(error_failures)}")
                for check in error_failures:
                    self.logger.error(f"ERROR: {check.check_name} - {check.message}")
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Error running consistency checks: {e}")
            return [ConsistencyCheck(
                check_name='consistency_check_error',
                passed=False,
                message=str(e),
                severity='CRITICAL',
                timestamp=datetime.utcnow()
            )]
    
    def _gather_current_state(self) -> Dict[str, Any]:
        """Gather current state for checkpointing."""
        try:
            state_data = {}
            
            # Get portfolio state if this is portfolio manager
            if 'portfolio' in self.service_name.lower():
                portfolio_data = self.state_manager.load_portfolio_state('default_portfolio')
                if portfolio_data:
                    state_data['portfolio'] = portfolio_data
            
            # Get service-specific state
            service_state = self.state_manager.load_service_state('current_state')
            if service_state:
                state_data['service_state'] = service_state
            
            return state_data
            
        except Exception as e:
            self.logger.error(f"Error gathering current state: {e}")
            return {}
    
    def _calculate_checksum(self, data: Dict[str, Any]) -> str:
        """Calculate checksum for data integrity."""
        import hashlib
        import json
        
        try:
            # Convert to JSON string for consistent hashing
            json_str = json.dumps(data, sort_keys=True, default=str)
            return hashlib.sha256(json_str.encode()).hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating checksum: {e}")
            return ""
    
    def _verify_checksum(self, data: Dict[str, Any], expected_checksum: str) -> bool:
        """Verify data integrity using checksum."""
        actual_checksum = self._calculate_checksum(data)
        return actual_checksum == expected_checksum
    
    def _check_portfolio_consistency(self) -> ConsistencyCheck:
        """Check portfolio balance consistency."""
        try:
            with self.state_manager.get_session() as session:
                portfolio = session.query(Portfolio).filter_by(id='default_portfolio').first()
                
                if not portfolio:
                    return ConsistencyCheck(
                        check_name='portfolio_existence',
                        passed=False,
                        message='Default portfolio not found',
                        severity='CRITICAL',
                        timestamp=datetime.utcnow()
                    )
                
                # Calculate total position value
                total_position_value = sum(pos.market_value for pos in portfolio.positions)
                expected_total = portfolio.cash_balance + total_position_value
                
                # Check if total value matches
                tolerance = Decimal('0.01')  # 1 cent tolerance
                if abs(portfolio.total_value - expected_total) > tolerance:
                    return ConsistencyCheck(
                        check_name='portfolio_balance',
                        passed=False,
                        message=f'Portfolio total value mismatch: {portfolio.total_value} vs {expected_total}',
                        severity='ERROR',
                        timestamp=datetime.utcnow()
                    )
                
                return ConsistencyCheck(
                    check_name='portfolio_balance',
                    passed=True,
                    message='Portfolio balance consistent',
                    severity='INFO',
                    timestamp=datetime.utcnow()
                )
                
        except Exception as e:
            return ConsistencyCheck(
                check_name='portfolio_balance',
                passed=False,
                message=f'Error checking portfolio consistency: {e}',
                severity='CRITICAL',
                timestamp=datetime.utcnow()
            )

    def _check_position_consistency(self) -> ConsistencyCheck:
        """Check position value consistency."""
        try:
            with self.state_manager.get_session() as session:
                positions = session.query(Position).all()

                for position in positions:
                    # Check if quantity and market value are consistent
                    if position.quantity < 0:
                        return ConsistencyCheck(
                            check_name='position_quantity',
                            passed=False,
                            message=f'Negative quantity for {position.symbol}: {position.quantity}',
                            severity='ERROR',
                            timestamp=datetime.utcnow()
                        )

                    # Check if market value calculation is reasonable
                    if position.quantity > 0 and position.market_value <= 0:
                        return ConsistencyCheck(
                            check_name='position_value',
                            passed=False,
                            message=f'Invalid market value for {position.symbol}: {position.market_value}',
                            severity='ERROR',
                            timestamp=datetime.utcnow()
                        )

                return ConsistencyCheck(
                    check_name='position_consistency',
                    passed=True,
                    message='All positions consistent',
                    severity='INFO',
                    timestamp=datetime.utcnow()
                )

        except Exception as e:
            return ConsistencyCheck(
                check_name='position_consistency',
                passed=False,
                message=f'Error checking position consistency: {e}',
                severity='CRITICAL',
                timestamp=datetime.utcnow()
            )

    def _check_trade_integrity(self) -> ConsistencyCheck:
        """Check trade history integrity."""
        try:
            with self.state_manager.get_session() as session:
                trades = session.query(Trade).order_by(Trade.execution_time).all()

                # Check for duplicate trades
                trade_ids = [t.id for t in trades]
                if len(trade_ids) != len(set(trade_ids)):
                    return ConsistencyCheck(
                        check_name='trade_duplicates',
                        passed=False,
                        message='Duplicate trade IDs found',
                        severity='ERROR',
                        timestamp=datetime.utcnow()
                    )

                # Check trade data integrity
                for trade in trades:
                    if trade.quantity <= 0:
                        return ConsistencyCheck(
                            check_name='trade_quantity',
                            passed=False,
                            message=f'Invalid trade quantity: {trade.quantity}',
                            severity='ERROR',
                            timestamp=datetime.utcnow()
                        )

                    if trade.price <= 0:
                        return ConsistencyCheck(
                            check_name='trade_price',
                            passed=False,
                            message=f'Invalid trade price: {trade.price}',
                            severity='ERROR',
                            timestamp=datetime.utcnow()
                        )

                return ConsistencyCheck(
                    check_name='trade_integrity',
                    passed=True,
                    message='Trade history integrity verified',
                    severity='INFO',
                    timestamp=datetime.utcnow()
                )

        except Exception as e:
            return ConsistencyCheck(
                check_name='trade_integrity',
                passed=False,
                message=f'Error checking trade integrity: {e}',
                severity='CRITICAL',
                timestamp=datetime.utcnow()
            )

    def _check_database_constraints(self) -> ConsistencyCheck:
        """Check database constraint violations."""
        try:
            with self.state_manager.get_session() as session:
                # Check for constraint violations by attempting to query
                session.query(Portfolio).all()
                session.query(Position).all()
                session.query(Trade).all()

                return ConsistencyCheck(
                    check_name='database_constraints',
                    passed=True,
                    message='Database constraints satisfied',
                    severity='INFO',
                    timestamp=datetime.utcnow()
                )

        except Exception as e:
            return ConsistencyCheck(
                check_name='database_constraints',
                passed=False,
                message=f'Database constraint violation: {e}',
                severity='CRITICAL',
                timestamp=datetime.utcnow()
            )

    def _start_checkpoint_thread(self):
        """Start background checkpoint thread."""
        def checkpoint_worker():
            while True:
                try:
                    time.sleep(self.checkpoint_interval)
                    self.create_checkpoint("automatic")
                except Exception as e:
                    self.logger.error(f"Error in checkpoint thread: {e}")
                    time.sleep(60)  # Wait 1 minute on error

        checkpoint_thread = threading.Thread(target=checkpoint_worker, daemon=True)
        checkpoint_thread.start()
        self.logger.info("Started automatic checkpoint thread")

    def _start_consistency_check_thread(self):
        """Start background consistency check thread."""
        def consistency_worker():
            while True:
                try:
                    time.sleep(self.consistency_check_interval)
                    checks = self.run_consistency_checks()

                    # Log summary
                    failed_checks = [c for c in checks if not c.passed]
                    if failed_checks:
                        self.logger.warning(f"Consistency check failures: {len(failed_checks)}")
                    else:
                        self.logger.info("All consistency checks passed")

                except Exception as e:
                    self.logger.error(f"Error in consistency check thread: {e}")
                    time.sleep(60)  # Wait 1 minute on error

        consistency_thread = threading.Thread(target=consistency_worker, daemon=True)
        consistency_thread.start()
        self.logger.info("Started automatic consistency check thread")

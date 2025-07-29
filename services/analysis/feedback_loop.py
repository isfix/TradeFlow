"""
Feedback Loop

Analyzes trade performance and provides feedback for continuous model improvement
with comprehensive performance attribution and model updating mechanisms.
"""

import logging
import time
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from decimal import Decimal
from collections import defaultdict, deque
import json

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage, RequestMessage, ResponseMessage
from ...core.database_clients import get_database_manager
from ...core.persistent_state_manager import PersistentStateManager
from ...configs import load_services_config, get_topic_name


@dataclass
class TradePerformance:
    """Individual trade performance metrics."""
    trade_id: str
    symbol: str
    side: str
    entry_price: Decimal
    exit_price: Optional[Decimal]
    quantity: Decimal
    entry_time: datetime
    exit_time: Optional[datetime]
    pnl: Optional[Decimal]
    pnl_percentage: Optional[float]
    holding_period: Optional[timedelta]
    signal_source: str
    signal_confidence: float
    is_closed: bool


@dataclass
class PerformanceAttribution:
    """Performance attribution analysis."""
    period_start: datetime
    period_end: datetime
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float
    total_return: float
    attribution_by_signal: Dict[str, Dict[str, float]]
    attribution_by_symbol: Dict[str, Dict[str, float]]


@dataclass
class ModelFeedback:
    """Feedback for model improvement."""
    model_name: str
    feedback_type: str  # 'performance', 'prediction_accuracy', 'signal_quality'
    metrics: Dict[str, float]
    recommendations: List[str]
    data_period: Tuple[datetime, datetime]
    timestamp: datetime


class PerformanceCalculator:
    """Calculates various performance metrics."""
    
    def __init__(self):
        """Initialize performance calculator."""
        self.logger = logging.getLogger(f"{__name__}.PerformanceCalculator")
    
    def calculate_trade_performance(self, entry_trade: Dict[str, Any], 
                                  exit_trade: Optional[Dict[str, Any]] = None) -> TradePerformance:
        """
        Calculate performance for a single trade.
        
        Args:
            entry_trade: Entry trade data
            exit_trade: Exit trade data (None if position still open)
            
        Returns:
            Trade performance metrics
        """
        try:
            entry_price = Decimal(str(entry_trade.get('price', 0)))
            quantity = Decimal(str(entry_trade.get('quantity', 0)))
            entry_time = datetime.fromisoformat(entry_trade.get('timestamp', ''))
            
            if exit_trade:
                exit_price = Decimal(str(exit_trade.get('price', 0)))
                exit_time = datetime.fromisoformat(exit_trade.get('timestamp', ''))
                
                # Calculate P&L
                if entry_trade.get('side') == 'BUY':
                    pnl = (exit_price - entry_price) * quantity
                else:  # SELL (short)
                    pnl = (entry_price - exit_price) * quantity
                
                pnl_percentage = float(pnl / (entry_price * quantity)) * 100
                holding_period = exit_time - entry_time
                is_closed = True
            else:
                exit_price = None
                exit_time = None
                pnl = None
                pnl_percentage = None
                holding_period = None
                is_closed = False
            
            return TradePerformance(
                trade_id=entry_trade.get('trade_id', ''),
                symbol=entry_trade.get('symbol', ''),
                side=entry_trade.get('side', ''),
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=quantity,
                entry_time=entry_time,
                exit_time=exit_time,
                pnl=pnl,
                pnl_percentage=pnl_percentage,
                holding_period=holding_period,
                signal_source=entry_trade.get('signal_source', 'unknown'),
                signal_confidence=float(entry_trade.get('signal_confidence', 0.0)),
                is_closed=is_closed
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating trade performance: {e}")
            raise
    
    def calculate_portfolio_metrics(self, trades: List[TradePerformance], 
                                  period_start: datetime, period_end: datetime) -> PerformanceAttribution:
        """
        Calculate portfolio-level performance metrics.
        
        Args:
            trades: List of trade performances
            period_start: Analysis period start
            period_end: Analysis period end
            
        Returns:
            Performance attribution analysis
        """
        try:
            closed_trades = [t for t in trades if t.is_closed and t.pnl is not None]
            
            if not closed_trades:
                return self._empty_performance_attribution(period_start, period_end)
            
            # Basic metrics
            total_trades = len(closed_trades)
            winning_trades = len([t for t in closed_trades if t.pnl > 0])
            losing_trades = len([t for t in closed_trades if t.pnl < 0])
            
            win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
            
            # P&L metrics
            wins = [float(t.pnl) for t in closed_trades if t.pnl > 0]
            losses = [float(t.pnl) for t in closed_trades if t.pnl < 0]
            
            average_win = np.mean(wins) if wins else 0.0
            average_loss = np.mean(losses) if losses else 0.0
            
            # Profit factor
            total_wins = sum(wins) if wins else 0.0
            total_losses = abs(sum(losses)) if losses else 0.0
            profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
            
            # Total return
            total_return = sum(float(t.pnl) for t in closed_trades)
            
            # Sharpe ratio (simplified)
            returns = [float(t.pnl) for t in closed_trades]
            sharpe_ratio = self._calculate_sharpe_ratio(returns)
            
            # Maximum drawdown
            max_drawdown = self._calculate_max_drawdown(returns)
            
            # Attribution analysis
            attribution_by_signal = self._calculate_signal_attribution(closed_trades)
            attribution_by_symbol = self._calculate_symbol_attribution(closed_trades)
            
            return PerformanceAttribution(
                period_start=period_start,
                period_end=period_end,
                total_trades=total_trades,
                winning_trades=winning_trades,
                losing_trades=losing_trades,
                win_rate=win_rate,
                average_win=average_win,
                average_loss=average_loss,
                profit_factor=profit_factor,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                total_return=total_return,
                attribution_by_signal=attribution_by_signal,
                attribution_by_symbol=attribution_by_symbol
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating portfolio metrics: {e}")
            return self._empty_performance_attribution(period_start, period_end)
    
    def _empty_performance_attribution(self, period_start: datetime, period_end: datetime) -> PerformanceAttribution:
        """Create empty performance attribution."""
        return PerformanceAttribution(
            period_start=period_start,
            period_end=period_end,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=0.0,
            average_win=0.0,
            average_loss=0.0,
            profit_factor=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
            total_return=0.0,
            attribution_by_signal={},
            attribution_by_symbol={}
        )
    
    def _calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio."""
        try:
            if not returns or len(returns) < 2:
                return 0.0
            
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            
            if std_return == 0:
                return 0.0
            
            # Annualized Sharpe ratio (simplified)
            excess_return = mean_return - (risk_free_rate / 252)  # Daily risk-free rate
            sharpe = excess_return / std_return * np.sqrt(252)  # Annualized
            
            return float(sharpe)
            
        except Exception:
            return 0.0
    
    def _calculate_max_drawdown(self, returns: List[float]) -> float:
        """Calculate maximum drawdown."""
        try:
            if not returns:
                return 0.0
            
            cumulative = np.cumsum(returns)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = cumulative - running_max
            max_drawdown = np.min(drawdown)
            
            return float(max_drawdown)
            
        except Exception:
            return 0.0
    
    def _calculate_signal_attribution(self, trades: List[TradePerformance]) -> Dict[str, Dict[str, float]]:
        """Calculate performance attribution by signal source."""
        try:
            attribution = defaultdict(lambda: {'total_pnl': 0.0, 'trade_count': 0, 'win_rate': 0.0})
            
            for trade in trades:
                signal = trade.signal_source
                pnl = float(trade.pnl) if trade.pnl else 0.0
                
                attribution[signal]['total_pnl'] += pnl
                attribution[signal]['trade_count'] += 1
                
                if pnl > 0:
                    attribution[signal]['wins'] = attribution[signal].get('wins', 0) + 1
            
            # Calculate win rates
            for signal, metrics in attribution.items():
                wins = metrics.get('wins', 0)
                total = metrics['trade_count']
                metrics['win_rate'] = wins / total if total > 0 else 0.0
                metrics.pop('wins', None)  # Remove temporary field
            
            return dict(attribution)
            
        except Exception as e:
            self.logger.error(f"Error calculating signal attribution: {e}")
            return {}
    
    def _calculate_symbol_attribution(self, trades: List[TradePerformance]) -> Dict[str, Dict[str, float]]:
        """Calculate performance attribution by symbol."""
        try:
            attribution = defaultdict(lambda: {'total_pnl': 0.0, 'trade_count': 0, 'win_rate': 0.0})
            
            for trade in trades:
                symbol = trade.symbol
                pnl = float(trade.pnl) if trade.pnl else 0.0
                
                attribution[symbol]['total_pnl'] += pnl
                attribution[symbol]['trade_count'] += 1
                
                if pnl > 0:
                    attribution[symbol]['wins'] = attribution[symbol].get('wins', 0) + 1
            
            # Calculate win rates
            for symbol, metrics in attribution.items():
                wins = metrics.get('wins', 0)
                total = metrics['trade_count']
                metrics['win_rate'] = wins / total if total > 0 else 0.0
                metrics.pop('wins', None)  # Remove temporary field
            
            return dict(attribution)
            
        except Exception as e:
            self.logger.error(f"Error calculating symbol attribution: {e}")
            return {}


class ModelFeedbackGenerator:
    """Generates feedback for model improvement."""

    def __init__(self):
        """Initialize model feedback generator."""
        self.logger = logging.getLogger(f"{__name__}.ModelFeedbackGenerator")

    def generate_signal_quality_feedback(self, trades: List[TradePerformance]) -> List[ModelFeedback]:
        """
        Generate feedback on signal quality.

        Args:
            trades: List of trade performances

        Returns:
            List of model feedback
        """
        try:
            feedback_list = []

            # Group trades by signal source
            signal_groups = defaultdict(list)
            for trade in trades:
                if trade.is_closed:
                    signal_groups[trade.signal_source].append(trade)

            for signal_source, signal_trades in signal_groups.items():
                if len(signal_trades) < 5:  # Need minimum trades for analysis
                    continue

                # Calculate signal-specific metrics
                win_rate = len([t for t in signal_trades if t.pnl > 0]) / len(signal_trades)
                avg_confidence = np.mean([t.signal_confidence for t in signal_trades])

                # Analyze confidence vs performance correlation
                high_conf_trades = [t for t in signal_trades if t.signal_confidence > 0.7]
                low_conf_trades = [t for t in signal_trades if t.signal_confidence < 0.3]

                high_conf_win_rate = len([t for t in high_conf_trades if t.pnl > 0]) / len(high_conf_trades) if high_conf_trades else 0
                low_conf_win_rate = len([t for t in low_conf_trades if t.pnl > 0]) / len(low_conf_trades) if low_conf_trades else 0

                # Generate recommendations
                recommendations = []

                if win_rate < 0.4:
                    recommendations.append(f"Signal quality is poor (win rate: {win_rate:.2f}). Consider retraining or adjusting parameters.")

                if high_conf_win_rate < low_conf_win_rate:
                    recommendations.append("High confidence signals are performing worse than low confidence. Review confidence calibration.")

                if avg_confidence < 0.3:
                    recommendations.append("Average confidence is low. Model may be uncertain - consider more training data.")

                if not recommendations:
                    recommendations.append("Signal quality appears satisfactory. Continue monitoring.")

                # Create feedback
                feedback = ModelFeedback(
                    model_name=signal_source,
                    feedback_type='signal_quality',
                    metrics={
                        'win_rate': win_rate,
                        'average_confidence': avg_confidence,
                        'high_confidence_win_rate': high_conf_win_rate,
                        'low_confidence_win_rate': low_conf_win_rate,
                        'total_trades': len(signal_trades)
                    },
                    recommendations=recommendations,
                    data_period=(min(t.entry_time for t in signal_trades), max(t.entry_time for t in signal_trades)),
                    timestamp=datetime.utcnow()
                )

                feedback_list.append(feedback)

            return feedback_list

        except Exception as e:
            self.logger.error(f"Error generating signal quality feedback: {e}")
            return []

    def generate_prediction_accuracy_feedback(self, predictions: List[Dict[str, Any]],
                                            actual_outcomes: List[Dict[str, Any]]) -> List[ModelFeedback]:
        """
        Generate feedback on prediction accuracy.

        Args:
            predictions: List of model predictions
            actual_outcomes: List of actual market outcomes

        Returns:
            List of model feedback
        """
        try:
            feedback_list = []

            # Match predictions with outcomes
            matched_data = self._match_predictions_with_outcomes(predictions, actual_outcomes)

            if not matched_data:
                return feedback_list

            # Group by model
            model_groups = defaultdict(list)
            for item in matched_data:
                model_name = item['prediction'].get('model_name', 'unknown')
                model_groups[model_name].append(item)

            for model_name, model_data in model_groups.items():
                if len(model_data) < 10:  # Need minimum predictions for analysis
                    continue

                # Calculate accuracy metrics
                predictions_list = [item['prediction']['predicted_value'] for item in model_data]
                actuals_list = [item['actual']['actual_value'] for item in model_data]

                mae = np.mean(np.abs(np.array(predictions_list) - np.array(actuals_list)))
                mse = np.mean((np.array(predictions_list) - np.array(actuals_list)) ** 2)
                rmse = np.sqrt(mse)

                # Calculate directional accuracy
                pred_directions = [1 if p > 0 else -1 for p in predictions_list]
                actual_directions = [1 if a > 0 else -1 for a in actuals_list]
                directional_accuracy = np.mean([p == a for p, a in zip(pred_directions, actual_directions)])

                # Generate recommendations
                recommendations = []

                if directional_accuracy < 0.55:
                    recommendations.append(f"Directional accuracy is low ({directional_accuracy:.2f}). Model may need retraining.")

                if mae > np.std(actuals_list):
                    recommendations.append("Mean absolute error is high relative to data variance. Consider feature engineering.")

                if not recommendations:
                    recommendations.append("Prediction accuracy appears acceptable. Continue monitoring.")

                # Create feedback
                feedback = ModelFeedback(
                    model_name=model_name,
                    feedback_type='prediction_accuracy',
                    metrics={
                        'mae': float(mae),
                        'mse': float(mse),
                        'rmse': float(rmse),
                        'directional_accuracy': float(directional_accuracy),
                        'sample_size': len(model_data)
                    },
                    recommendations=recommendations,
                    data_period=(
                        min(datetime.fromisoformat(item['prediction']['timestamp']) for item in model_data),
                        max(datetime.fromisoformat(item['prediction']['timestamp']) for item in model_data)
                    ),
                    timestamp=datetime.utcnow()
                )

                feedback_list.append(feedback)

            return feedback_list

        except Exception as e:
            self.logger.error(f"Error generating prediction accuracy feedback: {e}")
            return []

    def _match_predictions_with_outcomes(self, predictions: List[Dict[str, Any]],
                                       outcomes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Match predictions with actual outcomes."""
        try:
            matched = []

            # Create lookup for outcomes by symbol and time
            outcome_lookup = {}
            for outcome in outcomes:
                symbol = outcome.get('symbol')
                timestamp = outcome.get('timestamp')
                if symbol and timestamp:
                    key = f"{symbol}_{timestamp}"
                    outcome_lookup[key] = outcome

            # Match predictions
            for prediction in predictions:
                symbol = prediction.get('symbol')
                timestamp = prediction.get('timestamp')
                if symbol and timestamp:
                    key = f"{symbol}_{timestamp}"
                    if key in outcome_lookup:
                        matched.append({
                            'prediction': prediction,
                            'actual': outcome_lookup[key]
                        })

            return matched

        except Exception as e:
            self.logger.error(f"Error matching predictions with outcomes: {e}")
            return []


class FeedbackLoop:
    """
    Feedback loop service for performance analysis and model improvement.

    Features:
    - Trade performance tracking
    - Performance attribution analysis
    - Model feedback generation
    - Continuous improvement recommendations
    """

    def __init__(self, service_name: str = "feedback_loop"):
        """
        Initialize feedback loop service with persistent state.

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

        # Initialize analyzers
        self.performance_calculator = PerformanceCalculator()
        self.feedback_generator = ModelFeedbackGenerator()

        # Portfolio ID for tracking
        self.portfolio_id = self.config.get('portfolio', {}).get('default_portfolio_id', 'default_portfolio')

        # Topic names
        self.executed_trades_topic = get_topic_name('execution', 'executed_trade_events')
        self.performance_events_topic = get_topic_name('analysis', 'performance_events')
        self.model_update_events_topic = get_topic_name('analysis', 'model_update_events')
        self.model_feedback_topic = get_topic_name('analysis', 'model_feedback')

        # Subscribe to executed trades
        self.event_bus.subscribe(
            topic=self.executed_trades_topic,
            callback=self._handle_executed_trade,
            consumer_group='feedback_analysis'
        )

        # Register request handlers
        self.event_bus.register_request_handler('get_performance_metrics', self._handle_performance_request)
        self.event_bus.register_request_handler('get_trade_analysis', self._handle_trade_analysis_request)

        # Start periodic analysis
        self._start_periodic_analysis()

        self.logger.info(f"Feedback loop service initialized for portfolio {self.portfolio_id}")

    def _handle_executed_trade(self, message: EventMessage) -> None:
        """
        Handle incoming executed trade event with proper trade matching.

        Args:
            message: Executed trade event message
        """
        try:
            trade_data = message.data
            symbol = trade_data.get('symbol')
            side = trade_data.get('side')
            portfolio_id = trade_data.get('portfolio_id', self.portfolio_id)

            if not symbol or not side:
                self.logger.warning("Trade missing required fields")
                return

            # Perform sophisticated trade matching and P&L calculation
            self._process_trade_with_matching(trade_data, portfolio_id)

            # Generate performance analysis
            self._analyze_trade_performance(trade_data, portfolio_id)

            # Generate model feedback if this completes a round trip
            self._generate_model_feedback(trade_data, portfolio_id)

        except Exception as e:
            self.logger.error(f"Error handling executed trade: {e}")

    def _process_trade_with_matching(self, trade_data: Dict[str, Any], portfolio_id: str) -> None:
        """
        Process trade with sophisticated matching logic for accurate P&L calculation.

        Args:
            trade_data: Trade execution data
            portfolio_id: Portfolio ID
        """
        try:
            symbol = trade_data['symbol']
            side = trade_data['side']
            quantity = Decimal(str(trade_data['quantity']))
            price = Decimal(str(trade_data['price']))
            execution_time = trade_data.get('execution_time', datetime.utcnow().isoformat())

            # Get recent trades for this symbol to perform matching
            recent_trades = self.state_manager.get_portfolio_trades(
                portfolio_id=portfolio_id,
                symbol=symbol,
                limit=50
            )

            if side == 'SELL':
                # For sell orders, match against previous buy orders using FIFO
                remaining_quantity = quantity
                total_cost_basis = Decimal('0')
                realized_pnl = Decimal('0')

                # Sort buy trades by execution time (FIFO)
                buy_trades = [t for t in recent_trades if t['side'] == 'BUY']
                buy_trades.sort(key=lambda x: x['execution_time'])

                for buy_trade in buy_trades:
                    if remaining_quantity <= 0:
                        break

                    buy_quantity = Decimal(str(buy_trade['quantity']))
                    buy_price = Decimal(str(buy_trade['price']))

                    # Calculate how much of this buy trade to match
                    matched_quantity = min(remaining_quantity, buy_quantity)

                    # Calculate P&L for this match
                    cost_basis = matched_quantity * buy_price
                    sale_proceeds = matched_quantity * price
                    trade_pnl = sale_proceeds - cost_basis

                    total_cost_basis += cost_basis
                    realized_pnl += trade_pnl
                    remaining_quantity -= matched_quantity

                    # Create trade performance record
                    performance = TradePerformance(
                        trade_id=f"{buy_trade['id']}_{trade_data.get('id', 'unknown')}",
                        symbol=symbol,
                        side='ROUND_TRIP',
                        entry_price=buy_price,
                        exit_price=price,
                        quantity=matched_quantity,
                        entry_time=datetime.fromisoformat(buy_trade['execution_time']),
                        exit_time=datetime.fromisoformat(execution_time),
                        pnl=trade_pnl,
                        pnl_percentage=float((trade_pnl / cost_basis) * 100) if cost_basis > 0 else 0.0,
                        holding_period=datetime.fromisoformat(execution_time) - datetime.fromisoformat(buy_trade['execution_time']),
                        signal_source=buy_trade.get('signal_source', 'unknown'),
                        signal_confidence=float(buy_trade.get('signal_confidence', 0.0)),
                        is_closed=True
                    )

                    # Publish performance event
                    self._publish_performance_event(performance)

                    self.logger.info(f"Matched {matched_quantity} {symbol}: P&L ${trade_pnl:.2f}")

                # Update trade data with realized P&L
                trade_data['realized_pnl'] = float(realized_pnl)

        except Exception as e:
            self.logger.error(f"Error processing trade matching: {e}")

    def _analyze_trade_performance(self, trade_data: Dict[str, Any], portfolio_id: str) -> None:
        """
        Analyze trade performance and generate insights.

        Args:
            trade_data: Trade execution data
            portfolio_id: Portfolio ID
        """
        try:
            symbol = trade_data['symbol']

            # Get performance metrics for this symbol
            symbol_trades = self.state_manager.get_portfolio_trades(
                portfolio_id=portfolio_id,
                symbol=symbol,
                limit=100
            )

            if len(symbol_trades) >= 2:  # Need at least entry and exit
                # Calculate symbol-specific performance metrics
                total_pnl = sum(Decimal(str(t.get('realized_pnl', 0))) for t in symbol_trades if t.get('realized_pnl'))
                win_count = sum(1 for t in symbol_trades if t.get('realized_pnl', 0) > 0)
                loss_count = sum(1 for t in symbol_trades if t.get('realized_pnl', 0) < 0)

                win_rate = win_count / (win_count + loss_count) if (win_count + loss_count) > 0 else 0

                performance_metrics = {
                    'symbol': symbol,
                    'total_trades': len(symbol_trades),
                    'total_pnl': float(total_pnl),
                    'win_rate': win_rate,
                    'win_count': win_count,
                    'loss_count': loss_count,
                    'analysis_time': datetime.utcnow().isoformat()
                }

                # Publish performance analysis
                self.event_bus.publish(
                    topic=self.performance_events_topic,
                    message=performance_metrics,
                    event_type='symbol_performance_analysis'
                )

        except Exception as e:
            self.logger.error(f"Error analyzing trade performance: {e}")

    def _generate_model_feedback(self, trade_data: Dict[str, Any], portfolio_id: str) -> None:
        """
        Generate feedback for model improvement based on trade outcomes.

        Args:
            trade_data: Trade execution data
            portfolio_id: Portfolio ID
        """
        try:
            # Only generate feedback for completed round trips (sells)
            if trade_data.get('side') != 'SELL':
                return

            symbol = trade_data['symbol']
            realized_pnl = trade_data.get('realized_pnl', 0)

            # Get the original signal that led to this trade
            signal_source = trade_data.get('signal_source')
            signal_confidence = trade_data.get('signal_confidence', 0)

            if signal_source and realized_pnl is not None:
                # Create feedback for the model
                feedback = {
                    'signal_source': signal_source,
                    'symbol': symbol,
                    'original_confidence': signal_confidence,
                    'actual_pnl': float(realized_pnl),
                    'outcome': 'positive' if realized_pnl > 0 else 'negative',
                    'feedback_type': 'trade_outcome',
                    'timestamp': datetime.utcnow().isoformat(),
                    'portfolio_id': portfolio_id
                }

                # Publish feedback to model services
                self.event_bus.publish(
                    topic=self.model_feedback_topic,
                    message=feedback,
                    event_type='model_feedback'
                )

                # Also send direct feedback to the specific model service
                try:
                    self.event_bus.request(
                        target_service=signal_source,
                        request_type='update_model_performance',
                        data=feedback,
                        timeout_seconds=5
                    )
                except Exception as e:
                    self.logger.warning(f"Could not send direct feedback to {signal_source}: {e}")

                self.logger.info(f"Generated model feedback for {signal_source}: {feedback['outcome']} outcome")

        except Exception as e:
            self.logger.error(f"Error generating model feedback: {e}")

    def _handle_performance_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for performance metrics."""
        try:
            portfolio_id = request.data.get('portfolio_id', self.portfolio_id)
            symbol = request.data.get('symbol')
            days = request.data.get('days', 30)

            # Get trades for the specified period
            trades = self.state_manager.get_portfolio_trades(
                portfolio_id=portfolio_id,
                symbol=symbol,
                limit=1000
            )

            # Calculate performance metrics
            total_pnl = sum(Decimal(str(t.get('realized_pnl', 0))) for t in trades if t.get('realized_pnl'))
            total_trades = len(trades)
            winning_trades = sum(1 for t in trades if t.get('realized_pnl', 0) > 0)
            losing_trades = sum(1 for t in trades if t.get('realized_pnl', 0) < 0)

            win_rate = winning_trades / total_trades if total_trades > 0 else 0
            avg_win = sum(Decimal(str(t.get('realized_pnl', 0))) for t in trades if t.get('realized_pnl', 0) > 0) / winning_trades if winning_trades > 0 else Decimal('0')
            avg_loss = sum(Decimal(str(t.get('realized_pnl', 0))) for t in trades if t.get('realized_pnl', 0) < 0) / losing_trades if losing_trades > 0 else Decimal('0')

            response_data = {
                'portfolio_id': portfolio_id,
                'symbol': symbol,
                'total_pnl': float(total_pnl),
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'avg_win': float(avg_win),
                'avg_loss': float(avg_loss),
                'analysis_period_days': days
            }

            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_performance_metrics_response',
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
                response_type='get_performance_metrics_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _handle_trade_analysis_request(self, request: RequestMessage) -> ResponseMessage:
        """Handle requests for detailed trade analysis."""
        try:
            portfolio_id = request.data.get('portfolio_id', self.portfolio_id)
            symbol = request.data.get('symbol')

            # Get recent trades
            trades = self.state_manager.get_portfolio_trades(
                portfolio_id=portfolio_id,
                symbol=symbol,
                limit=50
            )

            # Analyze trade patterns
            analysis = {
                'symbol': symbol,
                'recent_trades': len(trades),
                'trade_frequency': len(trades) / 30 if trades else 0,  # trades per day
                'last_trade': trades[0] if trades else None,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }

            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_trade_analysis_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data=analysis,
                success=True,
                correlation_id=request.correlation_id
            )

        except Exception as e:
            return ResponseMessage(
                request_id=request.request_id,
                response_type='get_trade_analysis_response',
                source_service=self.service_name,
                target_service=request.source_service,
                timestamp=datetime.utcnow(),
                data={},
                success=False,
                error_message=str(e),
                correlation_id=request.correlation_id
            )

    def _start_periodic_analysis(self) -> None:
        """Start periodic performance analysis."""
        import threading

        def analysis_worker():
            while True:
                try:
                    # Run analysis every hour
                    time.sleep(3600)
                    self._run_periodic_analysis()

                except Exception as e:
                    self.logger.error(f"Error in periodic analysis: {e}")
                    time.sleep(300)  # Wait 5 minutes on error

        analysis_thread = threading.Thread(target=analysis_worker, daemon=True)
        analysis_thread.start()
        self.logger.info("Started periodic analysis thread")

    def _run_periodic_analysis(self) -> None:
        """Run periodic performance analysis using database data."""
        try:
            # Analyze last 24 hours
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)

            # Get recent trades from database
            recent_trades = self.state_manager.get_portfolio_trades(
                portfolio_id=self.portfolio_id,
                limit=500
            )

            # Filter trades within the analysis period
            period_trades = [
                trade for trade in recent_trades
                if datetime.fromisoformat(trade['execution_time']) >= start_time
            ]

            if len(period_trades) < 2:
                self.logger.info("Insufficient trades for analysis")
                return

            # Calculate performance metrics
            total_pnl = sum(Decimal(str(t.get('realized_pnl', 0))) for t in period_trades if t.get('realized_pnl'))
            total_trades = len(period_trades)
            winning_trades = sum(1 for t in period_trades if t.get('realized_pnl', 0) > 0)
            losing_trades = sum(1 for t in period_trades if t.get('realized_pnl', 0) < 0)

            win_rate = winning_trades / total_trades if total_trades > 0 else 0

            # Create performance attribution
            attribution = {
                'period_start': start_time.isoformat(),
                'period_end': end_time.isoformat(),
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'total_pnl': float(total_pnl),
                'portfolio_id': self.portfolio_id
            }

                # Publish attribution analysis
                self._publish_attribution_analysis(attribution)

                # Generate model feedback
                feedback_list = self.feedback_generator.generate_signal_quality_feedback(trade_performances)

                for feedback in feedback_list:
                    self._publish_model_feedback(feedback)

        except Exception as e:
            self.logger.error(f"Error in periodic analysis: {e}")

    def _find_entry_trade(self, exit_trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find corresponding entry trade for an exit trade."""
        try:
            symbol = exit_trade.get('symbol')
            exit_time = datetime.fromisoformat(exit_trade.get('timestamp', ''))

            # Find most recent BUY trade for the same symbol before this SELL
            for trade in reversed(self.trade_history):
                if (trade.get('symbol') == symbol and
                    trade.get('side') == 'BUY' and
                    datetime.fromisoformat(trade.get('timestamp', '')) < exit_time):
                    return trade

            return None

        except Exception as e:
            self.logger.error(f"Error finding entry trade: {e}")
            return None

    def _publish_performance_event(self, performance: TradePerformance) -> None:
        """Publish trade performance event."""
        try:
            message = asdict(performance)
            # Convert non-serializable types
            message['entry_price'] = float(performance.entry_price)
            message['exit_price'] = float(performance.exit_price) if performance.exit_price else None
            message['quantity'] = float(performance.quantity)
            message['pnl'] = float(performance.pnl) if performance.pnl else None
            message['entry_time'] = performance.entry_time.isoformat()
            message['exit_time'] = performance.exit_time.isoformat() if performance.exit_time else None
            message['holding_period'] = str(performance.holding_period) if performance.holding_period else None

            self.event_bus.publish(
                topic=self.performance_events_topic,
                message=message,
                event_type='trade_performance'
            )

            # Save to database
            self.database_manager.save_document('trade_performances', message)

            self.logger.debug(f"Published performance event for trade {performance.trade_id}")

        except Exception as e:
            self.logger.error(f"Error publishing performance event: {e}")

    def _publish_attribution_analysis(self, attribution: PerformanceAttribution) -> None:
        """Publish performance attribution analysis."""
        try:
            message = asdict(attribution)
            # Convert datetime objects
            message['period_start'] = attribution.period_start.isoformat()
            message['period_end'] = attribution.period_end.isoformat()

            self.event_bus.publish(
                topic=self.performance_events_topic,
                message=message,
                event_type='performance_attribution'
            )

            # Save to database
            self.database_manager.save_document('performance_attributions', message)

            self.logger.info(f"Published attribution analysis: {attribution.total_trades} trades, {attribution.total_return:.2f} total return")

        except Exception as e:
            self.logger.error(f"Error publishing attribution analysis: {e}")

    def _publish_model_feedback(self, feedback: ModelFeedback) -> None:
        """Publish model feedback."""
        try:
            message = asdict(feedback)
            # Convert datetime objects
            message['data_period'] = [feedback.data_period[0].isoformat(), feedback.data_period[1].isoformat()]
            message['timestamp'] = feedback.timestamp.isoformat()

            self.event_bus.publish(
                topic=self.model_update_events_topic,
                message=message,
                event_type='model_feedback'
            )

            # Save to database
            self.database_manager.save_document('model_feedback', message)

            self.logger.info(f"Published model feedback for {feedback.model_name}: {feedback.feedback_type}")

        except Exception as e:
            self.logger.error(f"Error publishing model feedback: {e}")

    def get_performance_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get performance summary for the last N days.

        Args:
            days: Number of days to analyze

        Returns:
            Performance summary
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)

            # Filter trades
            recent_trades = [
                trade for trade in self.trade_history
                if datetime.fromisoformat(trade.get('timestamp', '')) >= start_time
            ]

            # Calculate basic metrics
            total_trades = len(recent_trades)
            buy_trades = len([t for t in recent_trades if t.get('side') == 'BUY'])
            sell_trades = len([t for t in recent_trades if t.get('side') == 'SELL'])

            return {
                'period_days': days,
                'total_trades': total_trades,
                'buy_trades': buy_trades,
                'sell_trades': sell_trades,
                'open_positions': len(self.open_positions),
                'symbols_traded': len(set(t.get('symbol') for t in recent_trades if t.get('symbol'))),
                'analysis_timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return {}

    def stop(self) -> None:
        """Stop the feedback loop service."""
        self.event_bus.close()
        self.logger.info("Feedback loop service stopped")


def main():
    """Main entry point for the feedback loop service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    feedback_loop = FeedbackLoop()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down feedback loop...")
    finally:
        feedback_loop.stop()


if __name__ == "__main__":
    main()

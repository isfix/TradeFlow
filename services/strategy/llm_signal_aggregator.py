"""
LLM Signal Aggregator

Acts as a meta-model, synthesizing multiple streams of structured and unstructured data
into a single, high-level qualitative signal using LLM with stateful aggregation.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading

# LLM imports
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class DataContext:
    """Aggregated data context for a symbol."""
    symbol: str
    timestamp: datetime
    feature_data: Optional[Dict[str, Any]] = None
    sentiment_data: Optional[Dict[str, Any]] = None
    forecast_data: Optional[Dict[str, Any]] = None
    fundamental_data: Optional[Dict[str, Any]] = None
    is_complete: bool = False
    timeout_at: Optional[datetime] = None


@dataclass
class EnrichedTradeSignal:
    """Enriched trade signal with LLM analysis."""
    symbol: str
    signal: str  # 'STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'
    confidence: float  # 0.0 to 1.0
    reasoning: str
    data_sources: List[str]
    feature_summary: Dict[str, Any]
    sentiment_summary: Dict[str, Any]
    forecast_summary: Dict[str, Any]
    risk_factors: List[str]
    timestamp: datetime
    correlation_id: str


class AggregationWindow:
    """Manages stateful aggregation of data streams."""
    
    def __init__(self, window_timeout: int = 300):  # 5 minutes default
        """
        Initialize aggregation window.
        
        Args:
            window_timeout: Timeout for data aggregation in seconds
        """
        self.window_timeout = window_timeout
        self.contexts: Dict[str, DataContext] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(f"{__name__}.AggregationWindow")
    
    def add_feature_data(self, symbol: str, data: Dict[str, Any]) -> Optional[DataContext]:
        """
        Add feature data to aggregation window.
        
        Args:
            symbol: Stock symbol
            data: Feature data
            
        Returns:
            Complete context if ready, None otherwise
        """
        with self.lock:
            context = self._get_or_create_context(symbol)
            context.feature_data = data
            return self._check_completion(context)
    
    def add_sentiment_data(self, symbol: str, data: Dict[str, Any]) -> Optional[DataContext]:
        """
        Add sentiment data to aggregation window.
        
        Args:
            symbol: Stock symbol
            data: Sentiment data
            
        Returns:
            Complete context if ready, None otherwise
        """
        with self.lock:
            context = self._get_or_create_context(symbol)
            context.sentiment_data = data
            return self._check_completion(context)
    
    def add_forecast_data(self, symbol: str, data: Dict[str, Any]) -> Optional[DataContext]:
        """
        Add forecast data to aggregation window.
        
        Args:
            symbol: Stock symbol
            data: Forecast data
            
        Returns:
            Complete context if ready, None otherwise
        """
        with self.lock:
            context = self._get_or_create_context(symbol)
            context.forecast_data = data
            return self._check_completion(context)
    
    def _get_or_create_context(self, symbol: str) -> DataContext:
        """Get or create data context for symbol."""
        if symbol not in self.contexts:
            self.contexts[symbol] = DataContext(
                symbol=symbol,
                timestamp=datetime.utcnow(),
                timeout_at=datetime.utcnow() + timedelta(seconds=self.window_timeout)
            )
        return self.contexts[symbol]
    
    def _check_completion(self, context: DataContext) -> Optional[DataContext]:
        """Check if context is complete."""
        # Consider complete if we have at least feature and sentiment data
        if (context.feature_data is not None and 
            context.sentiment_data is not None):
            context.is_complete = True
            # Remove from active contexts
            if context.symbol in self.contexts:
                del self.contexts[context.symbol]
            return context
        return None
    
    def cleanup_expired_contexts(self) -> List[str]:
        """
        Clean up expired contexts.
        
        Returns:
            List of symbols with expired contexts
        """
        with self.lock:
            current_time = datetime.utcnow()
            expired_symbols = []
            
            for symbol, context in list(self.contexts.items()):
                if context.timeout_at and current_time > context.timeout_at:
                    expired_symbols.append(symbol)
                    del self.contexts[symbol]
                    self.logger.warning(f"Context expired for {symbol}")
            
            return expired_symbols


class LLMSignalGenerator:
    """Generates trading signals using LLM analysis."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize LLM signal generator.
        
        Args:
            config: LLM configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.LLMSignalGenerator")
        
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package is required for LLM signal generation")
        
        openai.api_key = config['api_key']
        self.model = config.get('model', 'gpt-4')
        self.max_tokens = config.get('max_tokens', 1500)
        self.temperature = config.get('temperature', 0.1)
    
    def create_enriched_signal(self, context: DataContext) -> Optional[EnrichedTradeSignal]:
        """
        Create enriched trade signal from data context.
        
        Args:
            context: Aggregated data context
            
        Returns:
            Enriched trade signal or None
        """
        try:
            # Create structured prompt
            prompt = self._create_analysis_prompt(context)
            
            # Get LLM analysis
            response = self._call_llm(prompt)
            
            if response:
                return self._parse_llm_response(context, response)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating enriched signal for {context.symbol}: {e}")
            return None
    
    def _create_analysis_prompt(self, context: DataContext) -> str:
        """
        Create structured analysis prompt for LLM.
        
        Args:
            context: Data context
            
        Returns:
            Formatted prompt
        """
        prompt = f"""
You are an expert quantitative analyst and portfolio manager. Analyze the following comprehensive data for {context.symbol} and provide a trading recommendation.

TECHNICAL ANALYSIS DATA:
{json.dumps(context.feature_data, indent=2) if context.feature_data else "No technical data available"}

SENTIMENT ANALYSIS DATA:
{json.dumps(context.sentiment_data, indent=2) if context.sentiment_data else "No sentiment data available"}

FORECAST DATA:
{json.dumps(context.forecast_data, indent=2) if context.forecast_data else "No forecast data available"}

FUNDAMENTAL DATA:
{json.dumps(context.fundamental_data, indent=2) if context.fundamental_data else "No fundamental data available"}

Please provide your analysis in the following JSON format:
{{
    "signal": "<one of: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL>",
    "confidence": <float between 0.0 and 1.0>,
    "reasoning": "<detailed explanation of your recommendation>",
    "data_sources": [<list of data sources you considered>],
    "feature_summary": {{
        "key_indicators": [<list of most important technical indicators>],
        "trend_direction": "<bullish/bearish/neutral>",
        "momentum": "<strong/moderate/weak>"
    }},
    "sentiment_summary": {{
        "overall_sentiment": "<positive/negative/neutral>",
        "confidence_level": "<high/medium/low>",
        "key_themes": [<list of important themes from news>]
    }},
    "forecast_summary": {{
        "predicted_direction": "<up/down/sideways>",
        "time_horizon": "<short/medium/long term>",
        "confidence_in_forecast": "<high/medium/low>"
    }},
    "risk_factors": [<list of key risks to consider>]
}}

ANALYSIS GUIDELINES:
1. Synthesize ALL available data sources
2. Weight technical, sentiment, and fundamental factors appropriately
3. Consider market context and current conditions
4. Be conservative with STRONG_BUY/STRONG_SELL signals
5. Provide clear, actionable reasoning
6. Identify specific risk factors
7. Return ONLY the JSON object, no additional text

Your analysis:
"""
        return prompt
    
    def _call_llm(self, prompt: str, max_retries: int = 3) -> Optional[str]:
        """
        Call LLM with retry logic.
        
        Args:
            prompt: Analysis prompt
            max_retries: Maximum retry attempts
            
        Returns:
            LLM response or None
        """
        for attempt in range(max_retries):
            try:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert financial analyst providing structured trading recommendations."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                    timeout=60
                )
                
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                self.logger.error(f"LLM API error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def _parse_llm_response(self, context: DataContext, response: str) -> Optional[EnrichedTradeSignal]:
        """
        Parse LLM response into structured signal.
        
        Args:
            context: Data context
            response: LLM response
            
        Returns:
            Parsed signal or None
        """
        try:
            # Extract JSON from response
            start = response.find('{')
            end = response.rfind('}') + 1
            
            if start == -1 or end <= start:
                self.logger.error("No JSON found in LLM response")
                return None
            
            json_str = response[start:end]
            parsed = json.loads(json_str)
            
            # Validate required fields
            required_fields = ['signal', 'confidence', 'reasoning']
            for field in required_fields:
                if field not in parsed:
                    self.logger.error(f"Missing required field: {field}")
                    return None
            
            # Validate signal value
            valid_signals = ['STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL']
            if parsed['signal'] not in valid_signals:
                self.logger.error(f"Invalid signal: {parsed['signal']}")
                return None
            
            # Validate confidence
            confidence = float(parsed['confidence'])
            if not 0.0 <= confidence <= 1.0:
                confidence = max(0.0, min(1.0, confidence))
            
            # Create enriched signal
            signal = EnrichedTradeSignal(
                symbol=context.symbol,
                signal=parsed['signal'],
                confidence=confidence,
                reasoning=parsed['reasoning'],
                data_sources=parsed.get('data_sources', []),
                feature_summary=parsed.get('feature_summary', {}),
                sentiment_summary=parsed.get('sentiment_summary', {}),
                forecast_summary=parsed.get('forecast_summary', {}),
                risk_factors=parsed.get('risk_factors', []),
                timestamp=datetime.utcnow(),
                correlation_id=f"{context.symbol}_{int(time.time())}"
            )
            
            return signal
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse LLM response as JSON: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing LLM response: {e}")
            return None


class LLMSignalAggregator:
    """
    LLM Signal Aggregator service.

    Features:
    - Stateful aggregation of multiple data streams
    - Timeout mechanism for incomplete data
    - Structured LLM prompts for consistent analysis
    - Comprehensive signal generation with reasoning
    """

    def __init__(self, service_name: str = "llm_signal_aggregator"):
        """
        Initialize LLM signal aggregator.

        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")

        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()

        # Initialize aggregation and LLM components
        self.aggregation_window = AggregationWindow(window_timeout=300)  # 5 minutes

        llm_config = self.config.get('llm_services', {}).get('openai', {})
        self.llm_generator = LLMSignalGenerator(llm_config)

        # Topic names
        self.feature_events_topic = get_topic_name('processing', 'feature_events')
        self.sentiment_events_topic = get_topic_name('processing', 'sentiment_events')
        self.forecast_events_topic = get_topic_name('processing', 'forecast_events')
        self.enriched_signals_topic = get_topic_name('strategy', 'enriched_trade_signals')

        # Subscribe to data streams
        self.event_bus.subscribe(
            topic=self.feature_events_topic,
            callback=self._handle_feature_event,
            consumer_group='signal_aggregation'
        )

        self.event_bus.subscribe(
            topic=self.sentiment_events_topic,
            callback=self._handle_sentiment_event,
            consumer_group='signal_aggregation'
        )

        self.event_bus.subscribe(
            topic=self.forecast_events_topic,
            callback=self._handle_forecast_event,
            consumer_group='signal_aggregation'
        )

        # Start cleanup thread
        self._start_cleanup_thread()

        self.logger.info("LLM signal aggregator initialized")

    def _handle_feature_event(self, message: EventMessage) -> None:
        """
        Handle incoming feature event.

        Args:
            message: Feature event message
        """
        try:
            data = message.data
            symbol = data.get('symbol')

            if symbol:
                complete_context = self.aggregation_window.add_feature_data(symbol, data)
                if complete_context:
                    self._process_complete_context(complete_context)

        except Exception as e:
            self.logger.error(f"Error handling feature event: {e}")

    def _handle_sentiment_event(self, message: EventMessage) -> None:
        """
        Handle incoming sentiment event.

        Args:
            message: Sentiment event message
        """
        try:
            data = message.data
            # Extract symbol from symbols list or use a default approach
            symbols = data.get('symbols', [])
            if symbols:
                symbol = symbols[0]  # Use first symbol for now
                complete_context = self.aggregation_window.add_sentiment_data(symbol, data)
                if complete_context:
                    self._process_complete_context(complete_context)

        except Exception as e:
            self.logger.error(f"Error handling sentiment event: {e}")

    def _handle_forecast_event(self, message: EventMessage) -> None:
        """
        Handle incoming forecast event.

        Args:
            message: Forecast event message
        """
        try:
            data = message.data
            symbol = data.get('symbol')

            if symbol:
                complete_context = self.aggregation_window.add_forecast_data(symbol, data)
                if complete_context:
                    self._process_complete_context(complete_context)

        except Exception as e:
            self.logger.error(f"Error handling forecast event: {e}")

    def _process_complete_context(self, context: DataContext) -> None:
        """
        Process complete data context to generate signal.

        Args:
            context: Complete data context
        """
        try:
            self.logger.info(f"Processing complete context for {context.symbol}")

            # Generate enriched signal using LLM
            enriched_signal = self.llm_generator.create_enriched_signal(context)

            if enriched_signal:
                self._publish_enriched_signal(enriched_signal)
            else:
                self.logger.error(f"Failed to generate signal for {context.symbol}")

        except Exception as e:
            self.logger.error(f"Error processing complete context for {context.symbol}: {e}")

    def _publish_enriched_signal(self, signal: EnrichedTradeSignal) -> None:
        """
        Publish enriched trade signal to event bus.

        Args:
            signal: Enriched trade signal
        """
        try:
            message = asdict(signal)
            # Convert datetime to ISO string
            message['timestamp'] = signal.timestamp.isoformat()

            self.event_bus.publish(
                topic=self.enriched_signals_topic,
                message=message,
                event_type='enriched_signal',
                correlation_id=signal.correlation_id
            )

            # Save to document database
            self.database_manager.save_document('enriched_signals', message)

            self.logger.info(f"Published enriched signal for {signal.symbol}: {signal.signal} (confidence: {signal.confidence:.2f})")

        except Exception as e:
            self.logger.error(f"Error publishing enriched signal: {e}")

    def _start_cleanup_thread(self) -> None:
        """Start background thread for cleaning up expired contexts."""
        def cleanup_worker():
            while True:
                try:
                    expired_symbols = self.aggregation_window.cleanup_expired_contexts()
                    if expired_symbols:
                        self.logger.info(f"Cleaned up expired contexts for: {expired_symbols}")

                    time.sleep(60)  # Check every minute

                except Exception as e:
                    self.logger.error(f"Error in cleanup thread: {e}")
                    time.sleep(60)

        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        self.logger.info("Started cleanup thread")

    def get_aggregation_status(self) -> Dict[str, Any]:
        """
        Get status of data aggregation.

        Returns:
            Aggregation status information
        """
        with self.aggregation_window.lock:
            contexts = {}
            for symbol, context in self.aggregation_window.contexts.items():
                contexts[symbol] = {
                    'has_feature_data': context.feature_data is not None,
                    'has_sentiment_data': context.sentiment_data is not None,
                    'has_forecast_data': context.forecast_data is not None,
                    'is_complete': context.is_complete,
                    'created_at': context.timestamp.isoformat(),
                    'timeout_at': context.timeout_at.isoformat() if context.timeout_at else None
                }

            return {
                'active_contexts': len(self.aggregation_window.contexts),
                'contexts': contexts,
                'window_timeout_seconds': self.aggregation_window.window_timeout,
                'status_timestamp': datetime.utcnow().isoformat()
            }

    def force_process_context(self, symbol: str) -> bool:
        """
        Force processing of a context even if incomplete.

        Args:
            symbol: Stock symbol

        Returns:
            True if context was processed
        """
        try:
            with self.aggregation_window.lock:
                if symbol in self.aggregation_window.contexts:
                    context = self.aggregation_window.contexts[symbol]
                    del self.aggregation_window.contexts[symbol]

                    self._process_complete_context(context)
                    return True

            return False

        except Exception as e:
            self.logger.error(f"Error force processing context for {symbol}: {e}")
            return False

    def stop(self) -> None:
        """Stop the LLM signal aggregator."""
        self.event_bus.close()
        self.logger.info("LLM signal aggregator stopped")


def main():
    """Main entry point for the LLM signal aggregator service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    aggregator = LLMSignalAggregator()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down LLM signal aggregator...")
    finally:
        aggregator.stop()


if __name__ == "__main__":
    main()

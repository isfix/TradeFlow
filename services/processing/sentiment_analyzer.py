"""
Sentiment Analyzer

Converts unstructured news text into structured sentiment scores using
LLM with robust error handling and structured prompt templates.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from dataclasses import dataclass
import asyncio
from collections import deque

# LLM imports
try:
    from langchain.llms import OpenAI
    from langchain.chat_models import ChatOpenAI
    from langchain.prompts import PromptTemplate, ChatPromptTemplate
    from langchain.schema import HumanMessage, SystemMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from ...core.event_bus import get_event_bus, initialize_event_bus, EventMessage
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class SentimentResult:
    """Structured sentiment analysis result."""
    sentiment_score: float  # -1.0 to 1.0
    confidence: float  # 0.0 to 1.0
    key_topics: List[str]
    reasoning: str
    symbols_mentioned: List[str]
    impact_assessment: str  # 'positive', 'negative', 'neutral'


class PromptTemplateManager:
    """Manages LLM prompt templates for sentiment analysis."""
    
    def __init__(self):
        """Initialize prompt template manager."""
        self.logger = logging.getLogger(f"{__name__}.PromptTemplateManager")
    
    def get_sentiment_prompt(self) -> str:
        """
        Get the sentiment analysis prompt template.
        
        Returns:
            Formatted prompt template
        """
        return """
You are an expert financial analyst specializing in market sentiment analysis. 
Analyze the following news headline and summary for market sentiment.

News Headline: {headline}
News Summary: {summary}
Symbols Mentioned: {symbols}

Please provide your analysis in the following JSON format:
{{
    "sentiment_score": <float between -1.0 and 1.0, where -1.0 is very negative, 0.0 is neutral, 1.0 is very positive>,
    "confidence": <float between 0.0 and 1.0 indicating your confidence in the sentiment score>,
    "key_topics": [<list of key topics/themes identified in the news>],
    "reasoning": "<brief explanation of your sentiment assessment>",
    "symbols_mentioned": [<list of stock symbols that could be affected>],
    "impact_assessment": "<one of: 'positive', 'negative', 'neutral'>"
}}

Important guidelines:
1. Focus on market impact and investor sentiment
2. Consider both direct and indirect effects on mentioned companies
3. Be conservative with extreme sentiment scores (-1.0 or 1.0)
4. Provide clear reasoning for your assessment
5. Identify all relevant stock symbols that could be affected
6. Return ONLY the JSON object, no additional text

Your analysis:
"""
    
    def get_batch_sentiment_prompt(self) -> str:
        """
        Get prompt template for batch sentiment analysis.
        
        Returns:
            Batch analysis prompt template
        """
        return """
You are an expert financial analyst. Analyze the following news items for market sentiment.
Provide a JSON array with sentiment analysis for each item.

News Items:
{news_items}

For each news item, provide analysis in this JSON format:
{{
    "item_id": "<id of the news item>",
    "sentiment_score": <float between -1.0 and 1.0>,
    "confidence": <float between 0.0 and 1.0>,
    "key_topics": [<list of key topics>],
    "reasoning": "<brief explanation>",
    "symbols_mentioned": [<list of affected symbols>],
    "impact_assessment": "<positive/negative/neutral>"
}}

Return a JSON array containing analysis for all items. Return ONLY the JSON array.
"""


class LLMClient:
    """Client for LLM interactions with error handling and retries."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize LLM client.
        
        Args:
            config: LLM service configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.LLMClient")
        
        # Initialize LLM based on configuration
        if config.get('provider') == 'openai':
            self._init_openai()
        else:
            raise ValueError(f"Unsupported LLM provider: {config.get('provider')}")
    
    def _init_openai(self) -> None:
        """Initialize OpenAI client."""
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package is required for OpenAI LLM")
        
        openai.api_key = self.config['api_key']
        self.model = self.config.get('model', 'gpt-3.5-turbo')
        self.max_tokens = self.config.get('max_tokens', 1000)
        self.temperature = self.config.get('temperature', 0.1)
        self.timeout = self.config.get('timeout', 60)
    
    def analyze_sentiment(self, prompt: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Analyze sentiment using LLM.
        
        Args:
            prompt: Formatted prompt
            max_retries: Maximum retry attempts
            
        Returns:
            Parsed sentiment result or None
        """
        for attempt in range(max_retries):
            try:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a financial sentiment analysis expert."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                    timeout=self.timeout
                )
                
                content = response.choices[0].message.content.strip()
                
                # Parse JSON response
                try:
                    result = json.loads(content)
                    return self._validate_sentiment_result(result)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse LLM response as JSON: {e}")
                    # Try to extract JSON from response
                    result = self._extract_json_from_text(content)
                    if result:
                        return self._validate_sentiment_result(result)
                
            except Exception as e:
                self.logger.error(f"LLM API error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def _validate_sentiment_result(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validate and clean sentiment result.
        
        Args:
            result: Raw LLM result
            
        Returns:
            Validated result or None
        """
        try:
            # Check required fields
            required_fields = ['sentiment_score', 'confidence', 'key_topics', 'reasoning', 'symbols_mentioned', 'impact_assessment']
            for field in required_fields:
                if field not in result:
                    self.logger.warning(f"Missing required field: {field}")
                    return None
            
            # Validate ranges
            sentiment_score = float(result['sentiment_score'])
            if not -1.0 <= sentiment_score <= 1.0:
                self.logger.warning(f"Invalid sentiment score: {sentiment_score}")
                sentiment_score = max(-1.0, min(1.0, sentiment_score))
                result['sentiment_score'] = sentiment_score
            
            confidence = float(result['confidence'])
            if not 0.0 <= confidence <= 1.0:
                self.logger.warning(f"Invalid confidence: {confidence}")
                confidence = max(0.0, min(1.0, confidence))
                result['confidence'] = confidence
            
            # Validate impact assessment
            valid_impacts = ['positive', 'negative', 'neutral']
            if result['impact_assessment'] not in valid_impacts:
                self.logger.warning(f"Invalid impact assessment: {result['impact_assessment']}")
                result['impact_assessment'] = 'neutral'
            
            # Ensure lists
            if not isinstance(result['key_topics'], list):
                result['key_topics'] = []
            if not isinstance(result['symbols_mentioned'], list):
                result['symbols_mentioned'] = []
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error validating sentiment result: {e}")
            return None
    
    def _extract_json_from_text(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Try to extract JSON from text response.
        
        Args:
            text: Text containing JSON
            
        Returns:
            Extracted JSON or None
        """
        try:
            # Look for JSON-like content between braces
            start = text.find('{')
            end = text.rfind('}') + 1
            
            if start != -1 and end > start:
                json_str = text[start:end]
                return json.loads(json_str)
            
            return None
            
        except Exception:
            return None


class DeadLetterQueue:
    """Manages failed sentiment analysis requests."""
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize dead letter queue.
        
        Args:
            max_size: Maximum queue size
        """
        self.queue = deque(maxlen=max_size)
        self.logger = logging.getLogger(f"{__name__}.DeadLetterQueue")
    
    def add_failed_item(self, news_item: Dict[str, Any], error: str) -> None:
        """
        Add failed item to queue.
        
        Args:
            news_item: News item that failed processing
            error: Error description
        """
        failed_item = {
            'news_item': news_item,
            'error': error,
            'timestamp': datetime.utcnow().isoformat(),
            'retry_count': news_item.get('retry_count', 0) + 1
        }
        
        self.queue.append(failed_item)
        self.logger.warning(f"Added item to dead letter queue: {error}")
    
    def get_failed_items(self) -> List[Dict[str, Any]]:
        """Get all failed items."""
        return list(self.queue)
    
    def clear(self) -> None:
        """Clear the queue."""
        self.queue.clear()


class SentimentAnalyzer:
    """
    Sentiment analyzer with LLM integration and error handling.
    
    Features:
    - Structured prompt templates via Langchain
    - Robust error handling and retries
    - Dead letter queue for failed analyses
    - Batch processing capabilities
    """
    
    def __init__(self, service_name: str = "sentiment_analyzer"):
        """
        Initialize sentiment analyzer.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Initialize LLM client
        llm_config = self.config.get('llm_services', {}).get('openai', {})
        llm_config['provider'] = 'openai'
        self.llm_client = LLMClient(llm_config)
        
        # Initialize other components
        self.prompt_manager = PromptTemplateManager()
        self.dead_letter_queue = DeadLetterQueue()
        
        # Topic names
        self.news_events_topic = get_topic_name('data_ingestion', 'news_events')
        self.sentiment_events_topic = get_topic_name('processing', 'sentiment_events')
        
        # Subscribe to news events
        self.event_bus.subscribe(
            topic=self.news_events_topic,
            callback=self._handle_news_event,
            consumer_group='sentiment_analysis'
        )
        
        self.logger.info("Sentiment analyzer initialized")
    
    def _handle_news_event(self, message: EventMessage) -> None:
        """
        Handle incoming news event message.
        
        Args:
            message: News event message
        """
        try:
            news_data = message.data
            
            # Analyze sentiment
            sentiment_result = self.analyze_sentiment_langchain(news_data)
            
            if sentiment_result:
                self._publish_sentiment_result(news_data, sentiment_result)
            else:
                # Add to dead letter queue
                self.dead_letter_queue.add_failed_item(
                    news_data, 
                    "Failed to analyze sentiment"
                )
            
        except Exception as e:
            self.logger.error(f"Error handling news event: {e}")
            self.dead_letter_queue.add_failed_item(
                message.data,
                f"Exception in handler: {str(e)}"
            )

    def analyze_sentiment_langchain(self, news_data: Dict[str, Any]) -> Optional[SentimentResult]:
        """
        Analyze sentiment using Langchain-managed prompts.

        Args:
            news_data: News item data

        Returns:
            Sentiment analysis result or None
        """
        try:
            # Extract news information
            headline = news_data.get('headline', '')
            summary = news_data.get('summary', '')
            symbols = news_data.get('symbols', [])

            if not headline:
                self.logger.warning("News item missing headline")
                return None

            # Format prompt
            prompt = self.prompt_manager.get_sentiment_prompt().format(
                headline=headline,
                summary=summary or "No summary available",
                symbols=', '.join(symbols) if symbols else "None specified"
            )

            # Analyze with LLM
            result = self.llm_client.analyze_sentiment(prompt)

            if result:
                return SentimentResult(
                    sentiment_score=result['sentiment_score'],
                    confidence=result['confidence'],
                    key_topics=result['key_topics'],
                    reasoning=result['reasoning'],
                    symbols_mentioned=result['symbols_mentioned'],
                    impact_assessment=result['impact_assessment']
                )

            return None

        except Exception as e:
            self.logger.error(f"Error in sentiment analysis: {e}")
            return None

    def analyze_batch_sentiment(self, news_items: List[Dict[str, Any]]) -> List[Optional[SentimentResult]]:
        """
        Analyze sentiment for multiple news items in batch.

        Args:
            news_items: List of news items

        Returns:
            List of sentiment results
        """
        try:
            # Format news items for batch prompt
            formatted_items = []
            for i, item in enumerate(news_items):
                formatted_item = {
                    'id': str(i),
                    'headline': item.get('headline', ''),
                    'summary': item.get('summary', ''),
                    'symbols': item.get('symbols', [])
                }
                formatted_items.append(formatted_item)

            # Create batch prompt
            prompt = self.prompt_manager.get_batch_sentiment_prompt().format(
                news_items=json.dumps(formatted_items, indent=2)
            )

            # Analyze with LLM
            response = self.llm_client.analyze_sentiment(prompt)

            if response and isinstance(response, list):
                results = []
                for item_result in response:
                    if self.llm_client._validate_sentiment_result(item_result):
                        results.append(SentimentResult(
                            sentiment_score=item_result['sentiment_score'],
                            confidence=item_result['confidence'],
                            key_topics=item_result['key_topics'],
                            reasoning=item_result['reasoning'],
                            symbols_mentioned=item_result['symbols_mentioned'],
                            impact_assessment=item_result['impact_assessment']
                        ))
                    else:
                        results.append(None)
                return results

            # Fallback to individual analysis
            return [self.analyze_sentiment_langchain(item) for item in news_items]

        except Exception as e:
            self.logger.error(f"Error in batch sentiment analysis: {e}")
            # Fallback to individual analysis
            return [self.analyze_sentiment_langchain(item) for item in news_items]

    def _publish_sentiment_result(self, news_data: Dict[str, Any], sentiment_result: SentimentResult) -> None:
        """
        Publish sentiment analysis result to event bus.

        Args:
            news_data: Original news data
            sentiment_result: Sentiment analysis result
        """
        try:
            message = {
                'news_id': news_data.get('id'),
                'headline': news_data.get('headline'),
                'source': news_data.get('source'),
                'published_at': news_data.get('published_at'),
                'symbols': news_data.get('symbols', []),
                'sentiment_score': sentiment_result.sentiment_score,
                'confidence': sentiment_result.confidence,
                'key_topics': sentiment_result.key_topics,
                'reasoning': sentiment_result.reasoning,
                'symbols_mentioned': sentiment_result.symbols_mentioned,
                'impact_assessment': sentiment_result.impact_assessment,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }

            self.event_bus.publish(
                topic=self.sentiment_events_topic,
                message=message,
                event_type='sentiment_analysis'
            )

            # Save to document database
            self.database_manager.save_document('sentiment_analysis', message)

            self.logger.debug(f"Published sentiment analysis for news ID: {news_data.get('id')}")

        except Exception as e:
            self.logger.error(f"Error publishing sentiment result: {e}")

    def get_sentiment_summary(self, symbol: str, hours: int = 24) -> Optional[Dict[str, Any]]:
        """
        Get sentiment summary for a symbol over a time period.

        Args:
            symbol: Stock symbol
            hours: Number of hours to look back

        Returns:
            Sentiment summary or None
        """
        try:
            # This would typically query the database for recent sentiment data
            # For now, return a placeholder implementation

            summary = {
                'symbol': symbol,
                'time_period_hours': hours,
                'average_sentiment': 0.0,
                'sentiment_count': 0,
                'positive_count': 0,
                'negative_count': 0,
                'neutral_count': 0,
                'confidence_average': 0.0,
                'key_topics': [],
                'generated_at': datetime.utcnow().isoformat()
            }

            return summary

        except Exception as e:
            self.logger.error(f"Error generating sentiment summary for {symbol}: {e}")
            return None

    def get_dead_letter_queue_status(self) -> Dict[str, Any]:
        """
        Get status of the dead letter queue.

        Returns:
            Dead letter queue status
        """
        failed_items = self.dead_letter_queue.get_failed_items()

        return {
            'total_failed_items': len(failed_items),
            'recent_failures': failed_items[-10:] if failed_items else [],
            'queue_capacity': self.dead_letter_queue.queue.maxlen,
            'last_updated': datetime.utcnow().isoformat()
        }

    def retry_failed_items(self, max_retries: int = 3) -> int:
        """
        Retry processing failed items from dead letter queue.

        Args:
            max_retries: Maximum retry attempts per item

        Returns:
            Number of items successfully processed
        """
        failed_items = self.dead_letter_queue.get_failed_items()
        successful_count = 0

        for failed_item in failed_items:
            if failed_item['retry_count'] >= max_retries:
                continue

            try:
                news_data = failed_item['news_item']
                sentiment_result = self.analyze_sentiment_langchain(news_data)

                if sentiment_result:
                    self._publish_sentiment_result(news_data, sentiment_result)
                    successful_count += 1

            except Exception as e:
                self.logger.error(f"Error retrying failed item: {e}")

        # Clear successfully processed items
        if successful_count > 0:
            self.dead_letter_queue.clear()
            self.logger.info(f"Successfully reprocessed {successful_count} failed items")

        return successful_count

    def stop(self) -> None:
        """Stop the sentiment analyzer."""
        self.event_bus.close()
        self.logger.info("Sentiment analyzer stopped")


def main():
    """Main entry point for the sentiment analyzer service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    analyzer = SentimentAnalyzer()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down sentiment analyzer...")
    finally:
        analyzer.stop()


if __name__ == "__main__":
    main()

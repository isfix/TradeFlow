"""
News Ingestor

Fetches news headlines, press releases, and text-based data from multiple sources
with rate limiting, deduplication, and intelligent scheduling.
"""

import asyncio
import hashlib
import logging
import time
import requests
import schedule
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import json

from ...core.event_bus import get_event_bus, initialize_event_bus
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class NewsItem:
    """Standard news item structure."""
    id: str
    headline: str
    summary: Optional[str]
    content: Optional[str]
    source: str
    author: Optional[str]
    published_at: datetime
    url: Optional[str]
    symbols: List[str]
    metadata: Optional[Dict[str, Any]] = None


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, requests_per_minute: int):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        self.requests = []
        self.logger = logging.getLogger(f"{__name__}.RateLimiter")
    
    def can_make_request(self) -> bool:
        """
        Check if a request can be made without exceeding rate limit.
        
        Returns:
            True if request can be made, False otherwise
        """
        now = time.time()
        
        # Remove requests older than 1 minute
        self.requests = [req_time for req_time in self.requests if now - req_time < 60]
        
        return len(self.requests) < self.requests_per_minute
    
    def record_request(self) -> None:
        """Record that a request was made."""
        self.requests.append(time.time())
    
    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limit."""
        if not self.can_make_request():
            # Calculate wait time
            oldest_request = min(self.requests) if self.requests else time.time()
            wait_time = 60 - (time.time() - oldest_request) + 1  # Add 1 second buffer
            
            if wait_time > 0:
                self.logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                time.sleep(wait_time)


class DeduplicationManager:
    """Manages deduplication of news items."""
    
    def __init__(self, cache_size: int = 10000):
        """
        Initialize deduplication manager.
        
        Args:
            cache_size: Maximum number of hashes to keep in memory
        """
        self.cache_size = cache_size
        self.seen_hashes: Set[str] = set()
        self.database_manager = get_database_manager()
        self.logger = logging.getLogger(f"{__name__}.DeduplicationManager")
    
    def generate_hash(self, headline: str, published_at: datetime) -> str:
        """
        Generate hash for news item.
        
        Args:
            headline: News headline
            published_at: Publication timestamp
            
        Returns:
            Hash string
        """
        content = f"{headline}_{published_at.isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def is_duplicate(self, news_item: NewsItem) -> bool:
        """
        Check if news item is a duplicate.
        
        Args:
            news_item: News item to check
            
        Returns:
            True if duplicate, False otherwise
        """
        item_hash = self.generate_hash(news_item.headline, news_item.published_at)
        
        # Check in-memory cache first
        if item_hash in self.seen_hashes:
            return True
        
        # Check in database
        if self._check_database_for_hash(item_hash):
            self.seen_hashes.add(item_hash)
            return True
        
        # Not a duplicate
        self.seen_hashes.add(item_hash)
        
        # Manage cache size
        if len(self.seen_hashes) > self.cache_size:
            # Remove oldest entries (simple approach - remove half)
            hashes_to_remove = list(self.seen_hashes)[:self.cache_size // 2]
            for hash_to_remove in hashes_to_remove:
                self.seen_hashes.remove(hash_to_remove)
        
        return False
    
    def _check_database_for_hash(self, item_hash: str) -> bool:
        """
        Check if hash exists in database.
        
        Args:
            item_hash: Hash to check
            
        Returns:
            True if hash exists, False otherwise
        """
        try:
            mongo_client = self.database_manager.get_client('mongodb')
            if mongo_client:
                # Query news_hashes collection
                collection = mongo_client.database['news_hashes']
                result = collection.find_one({'hash': item_hash})
                return result is not None
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking database for hash: {e}")
            return False
    
    def store_hash(self, news_item: NewsItem) -> None:
        """
        Store hash in database for persistence.
        
        Args:
            news_item: News item
        """
        try:
            item_hash = self.generate_hash(news_item.headline, news_item.published_at)
            
            hash_document = {
                'hash': item_hash,
                'headline': news_item.headline,
                'published_at': news_item.published_at,
                'source': news_item.source,
                'created_at': datetime.utcnow()
            }
            
            self.database_manager.save_document('news_hashes', hash_document)
            
        except Exception as e:
            self.logger.error(f"Error storing hash in database: {e}")


class NewsIngestor:
    """
    News ingestor with intelligent scheduling and deduplication.
    
    Features:
    - Rate limiting for multiple API providers
    - Deduplication using content hashing
    - Intelligent scheduling and polling
    - Data validation and normalization
    """
    
    def __init__(self, service_name: str = "news_ingestor"):
        """
        Initialize news ingestor.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        
        # Rate limiters for each source
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.deduplication_manager = DeduplicationManager()
        
        # Initialize rate limiters
        news_data_config = self.config.get('news_data', {})
        for source_name, source_config in news_data_config.items():
            rate_limit = source_config.get('rate_limit', 60)
            self.rate_limiters[source_name] = RateLimiter(rate_limit)
        
        # Control flags
        self.running = False
        
        # Topic names
        self.news_events_topic = get_topic_name('data_ingestion', 'news_events')
    
    def start_scheduled_fetching(self) -> None:
        """Start scheduled news fetching."""
        self.running = True
        
        # Schedule news fetching for different sources
        schedule.every(5).minutes.do(self._fetch_benzinga_news)
        schedule.every(10).minutes.do(self._fetch_news_api)
        
        self.logger.info("Started scheduled news fetching")
        
        # Run scheduler
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def fetch_news_api(self, source: str, symbols: Optional[List[str]] = None) -> List[NewsItem]:
        """
        Fetch news from a specific API source.
        
        Args:
            source: Source name ('benzinga', 'news_api', etc.)
            symbols: Optional list of symbols to filter by
            
        Returns:
            List of news items
        """
        if source == 'benzinga':
            return self._fetch_benzinga_news(symbols)
        elif source == 'news_api':
            return self._fetch_news_api(symbols)
        else:
            self.logger.warning(f"Unknown news source: {source}")
            return []
    
    def _fetch_benzinga_news(self, symbols: Optional[List[str]] = None) -> List[NewsItem]:
        """
        Fetch news from Benzinga API.
        
        Args:
            symbols: Optional list of symbols to filter by
            
        Returns:
            List of news items
        """
        try:
            rate_limiter = self.rate_limiters.get('benzinga')
            if not rate_limiter:
                return []
            
            rate_limiter.wait_if_needed()
            
            config = self.config['news_data']['benzinga']
            
            # Build API request
            url = f"{config['base_url']}/api/v2/news"
            headers = {'accept': 'application/json'}
            params = {
                'token': config['api_key'],
                'pagesize': 100,
                'displayOutput': 'full'
            }
            
            if symbols:
                params['tickers'] = ','.join(symbols)
            
            # Make request
            response = requests.get(url, headers=headers, params=params, timeout=config.get('timeout', 30))
            rate_limiter.record_request()
            
            if response.status_code == 200:
                data = response.json()
                news_items = []
                
                for item in data.get('data', []):
                    news_item = self._parse_benzinga_item(item)
                    if news_item and not self.deduplication_manager.is_duplicate(news_item):
                        news_items.append(news_item)
                        self._publish_news_item(news_item)
                        self.deduplication_manager.store_hash(news_item)
                
                self.logger.info(f"Fetched {len(news_items)} new items from Benzinga")
                return news_items
            else:
                self.logger.error(f"Benzinga API error: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching Benzinga news: {e}")
            return []

    def _fetch_news_api(self, symbols: Optional[List[str]] = None) -> List[NewsItem]:
        """
        Fetch news from News API.

        Args:
            symbols: Optional list of symbols to filter by

        Returns:
            List of news items
        """
        try:
            rate_limiter = self.rate_limiters.get('news_api')
            if not rate_limiter:
                return []

            rate_limiter.wait_if_needed()

            config = self.config['news_data']['news_api']

            # Build API request
            url = f"{config['base_url']}/everything"
            headers = {'X-API-Key': config['api_key']}
            params = {
                'q': 'stock market OR trading OR finance',
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 100
            }

            if symbols:
                # Add symbols to query
                symbol_query = ' OR '.join(symbols)
                params['q'] = f"({params['q']}) AND ({symbol_query})"

            # Make request
            response = requests.get(url, headers=headers, params=params, timeout=config.get('timeout', 30))
            rate_limiter.record_request()

            if response.status_code == 200:
                data = response.json()
                news_items = []

                for item in data.get('articles', []):
                    news_item = self._parse_news_api_item(item)
                    if news_item and not self.deduplication_manager.is_duplicate(news_item):
                        news_items.append(news_item)
                        self._publish_news_item(news_item)
                        self.deduplication_manager.store_hash(news_item)

                self.logger.info(f"Fetched {len(news_items)} new items from News API")
                return news_items
            else:
                self.logger.error(f"News API error: {response.status_code} - {response.text}")
                return []

        except Exception as e:
            self.logger.error(f"Error fetching News API: {e}")
            return []

    def _parse_benzinga_item(self, item: Dict[str, Any]) -> Optional[NewsItem]:
        """
        Parse Benzinga news item.

        Args:
            item: Raw Benzinga item

        Returns:
            Parsed news item or None
        """
        try:
            # Extract symbols from tickers
            symbols = []
            if 'tickers' in item:
                symbols = [ticker['name'] for ticker in item['tickers']]

            return NewsItem(
                id=str(item.get('id')),
                headline=item.get('title', ''),
                summary=item.get('teaser'),
                content=item.get('body'),
                source='benzinga',
                author=item.get('author'),
                published_at=datetime.fromisoformat(item.get('created').replace('Z', '+00:00')),
                url=item.get('url'),
                symbols=symbols,
                metadata={
                    'channels': item.get('channels', []),
                    'tags': item.get('tags', [])
                }
            )

        except Exception as e:
            self.logger.error(f"Error parsing Benzinga item: {e}")
            return None

    def _parse_news_api_item(self, item: Dict[str, Any]) -> Optional[NewsItem]:
        """
        Parse News API item.

        Args:
            item: Raw News API item

        Returns:
            Parsed news item or None
        """
        try:
            # Extract symbols from title and description (simple approach)
            symbols = self._extract_symbols_from_text(
                f"{item.get('title', '')} {item.get('description', '')}"
            )

            return NewsItem(
                id=hashlib.md5(f"{item.get('url', '')}{item.get('publishedAt', '')}".encode()).hexdigest(),
                headline=item.get('title', ''),
                summary=item.get('description'),
                content=item.get('content'),
                source='news_api',
                author=item.get('author'),
                published_at=datetime.fromisoformat(item.get('publishedAt').replace('Z', '+00:00')),
                url=item.get('url'),
                symbols=symbols,
                metadata={
                    'source_name': item.get('source', {}).get('name'),
                    'url_to_image': item.get('urlToImage')
                }
            )

        except Exception as e:
            self.logger.error(f"Error parsing News API item: {e}")
            return None

    def _extract_symbols_from_text(self, text: str) -> List[str]:
        """
        Extract stock symbols from text (simple implementation).

        Args:
            text: Text to search

        Returns:
            List of found symbols
        """
        # Common stock symbols (in production, use a more comprehensive list)
        common_symbols = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'BABA', 'V', 'JPM', 'JNJ', 'WMT', 'PG', 'UNH', 'HD', 'MA', 'DIS',
            'PYPL', 'BAC', 'ADBE', 'CRM', 'CMCSA', 'XOM', 'VZ', 'KO', 'PFE',
            'INTC', 'ABT', 'TMO', 'COST', 'AVGO', 'ACN', 'DHR', 'TXN', 'NKE',
            'LLY', 'QCOM', 'NEE', 'HON', 'UPS', 'LOW', 'IBM', 'SBUX', 'AMD'
        ]

        found_symbols = []
        text_upper = text.upper()

        for symbol in common_symbols:
            if symbol in text_upper:
                found_symbols.append(symbol)

        return found_symbols

    def _publish_news_item(self, news_item: NewsItem) -> None:
        """
        Publish news item to event bus.

        Args:
            news_item: News item to publish
        """
        try:
            message = {
                'id': news_item.id,
                'headline': news_item.headline,
                'summary': news_item.summary,
                'content': news_item.content,
                'source': news_item.source,
                'author': news_item.author,
                'published_at': news_item.published_at.isoformat(),
                'url': news_item.url,
                'symbols': news_item.symbols,
                'metadata': news_item.metadata
            }

            self.event_bus.publish(
                topic=self.news_events_topic,
                message=message,
                event_type='news_item'
            )

            # Also save to document database
            self.database_manager.save_document('news_items', message)

            self.logger.debug(f"Published news item: {news_item.headline[:50]}...")

        except Exception as e:
            self.logger.error(f"Error publishing news item: {e}")

    def stop(self) -> None:
        """Stop the news ingestor."""
        self.running = False
        self.event_bus.close()
        self.logger.info("News ingestor stopped")


def main():
    """Main entry point for the news ingestor service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    ingestor = NewsIngestor()

    try:
        ingestor.start_scheduled_fetching()

    except KeyboardInterrupt:
        logging.info("Shutting down news ingestor...")
    finally:
        ingestor.stop()


if __name__ == "__main__":
    main()

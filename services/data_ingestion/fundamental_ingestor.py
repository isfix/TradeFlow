"""
Fundamental Data Ingestor

Ingests deep company data including quarterly financial statements,
corporate actions, and fundamental metrics with data validation and normalization.
"""

import logging
import time
import requests
import schedule
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import json
import pandas as pd

from ...core.event_bus import get_event_bus, initialize_event_bus
from ...core.database_clients import get_database_manager
from ...configs import load_services_config, get_topic_name


@dataclass
class FinancialStatement:
    """Standard financial statement structure."""
    symbol: str
    period: str  # 'Q1', 'Q2', 'Q3', 'Q4', 'FY'
    fiscal_year: int
    statement_type: str  # 'income', 'balance_sheet', 'cash_flow'
    filing_date: datetime
    period_end_date: datetime
    data: Dict[str, float]
    source: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class CorporateAction:
    """Corporate action data structure."""
    symbol: str
    action_type: str  # 'dividend', 'split', 'merger', 'spinoff'
    announcement_date: datetime
    ex_date: Optional[datetime]
    record_date: Optional[datetime]
    payment_date: Optional[datetime]
    details: Dict[str, Any]
    source: str


class DataValidator:
    """Validates and normalizes fundamental data."""
    
    def __init__(self):
        """Initialize data validator."""
        self.logger = logging.getLogger(f"{__name__}.DataValidator")
    
    def validate_financial_statement(self, statement: FinancialStatement) -> bool:
        """
        Validate financial statement data.
        
        Args:
            statement: Financial statement to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check required fields
            if not statement.symbol or not statement.period or not statement.fiscal_year:
                self.logger.warning("Missing required fields in financial statement")
                return False
            
            # Validate fiscal year
            current_year = datetime.now().year
            if statement.fiscal_year < 1900 or statement.fiscal_year > current_year + 1:
                self.logger.warning(f"Invalid fiscal year: {statement.fiscal_year}")
                return False
            
            # Validate period
            valid_periods = ['Q1', 'Q2', 'Q3', 'Q4', 'FY']
            if statement.period not in valid_periods:
                self.logger.warning(f"Invalid period: {statement.period}")
                return False
            
            # Validate statement type
            valid_types = ['income', 'balance_sheet', 'cash_flow']
            if statement.statement_type not in valid_types:
                self.logger.warning(f"Invalid statement type: {statement.statement_type}")
                return False
            
            # Validate data values
            for key, value in statement.data.items():
                if not isinstance(value, (int, float)) and value is not None:
                    self.logger.warning(f"Invalid data value for {key}: {value}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating financial statement: {e}")
            return False
    
    def normalize_financial_data(self, raw_data: Dict[str, Any], source: str) -> Dict[str, float]:
        """
        Normalize financial data from different sources.
        
        Args:
            raw_data: Raw financial data
            source: Data source name
            
        Returns:
            Normalized financial data
        """
        normalized = {}
        
        try:
            if source == 'openbb':
                # OpenBB normalization
                field_mapping = {
                    'totalRevenue': 'revenue',
                    'grossProfit': 'gross_profit',
                    'operatingIncome': 'operating_income',
                    'netIncome': 'net_income',
                    'totalAssets': 'total_assets',
                    'totalLiabilities': 'total_liabilities',
                    'totalEquity': 'total_equity',
                    'operatingCashFlow': 'operating_cash_flow',
                    'freeCashFlow': 'free_cash_flow'
                }
                
                for source_field, normalized_field in field_mapping.items():
                    if source_field in raw_data:
                        value = raw_data[source_field]
                        normalized[normalized_field] = self._clean_numeric_value(value)
            
            elif source == 'sec_edgar':
                # SEC EDGAR normalization (simplified)
                # In practice, this would be much more complex
                for key, value in raw_data.items():
                    if isinstance(value, (int, float, str)):
                        normalized[key.lower().replace(' ', '_')] = self._clean_numeric_value(value)
            
            return normalized
            
        except Exception as e:
            self.logger.error(f"Error normalizing financial data from {source}: {e}")
            return {}
    
    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """
        Clean and convert numeric values.
        
        Args:
            value: Value to clean
            
        Returns:
            Cleaned float value or None
        """
        if value is None or value == '':
            return None
        
        try:
            if isinstance(value, str):
                # Remove common formatting
                cleaned = value.replace(',', '').replace('$', '').replace('(', '-').replace(')', '')
                if cleaned.lower() in ['n/a', 'na', 'null', 'none', '-']:
                    return None
                return float(cleaned)
            elif isinstance(value, (int, float)):
                return float(value)
            else:
                return None
                
        except (ValueError, TypeError):
            return None


class FundamentalIngestor:
    """
    Fundamental data ingestor with data validation and normalization.
    
    Features:
    - Scheduled batch processing
    - Data quality checks and normalization
    - Multiple data source support
    - Corporate actions tracking
    """
    
    def __init__(self, service_name: str = "fundamental_ingestor"):
        """
        Initialize fundamental data ingestor.
        
        Args:
            service_name: Name of this service
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.event_bus = initialize_event_bus(service_name)
        self.database_manager = get_database_manager()
        self.config = load_services_config()
        self.data_validator = DataValidator()
        
        # Control flags
        self.running = False
        
        # Topic names
        self.fundamental_events_topic = get_topic_name('data_ingestion', 'fundamental_events')
    
    def start_scheduled_processing(self) -> None:
        """Start scheduled fundamental data processing."""
        self.running = True
        
        # Schedule different types of data fetching
        schedule.every().day.at("02:00").do(self._fetch_financial_statements)
        schedule.every().day.at("03:00").do(self._fetch_corporate_actions)
        schedule.every().hour.do(self._fetch_key_metrics)
        
        self.logger.info("Started scheduled fundamental data processing")
        
        # Run scheduler
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def fetch_statements(self, symbols: List[str], statement_type: str = 'all') -> List[FinancialStatement]:
        """
        Fetch financial statements for given symbols.
        
        Args:
            symbols: List of stock symbols
            statement_type: Type of statement ('income', 'balance_sheet', 'cash_flow', 'all')
            
        Returns:
            List of financial statements
        """
        statements = []
        
        for symbol in symbols:
            try:
                if statement_type == 'all':
                    for stmt_type in ['income', 'balance_sheet', 'cash_flow']:
                        stmt_list = self._fetch_statements_for_symbol(symbol, stmt_type)
                        statements.extend(stmt_list)
                else:
                    stmt_list = self._fetch_statements_for_symbol(symbol, statement_type)
                    statements.extend(stmt_list)
                    
                # Add delay to respect rate limits
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error fetching statements for {symbol}: {e}")
        
        return statements
    
    def _fetch_financial_statements(self) -> None:
        """Scheduled task to fetch financial statements."""
        try:
            # Get list of symbols to process (in production, this would come from a watchlist)
            symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']  # Example symbols
            
            self.logger.info(f"Starting financial statements fetch for {len(symbols)} symbols")
            
            statements = self.fetch_statements(symbols)
            
            self.logger.info(f"Fetched {len(statements)} financial statements")
            
        except Exception as e:
            self.logger.error(f"Error in scheduled financial statements fetch: {e}")
    
    def _fetch_corporate_actions(self) -> None:
        """Scheduled task to fetch corporate actions."""
        try:
            # Fetch corporate actions for the last 30 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            actions = self._fetch_corporate_actions_for_period(start_date, end_date)
            
            self.logger.info(f"Fetched {len(actions)} corporate actions")
            
        except Exception as e:
            self.logger.error(f"Error in scheduled corporate actions fetch: {e}")
    
    def _fetch_key_metrics(self) -> None:
        """Scheduled task to fetch key financial metrics."""
        try:
            # Fetch key metrics for major symbols
            symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
            
            for symbol in symbols:
                metrics = self._fetch_key_metrics_for_symbol(symbol)
                if metrics:
                    self._publish_key_metrics(symbol, metrics)
                
                time.sleep(1)  # Rate limiting
            
        except Exception as e:
            self.logger.error(f"Error in scheduled key metrics fetch: {e}")
    
    def _fetch_statements_for_symbol(self, symbol: str, statement_type: str) -> List[FinancialStatement]:
        """
        Fetch financial statements for a specific symbol.
        
        Args:
            symbol: Stock symbol
            statement_type: Type of statement
            
        Returns:
            List of financial statements
        """
        statements = []
        
        try:
            # Try OpenBB first
            openbb_statements = self._fetch_from_openbb(symbol, statement_type)
            statements.extend(openbb_statements)
            
            # Could add other sources here
            
        except Exception as e:
            self.logger.error(f"Error fetching statements for {symbol}: {e}")
        
        return statements

    def _fetch_from_openbb(self, symbol: str, statement_type: str) -> List[FinancialStatement]:
        """
        Fetch financial statements from OpenBB.

        Args:
            symbol: Stock symbol
            statement_type: Type of statement

        Returns:
            List of financial statements
        """
        statements = []

        try:
            config = self.config['fundamental_data']['openbb']

            # Map statement types to OpenBB endpoints
            endpoint_mapping = {
                'income': 'income',
                'balance_sheet': 'balance',
                'cash_flow': 'cash'
            }

            endpoint = endpoint_mapping.get(statement_type)
            if not endpoint:
                return statements

            url = f"{config['base_url']}/stocks/fa/{endpoint}"
            headers = {'X-API-KEY': config['api_key']}
            params = {'symbol': symbol, 'limit': 4}  # Last 4 quarters

            response = requests.get(url, headers=headers, params=params, timeout=config.get('timeout', 30))

            if response.status_code == 200:
                data = response.json()

                for period_data in data.get('data', []):
                    # Extract period information
                    period = self._extract_period(period_data)
                    fiscal_year = self._extract_fiscal_year(period_data)

                    if period and fiscal_year:
                        # Normalize the data
                        normalized_data = self.data_validator.normalize_financial_data(period_data, 'openbb')

                        statement = FinancialStatement(
                            symbol=symbol,
                            period=period,
                            fiscal_year=fiscal_year,
                            statement_type=statement_type,
                            filing_date=datetime.now(timezone.utc),  # Approximate
                            period_end_date=self._extract_period_end_date(period_data),
                            data=normalized_data,
                            source='openbb',
                            metadata={'raw_data_keys': list(period_data.keys())}
                        )

                        if self.data_validator.validate_financial_statement(statement):
                            statements.append(statement)
                            self._publish_financial_statement(statement)
            else:
                self.logger.error(f"OpenBB API error for {symbol}: {response.status_code}")

        except Exception as e:
            self.logger.error(f"Error fetching from OpenBB for {symbol}: {e}")

        return statements

    def _extract_period(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract period from financial data."""
        # This would depend on the data source format
        # Simplified implementation
        if 'period' in data:
            return data['period']
        elif 'quarter' in data:
            return f"Q{data['quarter']}"
        return 'FY'  # Default to full year

    def _extract_fiscal_year(self, data: Dict[str, Any]) -> Optional[int]:
        """Extract fiscal year from financial data."""
        if 'fiscalYear' in data:
            return int(data['fiscalYear'])
        elif 'year' in data:
            return int(data['year'])
        return datetime.now().year  # Default to current year

    def _extract_period_end_date(self, data: Dict[str, Any]) -> datetime:
        """Extract period end date from financial data."""
        if 'periodEndDate' in data:
            return datetime.fromisoformat(data['periodEndDate'])
        elif 'date' in data:
            return datetime.fromisoformat(data['date'])
        return datetime.now(timezone.utc)  # Default to now

    def _publish_financial_statement(self, statement: FinancialStatement) -> None:
        """
        Publish financial statement to event bus.

        Args:
            statement: Financial statement to publish
        """
        try:
            message = {
                'symbol': statement.symbol,
                'period': statement.period,
                'fiscal_year': statement.fiscal_year,
                'statement_type': statement.statement_type,
                'filing_date': statement.filing_date.isoformat(),
                'period_end_date': statement.period_end_date.isoformat(),
                'data': statement.data,
                'source': statement.source,
                'metadata': statement.metadata
            }

            self.event_bus.publish(
                topic=self.fundamental_events_topic,
                message=message,
                event_type='financial_statement'
            )

            # Save to document database
            self.database_manager.save_document('financial_statements', message)

            self.logger.debug(f"Published financial statement for {statement.symbol} {statement.period} {statement.fiscal_year}")

        except Exception as e:
            self.logger.error(f"Error publishing financial statement: {e}")

    def _publish_key_metrics(self, symbol: str, metrics: Dict[str, Any]) -> None:
        """
        Publish key metrics to event bus.

        Args:
            symbol: Stock symbol
            metrics: Key metrics dictionary
        """
        try:
            message = {
                'symbol': symbol,
                'metrics': metrics,
                'timestamp': datetime.utcnow().isoformat()
            }

            self.event_bus.publish(
                topic=self.fundamental_events_topic,
                message=message,
                event_type='key_metrics'
            )

            self.logger.debug(f"Published key metrics for {symbol}")

        except Exception as e:
            self.logger.error(f"Error publishing key metrics: {e}")

    def stop(self) -> None:
        """Stop the fundamental ingestor."""
        self.running = False
        self.event_bus.close()
        self.logger.info("Fundamental ingestor stopped")


def main():
    """Main entry point for the fundamental ingestor service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    ingestor = FundamentalIngestor()

    try:
        ingestor.start_scheduled_processing()

    except KeyboardInterrupt:
        logging.info("Shutting down fundamental ingestor...")
    finally:
        ingestor.stop()


if __name__ == "__main__":
    main()

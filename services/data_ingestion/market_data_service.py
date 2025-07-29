#!/usr/bin/env python3
"""
Market Data Service Entry Point

Independent microservice for real-time market data ingestion.
Runs as a standalone service with its own lifecycle management.
"""

import asyncio
import logging
import signal
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from trading_platform.services.data_ingestion.market_data_ingestor import MarketDataIngestor
from trading_platform.core.event_bus import initialize_event_bus
from trading_platform.core.database_clients import get_database_manager
from trading_platform.configs import load_services_config


class MarketDataService:
    """
    Market Data Service - Standalone microservice for market data ingestion.
    
    This service runs independently and communicates with other services
    through the event bus (Kafka/Redis).
    """
    
    def __init__(self):
        """Initialize the market data service."""
        self.service_name = "market_data_service"
        self.logger = logging.getLogger(self.service_name)
        
        # Service state
        self.running = False
        self.shutdown_requested = False
        
        # Initialize components
        try:
            self.config = load_services_config()
            self.event_bus = initialize_event_bus(self.service_name)
            self.database_manager = get_database_manager()
            self.ingestor = MarketDataIngestor(service_name=self.service_name)
            
            self.logger.info(f"{self.service_name} initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize {self.service_name}: {e}")
            raise
    
    async def start(self):
        """Start the market data service."""
        try:
            self.logger.info(f"Starting {self.service_name}...")
            
            # Perform health checks
            await self._health_check()
            
            # Start the ingestor
            self.running = True
            
            # Setup signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            # Start data ingestion in background task
            ingestion_task = asyncio.create_task(self._run_ingestion())
            
            # Start health monitoring
            health_task = asyncio.create_task(self._health_monitor())
            
            self.logger.info(f"{self.service_name} started successfully")
            
            # Wait for shutdown signal
            await asyncio.gather(ingestion_task, health_task, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"Error starting {self.service_name}: {e}")
            raise
        finally:
            await self.stop()
    
    async def _run_ingestion(self):
        """Run the market data ingestion loop."""
        try:
            while self.running and not self.shutdown_requested:
                try:
                    # Start scheduled fetching (this will run in the background)
                    self.ingestor.start_scheduled_fetching()
                    
                    # Keep the service alive
                    while self.running and not self.shutdown_requested:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    self.logger.error(f"Error in ingestion loop: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                    
        except asyncio.CancelledError:
            self.logger.info("Ingestion task cancelled")
        except Exception as e:
            self.logger.error(f"Fatal error in ingestion: {e}")
            self.shutdown_requested = True
    
    async def _health_monitor(self):
        """Monitor service health and perform periodic checks."""
        try:
            while self.running and not self.shutdown_requested:
                try:
                    # Perform health checks every 30 seconds
                    await asyncio.sleep(30)
                    await self._health_check()
                    
                except Exception as e:
                    self.logger.warning(f"Health check failed: {e}")
                    
        except asyncio.CancelledError:
            self.logger.info("Health monitor task cancelled")
    
    async def _health_check(self):
        """Perform health checks on dependencies."""
        try:
            # Check database connectivity
            db_health = self.database_manager.health_check_all()
            
            # Check event bus connectivity
            event_bus_healthy = True
            try:
                # Simple connectivity test
                self.event_bus.publish(
                    topic="health_check",
                    message={"service": self.service_name, "status": "healthy"},
                    event_type="health_check"
                )
            except Exception as e:
                self.logger.warning(f"Event bus health check failed: {e}")
                event_bus_healthy = False
            
            # Log health status
            if all(db_health.values()) and event_bus_healthy:
                self.logger.debug("Health check passed")
            else:
                self.logger.warning(f"Health check issues - DB: {db_health}, EventBus: {event_bus_healthy}")
                
        except Exception as e:
            self.logger.error(f"Health check error: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
        self.running = False
    
    async def stop(self):
        """Stop the market data service gracefully."""
        try:
            self.logger.info(f"Stopping {self.service_name}...")
            self.running = False
            
            # Stop the ingestor
            if hasattr(self.ingestor, 'stop'):
                self.ingestor.stop()
            
            # Close event bus connection
            if hasattr(self.event_bus, 'close'):
                self.event_bus.close()
            
            self.logger.info(f"{self.service_name} stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping {self.service_name}: {e}")


def setup_logging():
    """Setup logging for the service."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "market_data_service.log")
        ]
    )


async def main():
    """Main entry point for the market data service."""
    setup_logging()
    
    logger = logging.getLogger("market_data_service.main")
    logger.info("Starting Market Data Service...")
    
    service = None
    try:
        service = MarketDataService()
        await service.start()
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if service:
            await service.stop()
        logger.info("Market Data Service shutdown complete")


if __name__ == "__main__":
    # Set environment variables for service identification
    os.environ.setdefault("SERVICE_NAME", "market_data_service")
    os.environ.setdefault("SERVICE_VERSION", "1.0.0")
    
    # Run the service
    asyncio.run(main())

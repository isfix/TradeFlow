#!/usr/bin/env python3
"""
News Service Entry Point

Independent microservice for financial news ingestion and processing.
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

from trading_platform.services.data_ingestion.news_ingestor import NewsIngestor
from trading_platform.core.event_bus import initialize_event_bus
from trading_platform.core.database_clients import get_database_manager
from trading_platform.configs import load_services_config


class NewsService:
    """News Service - Standalone microservice for news ingestion."""
    
    def __init__(self):
        """Initialize the news service."""
        self.service_name = "news_service"
        self.logger = logging.getLogger(self.service_name)
        
        # Service state
        self.running = False
        self.shutdown_requested = False
        
        # Initialize components
        try:
            self.config = load_services_config()
            self.event_bus = initialize_event_bus(self.service_name)
            self.database_manager = get_database_manager()
            self.ingestor = NewsIngestor(service_name=self.service_name)
            
            self.logger.info(f"{self.service_name} initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize {self.service_name}: {e}")
            raise
    
    async def start(self):
        """Start the news service."""
        try:
            self.logger.info(f"Starting {self.service_name}...")
            
            # Perform health checks
            await self._health_check()
            
            # Start the service
            self.running = True
            
            # Setup signal handlers
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            # Start news ingestion
            ingestion_task = asyncio.create_task(self._run_ingestion())
            health_task = asyncio.create_task(self._health_monitor())
            
            self.logger.info(f"{self.service_name} started successfully")
            
            # Wait for shutdown
            await asyncio.gather(ingestion_task, health_task, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"Error starting {self.service_name}: {e}")
            raise
        finally:
            await self.stop()
    
    async def _run_ingestion(self):
        """Run the news ingestion loop."""
        try:
            while self.running and not self.shutdown_requested:
                try:
                    # Start scheduled fetching
                    self.ingestor.start_scheduled_fetching()
                    
                    # Keep service alive
                    while self.running and not self.shutdown_requested:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    self.logger.error(f"Error in news ingestion: {e}")
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            self.logger.info("News ingestion task cancelled")
    
    async def _health_monitor(self):
        """Monitor service health."""
        try:
            while self.running and not self.shutdown_requested:
                await asyncio.sleep(30)
                await self._health_check()
        except asyncio.CancelledError:
            self.logger.info("Health monitor cancelled")
    
    async def _health_check(self):
        """Perform health checks."""
        try:
            db_health = self.database_manager.health_check_all()
            if all(db_health.values()):
                self.logger.debug("Health check passed")
            else:
                self.logger.warning(f"Health check issues: {db_health}")
        except Exception as e:
            self.logger.error(f"Health check error: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown_requested = True
        self.running = False
    
    async def stop(self):
        """Stop the service gracefully."""
        try:
            self.logger.info(f"Stopping {self.service_name}...")
            self.running = False
            
            if hasattr(self.ingestor, 'stop'):
                self.ingestor.stop()
            
            if hasattr(self.event_bus, 'close'):
                self.event_bus.close()
            
            self.logger.info(f"{self.service_name} stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping {self.service_name}: {e}")


def setup_logging():
    """Setup logging for the service."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "news_service.log")
        ]
    )


async def main():
    """Main entry point."""
    setup_logging()
    
    logger = logging.getLogger("news_service.main")
    logger.info("Starting News Service...")
    
    service = None
    try:
        service = NewsService()
        await service.start()
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if service:
            await service.stop()
        logger.info("News Service shutdown complete")


if __name__ == "__main__":
    os.environ.setdefault("SERVICE_NAME", "news_service")
    os.environ.setdefault("SERVICE_VERSION", "1.0.0")
    asyncio.run(main())

#!/usr/bin/env python3
"""
Signal Aggregation Service Entry Point

Independent microservice for LLM-powered signal aggregation and synthesis.
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

from trading_platform.services.strategy.llm_signal_aggregator import LLMSignalAggregator
from trading_platform.core.event_bus import initialize_event_bus
from trading_platform.core.database_clients import get_database_manager
from trading_platform.configs import load_services_config


class SignalAggregationService:
    """Signal Aggregation Service - Standalone microservice for signal synthesis."""
    
    def __init__(self):
        """Initialize the signal aggregation service."""
        self.service_name = "signal_aggregation_service"
        self.logger = logging.getLogger(self.service_name)
        
        # Service state
        self.running = False
        self.shutdown_requested = False
        
        # Initialize components
        try:
            self.config = load_services_config()
            self.event_bus = initialize_event_bus(self.service_name)
            self.database_manager = get_database_manager()
            self.aggregator = LLMSignalAggregator(service_name=self.service_name)
            
            self.logger.info(f"{self.service_name} initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize {self.service_name}: {e}")
            raise
    
    async def start(self):
        """Start the signal aggregation service."""
        try:
            self.logger.info(f"Starting {self.service_name}...")
            
            await self._health_check()
            self.running = True
            
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            # Start processing task
            processing_task = asyncio.create_task(self._run_processing())
            health_task = asyncio.create_task(self._health_monitor())
            
            self.logger.info(f"{self.service_name} started successfully")
            
            await asyncio.gather(processing_task, health_task, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"Error starting {self.service_name}: {e}")
            raise
        finally:
            await self.stop()
    
    async def _run_processing(self):
        """Run the signal aggregation processing loop."""
        try:
            while self.running and not self.shutdown_requested:
                try:
                    # The signal aggregator listens to multiple data streams
                    # and aggregates them using LLM analysis
                    while self.running and not self.shutdown_requested:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    self.logger.error(f"Error in signal aggregation: {e}")
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            self.logger.info("Signal aggregation task cancelled")
    
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
            
            # Check aggregation status
            aggregation_status = self.aggregator.get_aggregation_status()
            
            if all(db_health.values()):
                active_contexts = aggregation_status.get('active_contexts', 0)
                self.logger.debug(f"Health check passed - Active contexts: {active_contexts}")
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
            
            if hasattr(self.aggregator, 'stop'):
                self.aggregator.stop()
            
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
            logging.FileHandler(log_dir / "signal_aggregation_service.log")
        ]
    )


async def main():
    """Main entry point."""
    setup_logging()
    
    logger = logging.getLogger("signal_aggregation_service.main")
    logger.info("Starting Signal Aggregation Service...")
    
    service = None
    try:
        service = SignalAggregationService()
        await service.start()
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if service:
            await service.stop()
        logger.info("Signal Aggregation Service shutdown complete")


if __name__ == "__main__":
    os.environ.setdefault("SERVICE_NAME", "signal_aggregation_service")
    os.environ.setdefault("SERVICE_VERSION", "1.0.0")
    asyncio.run(main())

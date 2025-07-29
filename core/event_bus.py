"""
Event Bus Implementation

Provides a standardized interface for inter-service communication
using message brokers (Kafka/Redis). Handles serialization, error handling,
and connection management.
"""

import json
import logging
import time
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional, List, Union
from dataclasses import dataclass
from datetime import datetime
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError as FutureTimeoutError

# Import broker-specific libraries
try:
    from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from ..configs import load_services_config, load_kafka_topics


@dataclass
class EventMessage:
    """Standard event message format."""
    id: str
    timestamp: datetime
    topic: str
    event_type: str
    data: Dict[str, Any]
    source_service: str
    correlation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class RequestMessage:
    """Request message for request-response communication."""
    request_id: str
    request_type: str
    source_service: str
    target_service: str
    timestamp: datetime
    data: Dict[str, Any]
    timeout_seconds: int = 30
    correlation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ResponseMessage:
    """Response message for request-response communication."""
    request_id: str
    response_type: str
    source_service: str
    target_service: str
    timestamp: datetime
    data: Dict[str, Any]
    success: bool = True
    error_message: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class EventBusInterface(ABC):
    """Abstract interface for event bus implementations."""
    
    @abstractmethod
    def publish(self, topic: str, message: Dict[str, Any], 
                event_type: str = "default", correlation_id: Optional[str] = None) -> bool:
        """Publish a message to a topic."""
        pass
    
    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[EventMessage], None],
                 consumer_group: Optional[str] = None) -> None:
        """Subscribe to a topic with a callback function."""
        pass
    
    @abstractmethod
    def request(self, target_service: str, request_type: str, data: Dict[str, Any],
                timeout_seconds: int = 30, correlation_id: Optional[str] = None) -> Dict[str, Any]:
        """Send a request and wait for response."""
        pass

    @abstractmethod
    def register_request_handler(self, request_type: str,
                                handler: Callable[[RequestMessage], ResponseMessage]) -> bool:
        """Register a handler for incoming requests."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close all connections and cleanup resources."""
        pass


class KafkaEventBus(EventBusInterface):
    """Kafka-based event bus implementation."""
    
    def __init__(self, service_name: str, config: Dict[str, Any]):
        """
        Initialize Kafka event bus.
        
        Args:
            service_name: Name of the service using this event bus
            config: Kafka configuration from services.yaml
        """
        if not KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka is required for Kafka event bus")
        
        self.service_name = service_name
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{service_name}")

        # Request-response tracking
        self.pending_requests: Dict[str, Future] = {}
        self.request_handlers: Dict[str, Callable[[RequestMessage], ResponseMessage]] = {}
        self.request_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix=f"{service_name}-rpc")
        
        # Producer configuration
        producer_config = {
            'bootstrap.servers': config['bootstrap_servers'],
            'client.id': f"{service_name}-producer",
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
            'compression.type': 'snappy',
            'batch.size': 16384,
            'linger.ms': 10,
        }
        
        # Add security configuration if provided
        if config.get('security_protocol') != 'PLAINTEXT':
            producer_config.update({
                'security.protocol': config.get('security_protocol'),
                'sasl.mechanism': config.get('sasl_mechanism'),
                'sasl.username': config.get('sasl_username'),
                'sasl.password': config.get('sasl_password'),
            })
        
        self.producer = Producer(producer_config)
        self.consumers: Dict[str, Consumer] = {}
        self.consumer_threads: Dict[str, threading.Thread] = {}
        self.running = True
    
    def publish(self, topic: str, message: Dict[str, Any], 
                event_type: str = "default", correlation_id: Optional[str] = None) -> bool:
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Topic name
            message: Message data
            event_type: Type of event
            correlation_id: Optional correlation ID for tracing
            
        Returns:
            True if message was successfully queued for sending
        """
        try:
            event_message = EventMessage(
                id=str(uuid.uuid4()),
                timestamp=datetime.utcnow(),
                topic=topic,
                event_type=event_type,
                data=message,
                source_service=self.service_name,
                correlation_id=correlation_id
            )
            
            # Serialize message
            serialized_message = json.dumps({
                'id': event_message.id,
                'timestamp': event_message.timestamp.isoformat(),
                'topic': event_message.topic,
                'event_type': event_message.event_type,
                'data': event_message.data,
                'source_service': event_message.source_service,
                'correlation_id': event_message.correlation_id,
                'metadata': event_message.metadata
            })
            
            # Publish to Kafka
            self.producer.produce(
                topic=topic,
                value=serialized_message,
                key=event_message.id,
                callback=self._delivery_callback
            )
            
            # Trigger delivery
            self.producer.poll(0)
            
            self.logger.debug(f"Published message {event_message.id} to topic {topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish message to topic {topic}: {e}")
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def subscribe(self, topic: str, callback: Callable[[EventMessage], None],
                 consumer_group: Optional[str] = None) -> None:
        """
        Subscribe to a Kafka topic.
        
        Args:
            topic: Topic name
            callback: Function to call when message is received
            consumer_group: Consumer group name
        """
        if consumer_group is None:
            consumer_group = f"{self.service_name}-group"
        
        consumer_config = {
            'bootstrap.servers': self.config['bootstrap_servers'],
            'group.id': consumer_group,
            'client.id': f"{self.service_name}-consumer-{topic}",
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'max.poll.interval.ms': 300000,
        }
        
        # Add security configuration if provided
        if self.config.get('security_protocol') != 'PLAINTEXT':
            consumer_config.update({
                'security.protocol': self.config.get('security_protocol'),
                'sasl.mechanism': self.config.get('sasl_mechanism'),
                'sasl.username': self.config.get('sasl_username'),
                'sasl.password': self.config.get('sasl_password'),
            })
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        self.consumers[topic] = consumer
        
        # Start consumer thread
        consumer_thread = threading.Thread(
            target=self._consume_messages,
            args=(topic, consumer, callback),
            daemon=True
        )
        consumer_thread.start()
        self.consumer_threads[topic] = consumer_thread
        
        self.logger.info(f"Subscribed to topic {topic} with consumer group {consumer_group}")
    
    def _consume_messages(self, topic: str, consumer: Consumer, 
                         callback: Callable[[EventMessage], None]) -> None:
        """Consumer thread function."""
        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize message
                    message_data = json.loads(msg.value().decode('utf-8'))
                    
                    event_message = EventMessage(
                        id=message_data['id'],
                        timestamp=datetime.fromisoformat(message_data['timestamp']),
                        topic=message_data['topic'],
                        event_type=message_data['event_type'],
                        data=message_data['data'],
                        source_service=message_data['source_service'],
                        correlation_id=message_data.get('correlation_id'),
                        metadata=message_data.get('metadata')
                    )
                    
                    # Call the callback function
                    callback(event_message)
                    
                    self.logger.debug(f"Processed message {event_message.id} from topic {topic}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing message from topic {topic}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Consumer thread error for topic {topic}: {e}")
        finally:
            consumer.close()

    def request(self, target_service: str, request_type: str, data: Dict[str, Any],
                timeout_seconds: int = 30, correlation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Send a request and wait for response.

        Args:
            target_service: Target service name
            request_type: Type of request
            data: Request data
            timeout_seconds: Request timeout
            correlation_id: Optional correlation ID

        Returns:
            Response data

        Raises:
            TimeoutError: If request times out
            RuntimeError: If request fails
        """
        request_id = str(uuid.uuid4())

        request_message = RequestMessage(
            request_id=request_id,
            request_type=request_type,
            source_service=self.service_name,
            target_service=target_service,
            timestamp=datetime.utcnow(),
            data=data,
            timeout_seconds=timeout_seconds,
            correlation_id=correlation_id
        )

        # Create future for response
        response_future = Future()

        with self.request_lock:
            self.pending_requests[request_id] = response_future

        try:
            # Publish request to target service's request topic
            request_topic = f"{target_service}_requests"

            serialized_request = json.dumps({
                'request_id': request_message.request_id,
                'request_type': request_message.request_type,
                'source_service': request_message.source_service,
                'target_service': request_message.target_service,
                'timestamp': request_message.timestamp.isoformat(),
                'data': request_message.data,
                'timeout_seconds': request_message.timeout_seconds,
                'correlation_id': request_message.correlation_id,
                'metadata': request_message.metadata
            })

            self.producer.produce(
                request_topic,
                value=serialized_request,
                key=request_id
            )
            self.producer.flush()

            # Wait for response
            try:
                response_data = response_future.result(timeout=timeout_seconds)
                return response_data

            except FutureTimeoutError:
                raise TimeoutError(f"Request {request_id} to {target_service} timed out after {timeout_seconds}s")

        finally:
            # Clean up pending request
            with self.request_lock:
                self.pending_requests.pop(request_id, None)

    def register_request_handler(self, request_type: str,
                                handler: Callable[[RequestMessage], ResponseMessage]) -> bool:
        """
        Register a handler for incoming requests.

        Args:
            request_type: Type of request to handle
            handler: Handler function

        Returns:
            True if handler was registered successfully
        """
        try:
            self.request_handlers[request_type] = handler

            # Subscribe to our service's request topic if not already subscribed
            request_topic = f"{self.service_name}_requests"
            if request_topic not in self.consumers:
                self.subscribe(request_topic, self._handle_request_message, f"{self.service_name}_rpc")

            # Subscribe to our service's response topic if not already subscribed
            response_topic = f"{self.service_name}_responses"
            if response_topic not in self.consumers:
                self.subscribe(response_topic, self._handle_response_message, f"{self.service_name}_rpc_responses")

            self.logger.info(f"Registered request handler for {request_type}")
            return True

        except Exception as e:
            self.logger.error(f"Error registering request handler: {e}")
            return False

    def _handle_request_message(self, event_message: EventMessage) -> None:
        """Handle incoming request messages."""
        try:
            # Parse request message
            request_data = event_message.data

            request_message = RequestMessage(
                request_id=request_data['request_id'],
                request_type=request_data['request_type'],
                source_service=request_data['source_service'],
                target_service=request_data['target_service'],
                timestamp=datetime.fromisoformat(request_data['timestamp']),
                data=request_data['data'],
                timeout_seconds=request_data.get('timeout_seconds', 30),
                correlation_id=request_data.get('correlation_id'),
                metadata=request_data.get('metadata')
            )

            # Find handler
            handler = self.request_handlers.get(request_message.request_type)
            if not handler:
                # Send error response
                error_response = ResponseMessage(
                    request_id=request_message.request_id,
                    response_type=f"{request_message.request_type}_response",
                    source_service=self.service_name,
                    target_service=request_message.source_service,
                    timestamp=datetime.utcnow(),
                    data={},
                    success=False,
                    error_message=f"No handler for request type: {request_message.request_type}",
                    correlation_id=request_message.correlation_id
                )
                self._send_response(error_response)
                return

            # Execute handler in thread pool
            def execute_handler():
                try:
                    response = handler(request_message)
                    self._send_response(response)
                except Exception as e:
                    error_response = ResponseMessage(
                        request_id=request_message.request_id,
                        response_type=f"{request_message.request_type}_response",
                        source_service=self.service_name,
                        target_service=request_message.source_service,
                        timestamp=datetime.utcnow(),
                        data={},
                        success=False,
                        error_message=str(e),
                        correlation_id=request_message.correlation_id
                    )
                    self._send_response(error_response)

            self.executor.submit(execute_handler)

        except Exception as e:
            self.logger.error(f"Error handling request message: {e}")

    def _send_response(self, response: ResponseMessage) -> None:
        """Send response message."""
        try:
            response_topic = f"{response.target_service}_responses"

            serialized_response = json.dumps({
                'request_id': response.request_id,
                'response_type': response.response_type,
                'source_service': response.source_service,
                'target_service': response.target_service,
                'timestamp': response.timestamp.isoformat(),
                'data': response.data,
                'success': response.success,
                'error_message': response.error_message,
                'correlation_id': response.correlation_id,
                'metadata': response.metadata
            })

            self.producer.produce(
                response_topic,
                value=serialized_response,
                key=response.request_id
            )
            self.producer.flush()

        except Exception as e:
            self.logger.error(f"Error sending response: {e}")

    def _handle_response_message(self, event_message: EventMessage) -> None:
        """Handle incoming response messages."""
        try:
            response_data = event_message.data
            request_id = response_data['request_id']

            with self.request_lock:
                future = self.pending_requests.get(request_id)

            if future and not future.done():
                if response_data['success']:
                    future.set_result(response_data['data'])
                else:
                    future.set_exception(RuntimeError(response_data.get('error_message', 'Request failed')))

        except Exception as e:
            self.logger.error(f"Error handling response message: {e}")

    def close(self) -> None:
        """Close all connections and cleanup resources."""
        self.running = False

        # Wait for consumer threads to finish
        for thread in self.consumer_threads.values():
            thread.join(timeout=5.0)

        # Shutdown executor
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        # Close consumers
        for consumer in self.consumers.values():
            consumer.close()

        # Flush and close producer
        self.producer.flush(timeout=10.0)

        self.logger.info("Event bus closed")


class EventBusFactory:
    """Factory for creating event bus instances."""
    
    @staticmethod
    def create_event_bus(service_name: str, broker_type: str = "kafka") -> EventBusInterface:
        """
        Create an event bus instance.
        
        Args:
            service_name: Name of the service
            broker_type: Type of message broker ('kafka' or 'redis')
            
        Returns:
            Event bus instance
            
        Raises:
            ValueError: If broker type is not supported
            ImportError: If required dependencies are not available
        """
        config = load_services_config()
        
        if broker_type == "kafka":
            if not KAFKA_AVAILABLE:
                raise ImportError("confluent-kafka is required for Kafka event bus")
            return KafkaEventBus(service_name, config['message_broker']['kafka'])
        else:
            raise ValueError(f"Unsupported broker type: {broker_type}")


# Global event bus instance (initialized by services)
_event_bus: Optional[EventBusInterface] = None


def get_event_bus() -> EventBusInterface:
    """Get the global event bus instance."""
    if _event_bus is None:
        raise RuntimeError("Event bus not initialized. Call initialize_event_bus() first.")
    return _event_bus


def initialize_event_bus(service_name: str, broker_type: str = "kafka") -> EventBusInterface:
    """Initialize the global event bus instance."""
    global _event_bus
    _event_bus = EventBusFactory.create_event_bus(service_name, broker_type)
    return _event_bus

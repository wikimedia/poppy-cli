import json
import sys
from queue import Empty
from typing import Dict, List, Optional, Union

from kafka import KafkaConsumer, KafkaProducer
from kombu.connection import Connection
from kombu.simple import SimpleQueue

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


class ConfigDict(TypedDict, total=False):
    BROKER_URL: str
    QUEUE_NAME: str
    CONNECTION_TIMEOUT: int
    DEQUEUE_TIMEOUT: Optional[int]
    RAISE_ON_EMPTY_DEQUEUE: bool
    CONSUMER_GROUP_ID: str
    CONSUMER_AUTOCOMMIT: bool
    CONSUMER_AUTO_OFFSET_RESET: str
    DEQUEUE_EXIT_ON_EMPTY: bool


DEFAULT_CONFIG = ConfigDict(
    CONNECTION_TIMEOUT=5,
    DEQUEUE_TIMEOUT=None,
    RAISE_ON_EMPTY_DEQUEUE=False,
    CONSUMER_AUTOCOMMIT=True,
    CONSUMER_AUTO_OFFSET_RESET="earliest",
    DEQUEUE_EXIT_ON_EMPTY=False,
)


class EmptyQueueException(Exception):
    def __str__(self) -> str:
        return "Queue is empty"


class KombuEngine:
    """
    Kombu backend for Queue implementation

    :param config: Kombu backend config
    """

    def __init__(self, config: ConfigDict) -> None:
        self.config: ConfigDict = config
        self.name: str = config["QUEUE_NAME"]
        self.broker_url: str = config["BROKER_URL"]
        self.conn: Connection = Connection(
            self.broker_url, connect_timeout=self.connection_timeout
        )
        self.conn.ensure_connection(timeout=self.connection_timeout)
        self.queue: SimpleQueue = self.conn.SimpleQueue(
            self.name, serializer=self.serializer
        )

    @property
    def serializer(self) -> str:
        return "json"

    @property
    def connection_timeout(self) -> int:
        return self.config.get(
            "CONNECTION_TIMEOUT", DEFAULT_CONFIG["CONNECTION_TIMEOUT"]
        )

    @property
    def blocking_dequeue_timeout(self) -> Optional[int]:
        return self.config.get("DEQUEUE_TIMEOUT", DEFAULT_CONFIG["DEQUEUE_TIMEOUT"])

    @property
    def raise_on_empty_dequeue(self) -> bool:
        return self.config.get(
            "RAISE_ON_EMPTY_DEQUEUE",
            DEFAULT_CONFIG["RAISE_ON_EMPTY_DEQUEUE"],
        )

    def enqueue(self, message: Dict[str, str]) -> None:
        """Enqueue a message in the queue

        :param message: Dict with key/value information about the message
        """
        self.queue.put(message)

    def dequeue(self) -> str:
        """Dequeue a message from the queue

        :returns: String with message consumed
        """
        try:
            msg = self.queue.get(block=True, timeout=self.blocking_dequeue_timeout)
            msg.ack()
            return msg.body
        except Empty:
            if self.raise_on_empty_dequeue:
                raise EmptyQueueException()
            return "{}"

    def close(self) -> None:
        """Close connections"""
        self.queue.close()
        self.conn.release()


class KafkaEngine:
    """
    Kafka backend for Queue implementation

    :param config: Kombu backend config
    """

    def __init__(self, config: ConfigDict) -> None:
        self.topic: str = config["QUEUE_NAME"]
        self.config: ConfigDict = config
        self.producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=self.serializer,
        )
        self.consumer: KafkaConsumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.servers,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.autocommit,
            group_id=self.group_id,
            consumer_timeout_ms=self.blocking_dequeue_timeout,
        )

    @staticmethod
    def serializer(x: Dict[str, str]) -> bytes:
        """Serializer for kafka messages"""
        return json.dumps(x).encode("utf-8")

    @property
    def servers(self) -> List[str]:
        """Parse broker URL to extract kafka servers"""
        broker_url: str = self.config["BROKER_URL"]
        prefix: str = "kafka://"

        if broker_url.startswith("kafka://"):
            broker_url = broker_url[len(prefix) :]
            return broker_url.split(";")

        raise ValueError("Broker URL is misformatted")

    @property
    def raise_on_empty_dequeue(self) -> bool:
        return self.config.get(
            "RAISE_ON_EMPTY_DEQUEUE",
            DEFAULT_CONFIG["RAISE_ON_EMPTY_DEQUEUE"],
        )

    @property
    def blocking_dequeue_timeout(self) -> Union[float, int]:
        timeout: Optional[int] = self.config.get(
            "DEQUEUE_TIMEOUT", DEFAULT_CONFIG["DEQUEUE_TIMEOUT"]
        )

        if timeout:
            return timeout * 1000
        return float("inf")

    @property
    def group_id(self) -> str:
        """Get kafka consumer group ID"""
        return self.config.get("CONSUMER_GROUP_ID", f"poppy-{self.topic}")

    @property
    def autocommit(self) -> bool:
        """Get config for kafka consumer autocommit"""
        return self.config.get(
            "CONSUMER_AUTOCOMMIT", DEFAULT_CONFIG["CONSUMER_AUTOCOMMIT"]
        )

    @property
    def auto_offset_reset(self) -> str:
        """Get confic for kafka consumer auto offset reset"""
        return self.config.get(
            "CONSUMER_AUTO_OFFSET_RESET",
            DEFAULT_CONFIG["CONSUMER_AUTO_OFFSET_RESET"],
        )

    def enqueue(self, message: Dict[str, str]) -> None:
        """Enqueue a message in the queue

        :param message: Dict with key/value information about the message
        """
        self.producer.send(self.topic, value=message)

    def dequeue(self) -> str:
        """Dequeue a message from the queue

        :returns: A dict with message related key/value information
        """
        try:
            return next(self.consumer).value
        except StopIteration:
            if self.raise_on_empty_dequeue:
                raise EmptyQueueException()
            return "{}"

    def close(self) -> None:
        """Close connections and commit offset"""
        self.consumer.commit()
        self.consumer.close()
        self.producer.close()

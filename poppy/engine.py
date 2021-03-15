import json
import sys
from queue import Empty
from typing import Dict, List

from kafka import KafkaConsumer, KafkaProducer
from kombu.connection import Connection

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


class ConfigDict(TypedDict, total=False):
    BROKER_URL: str
    QUEUE_NAME: str
    CONNECTION_TIMEOUT: int
    BLOCKING_DEQUEUE: bool
    DEQUEUE_TIMEOUT: int
    RAISE_ON_EMPTY_DEQUEUE: bool
    CONSUMER_GROUP_ID: str
    CONSUMER_AUTOCOMMIT: bool
    CONSUMER_AUTO_OFFSET_RESET: bool


class KombuEngine:
    """
    Kombu backend for TaskQueue implementation

    :param config: Kombu backend config
    """

    def __init__(self, config: ConfigDict):
        self.config = config
        self.name = config["QUEUE_NAME"]
        self.broker_url = config["BROKER_URL"]
        self.conn = Connection(self.broker_url, connect_timeout=self.connection_timeout)
        self.queue = self.conn.SimpleQueue(self.name, serializer=self.serializer)

    @property
    def serializer(self):
        return "json"

    @staticmethod
    def get_default_config() -> ConfigDict:
        return {
            "CONNECTION_TIMEOUT": 5,
            "BLOCKING_DEQUEUE": False,
            "DEQUEUE_TIMEOUT": 5,
            "RAISE_ON_EMPTY_DEQUEUE": False,
        }

    @property
    def connection_timeout(self) -> int:
        return self.config.get(
            "CONNECTION_TIMEOUT", self.get_default_config()["CONNECTION_TIMEOUT"]
        )

    @property
    def is_blocking_dequeue(self) -> bool:
        return self.config.get(
            "BLOCKING_DEQUEUE", self.get_default_config()["BLOCKING_DEQUEUE"]
        )

    @property
    def blocking_dequeue_timeout(self) -> int:
        return self.config.get(
            "DEQUEUE_TIMEOUT", self.get_default_config()["DEQUEUE_TIMEOUT"]
        )

    @property
    def raise_on_empty_dequeue(self) -> bool:
        return self.config.get(
            "RAISE_ON_EMPTY_DEQUEUE",
            self.get_default_config()["RAISE_ON_EMPTY_DEQUEUE"],
        )

    def enqueue(self, task: Dict):
        """Enqueue a task in the queue

        :param task: Dict with key/value information about the task
        """
        self.queue.put(task)

    def dequeue(self) -> str:
        """Dequeue a task from the queue

        :returns: A dict with task related key/value information
        """
        try:
            msg = self.queue.get(
                block=self.is_blocking_dequeue, timeout=self.blocking_dequeue_timeout
            )
            return msg.body
        except Empty as e:
            if self.raise_on_empty_dequeue:
                raise e
            return "{}"

    def close(self):
        """Close connections"""
        self.queue.close()
        self.conn.release()


class KafkaEngine:
    """
    Kafka backend for TaskQueue implementation

    :param config: Kombu backend config
    """

    def __init__(self, config: ConfigDict):
        self.topic = config["QUEUE_NAME"]
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=self.serializer,
        )
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.servers,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.autocommit,
            group_id=self.group_id,
            consumer_timeout_ms=self.blocking_dequeue_timeout,
        )

    @staticmethod
    def serializer(x: Dict) -> bytes:
        """Serializer for kafka messages"""
        return json.dumps(x).encode("utf-8")

    @property
    def servers(self) -> List[str]:
        """Parse broker URL to extract kafka servers"""
        broker_url = self.config["BROKER_URL"]
        prefix = "kafka://"

        if broker_url.startswith("kafka://"):
            broker_url = broker_url[len(prefix) :]
            return broker_url.split(";")

        raise ValueError("Broker URL is misformatted")

    @property
    def raise_on_empty_dequeue(self) -> bool:
        return self.config.get(
            "RAISE_ON_EMPTY_DEQUEUE",
            self.get_default_config()["RAISE_ON_EMPTY_DEQUEUE"],
        )

    @property
    def blocking_dequeue_timeout(self) -> int:
        timeout = self.config.get(
            "DEQUEUE_TIMEOUT", self.get_default_config()["DEQUEUE_TIMEOUT"]
        )

        return timeout * 1000

    @property
    def group_id(self) -> str:
        """Get kafka consumer group ID"""
        return self.config.get("CONSUMER_GROUP_ID", f"poppy-{self.topic}")

    @property
    def autocommit(self) -> bool:
        """Get config for kafka consumer autocommit"""
        return self.config.get(
            "CONSUMER_AUTOCOMMIT", self.get_default_config()["CONSUMER_AUTOCOMMIT"]
        )

    @property
    def auto_offset_reset(self) -> bool:
        """Get confic for kafka consumer auto offset reset"""
        return self.config.get(
            "CONSUMER_AUTO_OFFSET_RESET",
            self.get_default_config()["CONSUMER_AUTO_OFFSET_RESET"],
        )

    def enqueue(self, task: Dict):
        """Enqueue a task in the queue

        :param task: Dict with key/value information about the task
        """
        self.producer.send(self.topic, value=task)

    def dequeue(self) -> str:
        """Dequeue a task from the queue

        :returns: A dict with task related key/value information
        """
        try:
            msg = next(self.consumer)
            return msg.value
        except StopIteration as e:
            if self.raise_on_empty_dequeue:
                raise e
            return "{}"

    def close(self):
        """Close connections and commit offset"""
        self.consumer.commit()
        self.consumer.close()
        self.producer.close()

    @staticmethod
    def get_default_config() -> Dict:
        return {
            "CONSUMER_AUTOCOMMIT": True,
            "CONSUMER_AUTO_OFFSET_RESET": "earliest",
            "DEQUEUE_TIMEOUT": 5,
            "RAISE_ON_EMPTY_DEQUEUE": False,
        }

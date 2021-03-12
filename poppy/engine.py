import sys

from queue import Empty

from kombu.connection import Connection

if sys.version_info >= (3, 8):
    from typing import Dict, TypedDict
else:
    from typing import Dict
    from typing_extensions import TypedDict


class ConfigDict(TypedDict, total=False):
    BROKER_URL: str
    QUEUE_NAME: str
    CONNECTION_TIMEOUT: int
    BLOCKING_DEQUEUE: bool
    DEQUEUE_TIMEOUT: int
    RAISE_ON_EMPTY_DEQUEUE: bool


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

    def dequeue(self) -> Dict:
        """Dequeue a task from the queue

        :returns: A dict with task related key/value information
        """
        try:
            msg = self.queue.get(
                block=self.is_blocking_dequeue, timeout=self.blocking_dequeue_timeout
            )
            return msg.payload
        except Empty as e:
            if self.raise_on_empty_dequeue:
                raise e
            return {}

    def close(self):
        """Close connections"""
        self.queue.close()
        self.conn.release()

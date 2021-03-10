from typing import Dict, TypedDict

from kombu.connection import Connection
from kombu.simple import Empty

DEFAULT_SERIALIZER = "json"
DEFAULT_TIMEOUT = 5
DEFAULT_BLOCKING_DEQUEUE = False
DEFAULT_RAISE_ON_EMPTY_DEQUEUE = False


class ConfigDict(TypedDict):
    BROKER_URL: str
    QUEUE_NAME: str
    CONNECTION_TIMEOUT: int
    BLOCKING_DEQUEUE: bool
    DEQUEUE_TIMEOUT: int
    RAISE_ON_EMPTY_DEQUEUE: bool


class TaskQueue:
    """
    Task queue implementation

    :param queue_name: Task queue name
    :param broker_url: Backend broker URL
    """

    def __init__(self, config: ConfigDict):
        self.serializer = DEFAULT_SERIALIZER
        self.config = config
        self.name = config["QUEUE_NAME"]
        self.broker_url = config["BROKER_URL"]
        self.connection_timeout = config["CONNECTION_TIMEOUT"]
        self.conn = Connection(self.broker_url, connect_timeout=self.connection_timeout)
        self.queue = self.conn.SimpleQueue(self.name, serializer=self.serializer)

    @property
    def is_blocking_dequeue(self):
        return self.config["BLOCKING_DEQUEUE"]

    @property
    def blocking_dequeue_timeout(self):
        return self.config["DEQUEUE_TIMEOUT"]

    @property
    def raise_on_empty_dequeue(self):
        return self.config["RAISE_ON_EMPTY_DEQUEUE"]

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

    @staticmethod
    def get_default_config() -> ConfigDict(total=False):
        """Get default TaskQueue config"""
        return {
            "CONNECTION_TIMEOUT": DEFAULT_TIMEOUT,
            "BLOCKING_DEQUEUE": DEFAULT_BLOCKING_DEQUEUE,
            "DEQUEUE_TIMEOUT": DEFAULT_TIMEOUT,
            "RAISE_ON_EMPTY_DEQUEUE": DEFAULT_RAISE_ON_EMPTY_DEQUEUE,
        }

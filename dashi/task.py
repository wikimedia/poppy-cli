from typing import Dict

from kombu.connection import Connection
from kombu.simple import Empty

DEFAULT_SERIALIZER = "json"
DEFAULT_TIMEOUT = 5
DEFAULT_BLOCKING = True


class TaskQueue:
    """
    Task queue implementation

    :param queue_name: Task queue name
    :param broker_url: Backend broker URL
    """

    def __init__(
        self,
        queue_name: str,
        broker_url: str,
        serializer: str = DEFAULT_SERIALIZER,
        timeout: int = DEFAULT_TIMEOUT,
        blocking: bool = DEFAULT_BLOCKING,
    ):
        self.name = queue_name
        self.broker_url = broker_url
        self.serializer = serializer
        self.blocking = blocking
        self.timeout = timeout
        self.conn = Connection(self.broker_url, connect_timeout=timeout)
        self.queue = self.conn.SimpleQueue(self.name, serializer=self.serializer)

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
            return self.queue.get(block=self.blocking, timeout=self.timeout).payload
        except Empty:
            return {}

    def close(self):
        """Close connections"""
        self.queue.close()
        self.conn.release()

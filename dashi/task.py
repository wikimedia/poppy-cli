from typing import Dict

from kombu.connection import Connection

DEFAULT_SERIALIZER = "json"


class TaskQueue:
    """
    Task queue implementation

    :param queue_name: Task queue name
    :param broker_url: Backend broker URL
    """

    def __init__(self, queue_name: str, broker_url: str):
        self.name = queue_name
        self.broker_url = broker_url
        self.serializer = DEFAULT_SERIALIZER
        # TODO: Setup sane connection timeout defaults
        self.conn = Connection(self.broker_url)
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
        return self.queue.get().payload

    def close(self):
        """Close connections"""
        self.queue.close()
        self.conn.release()

from typing import Dict

from poppy.engine import ConfigDict, KombuEngine


class TaskQueue:
    """
    Task queue implementation

    :param config: TaskQueue config
    """

    def __init__(self, config: ConfigDict):
        self.config = config
        self.engine = self._get_engine()

    def _get_engine(self) -> KombuEngine:
        """Get the backend engine based on broker URL"""

        if self.config["BROKER_URL"].startswith("kafka://"):
            raise NotImplementedError()
        return KombuEngine(self.config)

    def enqueue(self, task: Dict):
        self.engine.enqueue(task)

    def dequeue(self) -> Dict:
        return self.engine.dequeue()

    def close(self):
        self.engine.close()

    @staticmethod
    def get_default_config():
        return {**KombuEngine.get_default_config()}

from typing import Dict, Union

from poppy.engine import ConfigDict, KafkaEngine, KombuEngine


class TaskQueue:
    """
    Task queue implementation

    :param config: TaskQueue config
    """

    def __init__(self, config: ConfigDict):
        self.config = config
        self.engine = self._get_engine()

    def _get_engine(self) -> Union[KombuEngine, KafkaEngine]:
        """Get the backend engine based on broker URL"""

        if self.config["BROKER_URL"].startswith("kafka://"):
            return KafkaEngine(self.config)
        return KombuEngine(self.config)

    def enqueue(self, task: Dict):
        self.engine.enqueue(task)

    def dequeue(self) -> str:
        return self.engine.dequeue()

    def close(self):
        self.engine.close()

    @staticmethod
    def get_default_config():
        return {**KombuEngine.get_default_config(), **KafkaEngine.get_default_config()}

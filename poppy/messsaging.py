from typing import Dict, Union

from poppy.engine import DEFAULT_CONFIG as ENGINE_DEFAULT_CONFIG
from poppy.engine import ConfigDict, KafkaEngine, KombuEngine


class Queue:
    """
    Message queue implementation

    :param config: Message queue config
    """

    def __init__(self, config: ConfigDict):
        self.config = config
        self.engine = self._get_engine()

    def _get_engine(self) -> Union[KombuEngine, KafkaEngine]:
        """Get the backend engine based on broker URL"""

        if self.config["BROKER_URL"].startswith("kafka://"):
            return KafkaEngine(self.config)
        return KombuEngine(self.config)

    def enqueue(self, message: Dict) -> None:
        self.engine.enqueue(message)

    def dequeue(self) -> str:
        return self.engine.dequeue()

    def close(self) -> None:
        self.engine.close()

    @staticmethod
    def get_default_config() -> ConfigDict:
        return ENGINE_DEFAULT_CONFIG

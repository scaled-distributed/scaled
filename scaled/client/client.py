from typing import Callable

import cloudpickle

from scaled.system.config import ZMQConfig


class Client:
    def __init__(self, config: ZMQConfig):
        self._config = config

    def register(self, fn: Callable):
        payload = cloudpickle.dumps(fn)
        self.address

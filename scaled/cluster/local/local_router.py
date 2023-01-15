import asyncio
import multiprocessing

from scaled.io.config import ZMQConfig
from scaled.scheduler.router import Router


class LocalRouter(multiprocessing.Process):
    def __init__(self, config: ZMQConfig):
        multiprocessing.Process.__init__(self, name="local_router")
        self._router = Router(config=config)

    def run(self) -> None:
        asyncio.run(self._router.loop())

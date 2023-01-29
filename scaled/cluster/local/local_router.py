import asyncio
import multiprocessing
import threading
from typing import Optional

# import uvloop

from scaled.io.config import ZMQConfig
from scaled.router.router import Router
from scaled.utility.logging.utility import setup_logger


class LocalRouter(multiprocessing.get_context("spawn").Process):
    def __init__(self, address: ZMQConfig, stop_event: threading.Event):
        multiprocessing.Process.__init__(self, name="LocalRouter")
        self._address = address
        self._stop_event = stop_event
        self._router: Optional[Router] = None

    def run(self) -> None:
        # router have its own single process
        setup_logger()
        self._router = Router(address=self._address, stop_event=self._stop_event)

        # uvloop.install()
        asyncio.run(self._router.loop())

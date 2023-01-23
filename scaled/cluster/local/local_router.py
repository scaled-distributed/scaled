import asyncio
import logging
import multiprocessing
import threading
from typing import Optional

from scaled.io.config import ZMQConfig
from scaled.router.router import Router
from scaled.utility.logging import setup_logger


class LocalRouter(multiprocessing.get_context("spawn").Process):
    def __init__(self, address: ZMQConfig, stop_event: threading.Event):
        multiprocessing.Process.__init__(self, name="LocalRouter")
        self._address = address
        self._stop_event = stop_event
        self._router: Optional[Router] = None

    def run(self) -> None:
        setup_logger()
        self._router = Router(address=self._address, stop_event=self._stop_event)
        logging.info("LocalRouter started")
        asyncio.run(self._router.loop())
        logging.info("LocalRouter exited")

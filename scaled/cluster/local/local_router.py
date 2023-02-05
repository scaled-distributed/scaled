import asyncio
import multiprocessing
import threading
from typing import Optional

import uvloop

from scaled.scheduler.worker_manager.vanilla import AllocatorType
from scaled.utility.zmq_config import ZMQConfig
from scaled.scheduler.router import Router
from scaled.utility.logging.utility import setup_logger


class LocalRouter(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: threading.Event,
        allocator_type: AllocatorType,
        worker_timeout_seconds: int,
        function_timeout_seconds: int,
    ):
        multiprocessing.Process.__init__(self, name="LocalRouter")
        self._address = address
        self._stop_event = stop_event
        self._allocator_type = allocator_type
        self._worker_timeout_seconds = worker_timeout_seconds
        self._function_timeout_seconds = function_timeout_seconds
        self._router: Optional[Router] = None

    def run(self) -> None:
        # scheduler have its own single process
        setup_logger()
        self._router = Router(
            address=self._address,
            stop_event=self._stop_event,
            allocator_type=self._allocator_type,
            worker_timeout_seconds=self._worker_timeout_seconds,
            function_timeout_seconds=self._function_timeout_seconds,
        )
        uvloop.install()
        asyncio.run(self._router.loop())

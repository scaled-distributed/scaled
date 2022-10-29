import asyncio

from scaled.scheduler.engine import Engine
from scaled.scheduler.job_manager.simple import SimpleJobManager
from scaled.io.config import ZMQConfig
from scaled.scheduler.binder import Binder
from scaled.scheduler.worker_manager.vanilla import VanillaWorkerManager

WORKER_TIMEOUT_SECONDS = 10


class Scheduler:
    def __init__(self, address: ZMQConfig):
        self._address = address

        self._stop_event = asyncio.Event()
        self._binder = Binder(prefix="S", address=self._address, stop_event=self._stop_event)

        self._job_manager = SimpleJobManager()
        self._worker_scheduler = VanillaWorkerManager(timeout_seconds=WORKER_TIMEOUT_SECONDS)

        self._engine = Engine(
            binder=self._binder, job_manager=self._job_manager, worker_scheduler=self._worker_scheduler
        )

        self._binder.register(self._engine.on_receive_message)

    def run(self):
        asyncio.gather(self._engine.loop(), self._worker_scheduler.loop())

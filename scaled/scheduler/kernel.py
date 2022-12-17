import asyncio
import logging
from typing import List

from scaled.protocol.python.objects import MessageType
from scaled.protocol.python.message import PROTOCOL
from scaled.scheduler.job_dispatcher.simple import SimpleJobDispatcher
from scaled.io.config import ZMQConfig
from scaled.scheduler.io.zmq_binder import ZMQBinder
from scaled.scheduler.worker_manager.vanilla import VanillaWorkerManager

WORKER_TIMEOUT_SECONDS = 10


class Router:
    def __init__(self, address: ZMQConfig):
        self._address = address

        self._stop_event = asyncio.Event()
        self._binder = ZMQBinder(stop_event=self._stop_event, prefix="S", address=self._address)

        self._job_dispatcher = SimpleJobDispatcher(stop_event=self._stop_event)
        self._worker_manager = VanillaWorkerManager(stop_event=self._stop_event, timeout_seconds=WORKER_TIMEOUT_SECONDS)

        self._binder.register(self.on_receive_message)
        self._job_dispatcher.hook(self._binder, self._worker_manager)
        self._worker_manager.hook(self._binder, self._job_dispatcher)

    async def on_receive_message(self, source: bytes, message_type: bytes, data: List[bytes]):
        obj = PROTOCOL[message_type].deserialize(*data)
        match message_type:
            case MessageType.Heartbeat.value:
                await self._worker_manager.on_heartbeat(source, obj)
            case MessageType.Task.value:
                await self._job_dispatcher.on_job(source, obj)
            case MessageType.TaskResult.value:
                await self._worker_manager.on_task_done(obj)
                await self._job_dispatcher.on_job_done(obj)
            case _:
                logging.error(f"unknown {message_type} from {source=}: {data}")

    async def loop(self):
        await asyncio.gather(self._binder.loop(), self._job_dispatcher.loop(), self._worker_manager.loop())

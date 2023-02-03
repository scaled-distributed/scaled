import asyncio
import threading
import logging

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import MessageVariant, MonitorRequest, MonitorResponse
from scaled.protocol.python.objects import MessageType
from scaled.scheduler.task_manager.simple import SimpleTaskManager
from scaled.scheduler.worker_manager.simple import AllocatorType, SimpleWorkerManager

PREFIX = "Router:"


class Router:
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: threading.Event,
        worker_timeout_seconds: int,
        allocator_type: AllocatorType,
    ):
        self._address = address
        self._stop_event = stop_event

        self._binder = AsyncBinder(prefix="S", address=self._address)
        self._task_manager = SimpleTaskManager(stop_event=self._stop_event)
        self._worker_manager = SimpleWorkerManager(
            stop_event=self._stop_event, allocator_type=allocator_type, timeout_seconds=worker_timeout_seconds
        )

        self._binder.register(self.on_receive_message)
        self._task_manager.hook(self._binder, self._worker_manager)
        self._worker_manager.hook(self._binder, self._task_manager)

    async def on_receive_message(self, source: bytes, message_type: MessageType, message: MessageVariant):
        match message_type:
            case MessageType.Heartbeat:
                await self._worker_manager.on_heartbeat(source, message)
            case MessageType.MonitorRequest:
                await self.statistics(source, message)
            case MessageType.TaskResult:
                await self._worker_manager.on_task_done(message)
            case MessageType.Task:
                await self._task_manager.on_task_new(source, message)
            case MessageType.TaskCancel:
                await self._task_manager.on_task_cancel(source, message.task_id)
            case _:
                logging.error(f"{PREFIX} unknown {message_type} from {source=}: {message}")

    async def loop(self):
        logging.info("LocalRouter started")
        while not self._stop_event.is_set():
            await asyncio.gather(self._binder.routine(), self._task_manager.routine(), self._worker_manager.routine())
        logging.info("LocalRouter quited")

    async def statistics(self, source: bytes, request: MonitorRequest):
        stats = MonitorResponse(
            {
                "binder": await self._binder.statistics(),
                "task_manager": await self._task_manager.statistics(),
                "worker_manager": await self._worker_manager.statistics(),
            }
        )
        await self._binder.send(source, MessageType.MonitorResponse, stats)

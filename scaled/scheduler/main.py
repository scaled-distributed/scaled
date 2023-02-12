import asyncio
import threading
import logging

from scaled.scheduler.client_manager.vanilla import VanillaClientManager
from scaled.scheduler.function_manager.vanilla import VanillaFunctionManager
from scaled.utility.zmq_config import ZMQConfig
from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import MessageType, MessageVariant, MonitorRequest, MonitorResponse
from scaled.scheduler.task_manager.vanilla import VanillaTaskManager
from scaled.scheduler.worker_manager.vanilla import VanillaWorkerManager

PREFIX = "Scheduler:"


class Scheduler:
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: threading.Event,
        per_worker_queue_size: int,
        worker_timeout_seconds: int,
        function_retention_seconds: int,
    ):
        self._address = address
        self._stop_event = stop_event

        self._binder = AsyncBinder(prefix="S", address=self._address)
        self._client_manager = VanillaClientManager()
        self._function_manager = VanillaFunctionManager(function_retention_seconds=function_retention_seconds)
        self._task_manager = VanillaTaskManager(stop_event=self._stop_event)
        self._worker_manager = VanillaWorkerManager(
            stop_event=self._stop_event,
            per_worker_queue_size=per_worker_queue_size,
            timeout_seconds=worker_timeout_seconds,
        )

        self._binder.register(self.on_receive_message)
        self._function_manager.hook(self._binder)
        self._task_manager.hook(self._binder, self._function_manager, self._worker_manager)
        self._worker_manager.hook(self._binder, self._task_manager)

    async def on_receive_message(self, source: bytes, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.Heartbeat:
            await self._worker_manager.on_heartbeat(source, message)
            return

        if message_type == MessageType.MonitorRequest:
            await self.statistics(source, message)
            return

        if message_type == MessageType.Task:
            await self._task_manager.on_task_new(source, message)
            return

        if message_type == MessageType.TaskCancel:
            await self._task_manager.on_task_cancel(source, message.task_id)
            return

        if message_type == MessageType.TaskResult:
            await self._worker_manager.on_task_done(message)
            return

        if message_type == MessageType.FunctionRequest:
            await self._function_manager.on_function(source, message)
            return

        logging.error(f"{PREFIX} unknown {message_type} from {source=}: {message}")

    async def loop(self):
        logging.info("Scheduler started")
        while not self._stop_event.is_set():
            await asyncio.gather(
                self._binder.routine(),
                self._task_manager.routine(),
                self._function_manager.routine(),
                self._worker_manager.routine(),
            )
        logging.info("Scheduler quited")

    async def statistics(self, source: bytes, request: MonitorRequest):
        assert isinstance(request, MonitorRequest)
        stats = MonitorResponse(
            {
                "binder": await self._binder.statistics(),
                "task_manager": await self._task_manager.statistics(),
                "function_manager": await self._function_manager.statistics(),
                "worker_manager": await self._worker_manager.statistics(),
            }
        )
        await self._binder.send(source, MessageType.MonitorResponse, stats)

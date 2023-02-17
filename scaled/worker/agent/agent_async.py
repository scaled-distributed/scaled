import asyncio
import queue
import threading

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, MessageVariant
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.function_cache import FunctionCache
from scaled.worker.agent.heart_beat import WorkerHeartbeat
from scaled.worker.agent.task_queue import TaskQueue


class AgentAsync:
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.asyncio.Context,
        address: ZMQConfig,
        receive_task_queue: queue.Queue,
        send_task_queue: queue.Queue,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
    ):
        self._stop_event = stop_event

        self._connector_external = AsyncConnector(
            prefix="W",
            context=context,
            socket_type=zmq.DEALER,
            address=address,
            bind_or_connect="connect",
            callback=self.on_receive_external,
        )
        self._task_queue = TaskQueue(
            receive_task_queue=receive_task_queue,
            send_task_queue=send_task_queue,
            connector_external=self._connector_external,
        )
        self._function_cache = FunctionCache(
            connector_external=self._connector_external,
            task_queue=self._task_queue,
            function_retention_seconds=function_retention_seconds,
        )
        self._heartbeat = WorkerHeartbeat(
            connector=self._connector_external, heartbeat_interval_seconds=heartbeat_interval_seconds
        )

    @property
    def identity(self):
        return self._connector_external.identity

    async def on_receive_external(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.Task:
            await self._function_cache.on_new_task(message)
            return

        if message_type == MessageType.FunctionResponse:
            await self._function_cache.on_new_function(message)
            return

        raise TypeError(f"Unknown {message_type=}")

    async def on_receive_internal(self, message_type: MessageType, message: MessageVariant):
        await self._connector_external.send(message_type, message)

    async def loop(self):
        while not self._stop_event.is_set():
            await asyncio.gather(
                self._heartbeat.routine(),
                self._connector_external.routine(),
                self._function_cache.routine(),
                self._task_queue.routine(),
            )

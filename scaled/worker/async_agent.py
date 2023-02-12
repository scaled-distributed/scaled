import asyncio
import threading
import time
from collections import defaultdict
from typing import Dict, List

import psutil
import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    Heartbeat,
    MessageType,
    MessageVariant,
    Task,
)
from scaled.utility.zmq_config import ZMQConfig


class AsyncAgent:
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.asyncio.Context,
        address: ZMQConfig,
        address_internal: ZMQConfig,
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
        self._connector_internal = AsyncConnector(
            prefix="A",
            context=context,
            socket_type=zmq.PAIR,
            address=address_internal,
            bind_or_connect="bind",
            callback=self.on_receive_internal,
        )

        self._function_cache = _FunctionCache(
            connector_external=self._connector_external,
            connector_internal=self._connector_internal,
            function_retention_seconds=function_retention_seconds,
        )

        self._heartbeat = _WorkerHeartbeat(
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
                self._connector_internal.routine(),
                self._function_cache.routine(),
            )


class _FunctionCache:
    def __init__(
        self, connector_external: AsyncConnector, connector_internal: AsyncConnector, function_retention_seconds: int
    ):
        self._connector_external = connector_external
        self._connector_internal = connector_internal
        self._function_retention_seconds = function_retention_seconds

        self._cached_functions: Dict[bytes, bytes] = dict()
        self._cached_functions_alive_since: Dict[bytes, float] = dict()
        self._pending_tasks: Dict[bytes, List[Task]] = defaultdict(list)

    async def on_new_task(self, task: Task):
        if task.function_id in self._cached_functions:
            task.function_content = self._cached_functions[task.function_id]
            self._cached_functions_alive_since[task.function_id] = time.time()
            await self._connector_internal.send(MessageType.Task, task)
            return

        self._pending_tasks[task.function_id].append(task)
        await self._connector_external.send(
            MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
        )

    async def on_new_function(self, response: FunctionResponse):
        if response.function_id in self._cached_functions:
            return

        assert response.status == FunctionResponseType.OK
        function_content = response.content

        self._cached_functions[response.function_id] = function_content
        self._cached_functions_alive_since[response.function_id] = time.time()

        for task in self._pending_tasks.pop(response.function_id):
            task.function_content = function_content
            await self._connector_internal.send(MessageType.Task, task)

    async def routine(self):
        now = time.time()
        idle_functions = [
            function_id
            for function_id, alive_since in self._cached_functions_alive_since.items()
            if now - alive_since > self._function_retention_seconds
        ]
        for function_id in idle_functions:
            self._cached_functions_alive_since.pop(function_id)
            self._cached_functions.pop(function_id)


class _WorkerHeartbeat:
    def __init__(self, connector: AsyncConnector, heartbeat_interval_seconds: int):
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._connector: AsyncConnector = connector
        self._process = psutil.Process()

        # minus heartbeat interval seconds to trigger very first heartbeat when launching
        self._start: float = time.time() - self._heartbeat_interval_seconds

    async def routine(self):
        if time.time() - self._start < self._heartbeat_interval_seconds:
            return

        await self._connector.send(
            MessageType.Heartbeat, Heartbeat(self._process.cpu_percent() / 100, self._process.memory_info().rss)
        )
        self._start = time.time()

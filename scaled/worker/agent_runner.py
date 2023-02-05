import asyncio
import threading
import time

import psutil
import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Heartbeat, MessageType, MessageVariant
from scaled.utility.zmq_config import ZMQConfig


class AgentRunner:
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.asyncio.Context,
        address: ZMQConfig,
        address_internal: ZMQConfig,
        heartbeat_interval_seconds: int,
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
            bind_or_connect="connect",
            callback=self.on_receive_internal,
        )

        self._heartbeat = WorkerHeartbeat(
            connector=self._connector_external, heartbeat_interval_seconds=heartbeat_interval_seconds
        )

    @property
    def identity(self):
        return self._connector_external.identity

    async def on_receive_external(self, message_type: MessageType, message: MessageVariant):
        await self._connector_internal.send(message_type, message)

    async def on_receive_internal(self, message_type: MessageType, message: MessageVariant):
        await self._connector_external.send(message_type, message)

    async def loop(self):
        while not self._stop_event.is_set():
            await asyncio.gather(
                self._heartbeat.routine(), self._connector_external.routine(), self._connector_internal.routine()
            )


class WorkerHeartbeat:
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

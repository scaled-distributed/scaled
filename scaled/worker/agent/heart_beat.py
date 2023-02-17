import time

import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Heartbeat, MessageType


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

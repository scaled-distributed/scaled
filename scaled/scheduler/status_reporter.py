import json
from collections import ChainMap
from typing import List

import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, SchedulerStatus
from scaled.scheduler.mixins import Looper, Reporter


class StatusReporter(Looper):
    def __init__(self, binder: AsyncConnector):
        self._managers: List[Reporter] = []
        self._monitor_binder: AsyncConnector = binder

        self._process = psutil.Process()

    def register_manager(self, managers: List[Reporter]):
        self._managers.extend(managers)

    async def routine(self):
        stats = dict(ChainMap(*[await manager.statistics() for manager in self._managers]))
        stats["scheduler"] = {"cpu": self._process.cpu_percent() / 100, "rss": self._process.memory_info().rss}
        await self._monitor_binder.send(MessageType.SchedulerStatus, SchedulerStatus(json.dumps(stats).encode()))

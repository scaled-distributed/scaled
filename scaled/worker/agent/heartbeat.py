from typing import Optional

import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Heartbeat, MessageType
from scaled.worker.agent.mixins import Looper, HeartbeatManager, TaskManager


class VanillaHeartbeatManager(Looper, HeartbeatManager):
    def __init__(self):
        self._agent_process = psutil.Process()
        self._worker_process: Optional[psutil.Process] = None

        self._connector_external: Optional[AsyncConnector] = None
        self._worker_task_manager: Optional[TaskManager] = None

    def register(self, connector_external: AsyncConnector, worker_task_manager: TaskManager):
        self._connector_external = connector_external
        self._worker_task_manager = worker_task_manager

    def set_processor_pid(self, process_id: int):
        self._worker_process = psutil.Process(process_id)

    async def routine(self):
        if self._worker_process is None:
            return

        await self._connector_external.send(
            MessageType.Heartbeat,
            Heartbeat(
                self._agent_process.cpu_percent() / 100,
                self._agent_process.memory_info().rss,
                self._worker_process.cpu_percent() / 100,
                self._worker_process.memory_info().rss,
                self._worker_task_manager.get_queued_size(),
            ),
        )

import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Heartbeat, MessageType
from scaled.worker.agent.worker_task_manager import WorkerTaskManager


class WorkerHeartbeat:
    def __init__(self, connector_external: AsyncConnector, task_manager: WorkerTaskManager):
        self._connector_external: AsyncConnector = connector_external
        self._task_manager = task_manager

        self._process = psutil.Process()

    async def routine(self):
        await self._connector_external.send(
            MessageType.Heartbeat,
            Heartbeat(
                self._process.cpu_percent() / 100, self._process.memory_info().rss, self._task_manager.get_queue_size()
            ),
        )

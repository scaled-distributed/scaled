import time
from typing import Optional

import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Heartbeat, HeartbeatEcho
from scaled.worker.agent.mixins import Looper, HeartbeatManager, TaskManager, TimeoutManager


class VanillaHeartbeatManager(Looper, HeartbeatManager):
    def __init__(self):
        self._agent_process = psutil.Process()
        self._worker_process: Optional[psutil.Process] = None

        self._connector_external: Optional[AsyncConnector] = None
        self._worker_task_manager: Optional[TaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

    def register(
        self, connector_external: AsyncConnector, worker_task_manager: TaskManager, timeout_manager: TimeoutManager
    ):
        self._connector_external = connector_external
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager

    def set_processor_pid(self, process_id: int):
        self._worker_process = psutil.Process(process_id)

    async def on_heartbeat_echo(self, heartbeat: HeartbeatEcho):
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = (time.time_ns() - self._start_timestamp_ns) // 1_000
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

    async def routine(self):
        if self._worker_process is None:
            return

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        await self._connector_external.send(
            Heartbeat(
                self._agent_process.cpu_percent() / 100,
                self._agent_process.memory_info().rss,
                self._worker_process.cpu_percent() / 100,
                self._worker_process.memory_info().rss,
                self._worker_task_manager.get_queued_size(),
                self._latency_us,
            )
        )
        self._start_timestamp_ns = time.time_ns()

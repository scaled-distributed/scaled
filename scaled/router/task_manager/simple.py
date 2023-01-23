import asyncio
import threading
import logging
from typing import Dict, Optional, Set

from scaled.protocol.python.message import Task, TaskCancelEcho, TaskEcho, TaskResult
from scaled.protocol.python.objects import MessageType, TaskEchoStatus, TaskStatus
from scaled.router.mixins import Binder, TaskManager, WorkerManager


class SimpleTaskManager(TaskManager):
    def __init__(self, stop_event: threading.Event):
        self._stop_event = stop_event

        self._binder: Optional[Binder] = None
        self._worker_manager: Optional[WorkerManager] = None

        self._task_id_to_client: Dict[bytes, bytes] = dict()
        self._task_id_to_task: Dict[bytes,] = dict()
        self._running: Set[bytes] = set()
        self._canceling: Set[bytes] = set()

        self._failed: Set[bytes] = set()
        self._canceled: Set[bytes] = set()

    def hook(self, binder: Binder, worker_manager: WorkerManager):
        self._binder = binder
        self._worker_manager = worker_manager

    async def loop(self):
        while not self._stop_event.is_set():
            if not self._running or not self._canceling:
                await asyncio.sleep(0)
                continue

    async def on_task_new(self, client: bytes, task: Task):
        if task.task_id in self._running:
            logging.warning(f"task already running: {task=}")
            await self._binder.send(client, MessageType.TaskEcho, TaskEcho(task.task_id, TaskEchoStatus.Duplicated))
            return

        self._task_id_to_client[task.task_id] = client
        self._task_id_to_task[task.task_id] = task
        self._running.add(task.task_id)
        await self._binder.send(client, MessageType.TaskEcho, TaskEcho(task.task_id, TaskEchoStatus.OK))
        await self._worker_manager.on_task_new(task)

    async def on_task_cancel(self, client: bytes, task_id: bytes):
        if task_id in self._canceling:
            logging.warning(f"already canceling: {task_id=}")
            await self._binder.send(
                client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.Duplicated)
            )
            return

        if task_id not in self._running:
            logging.warning(f"cannot cancel task is not running: {task_id=}")
            await self._binder.send(
                client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.Failed)
            )
            return

        self._running.remove(task_id)
        self._canceling.add(task_id)
        await self._binder.send(client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.OK))
        await self._worker_manager.on_task_cancel(task_id)

    async def on_task_done(self, result: TaskResult):
        """job done can be success or failed"""
        match result.status:
            case TaskStatus.Success:
                await self._on_task_success(result)
            case TaskStatus.Failed:
                await self._on_task_failed(result)
            case TaskStatus.Canceled:
                await self._on_task_canceled(result)
            case _:
                raise ValueError(f"unknown TaskResult status: {result.status}")

    async def _on_task_success(self, result: TaskResult):
        assert result.task_id in self._running

        self._running.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        await self._binder.send(client, MessageType.TaskResult, result)

    async def _on_task_failed(self, result: TaskResult):
        assert result.task_id in self._running

        self._running.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        self._failed.add(result.task_id)
        await self._binder.send(client, MessageType.TaskResult, result)

    async def _on_task_canceled(self, result: TaskResult):
        assert result.task_id in self._canceling

        self._canceling.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        self._canceled.add(result.task_id)
        await self._binder.send(client, MessageType.TaskResult, result)

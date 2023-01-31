import threading
import logging
from collections import deque
from typing import Deque, Dict, Optional, Set, Tuple

from scaled.protocol.python.message import Task, TaskCancelEcho, TaskEcho, TaskResult
from scaled.protocol.python.objects import MessageType, TaskEchoStatus, TaskStatus
from scaled.scheduler.mixins import Binder, TaskManager, WorkerManager


class SimpleTaskManager(TaskManager):
    def __init__(self, stop_event: threading.Event):
        self._stop_event = stop_event

        self._binder: Optional[Binder] = None
        self._worker_manager: Optional[WorkerManager] = None

        self._task_id_to_client: Dict[bytes, bytes] = dict()
        self._task_id_to_task: Dict[bytes, Task] = dict()

        self._running: Set[bytes] = set()
        self._canceling: Set[bytes] = set()
        self._unassigned: Deque[Tuple[bytes, Task]] = deque()
        self._failed: Set[bytes] = set()
        self._canceled: Set[bytes] = set()

    def hook(self, binder: Binder, worker_manager: WorkerManager):
        self._binder = binder
        self._worker_manager = worker_manager

    async def routine(self):
        await self.__on_assign_tasks()

    async def statistics(self) -> Dict:
        return {
            "running": len(self._running),
            "canceling": len(self._canceling),
            "unassigned": len(self._unassigned),
            "failed": len(self._failed),
            "canceled": len(self._canceled),
        }

    async def on_task_new(self, client: bytes, task: Task):
        self._unassigned.append((client, task))
        await self.__on_assign_tasks()

    async def on_task(self, task: Task):
        assert task.task_id in self._task_id_to_client
        client = self._task_id_to_client[task.task_id]
        self._unassigned.append((client, task))

    async def on_task_cancel(self, client: bytes, task_id: bytes):
        if task_id in self._canceling:
            logging.warning(f"already canceling: {task_id=}")
            await self._binder.send(
                client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.Duplicated)
            )
            return

        if task_id not in self._running:
            logging.warning(f"cannot cancel task is not running: {task_id=}")
            await self._binder.send(client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.Failed))
            return

        self._running.remove(task_id)
        self._canceling.add(task_id)
        await self._binder.send(client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.OK))
        await self._worker_manager.on_task_cancel(task_id)

    async def on_task_done(self, result: TaskResult):
        """job done can be success or failed"""
        match result.status:
            case TaskStatus.Success:
                await self.__on_task_success(result)
            case TaskStatus.Failed:
                await self.__on_task_done(self._running, self._failed, result)
            case TaskStatus.Canceled:
                await self.__on_task_done(self._canceling, self._canceled, result)
            case _:
                raise ValueError(f"unknown TaskResult status: {result.status}")

    async def __on_assign_tasks(self):
        if not self._unassigned:
            return

        client, task = self._unassigned[0]
        if not await self._worker_manager.assign_task_to_worker(task):
            return

        self._unassigned.popleft()
        self._task_id_to_client[task.task_id] = client
        self._task_id_to_task[task.task_id] = task
        self._running.add(task.task_id)
        await self._binder.send(client, MessageType.TaskEcho, TaskEcho(task.task_id, TaskEchoStatus.OK))

    async def __on_task_success(self, result: TaskResult):
        assert result.task_id in self._running

        self._running.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        await self._binder.send(client, MessageType.TaskResult, result)

    async def __on_task_done(self, processing_set: Set[bytes], processed_set: Set[bytes], result: TaskResult):
        assert result.task_id in processing_set

        processing_set.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        processed_set.add(result.task_id)
        await self._binder.send(client, MessageType.TaskResult, result)

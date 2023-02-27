import asyncio
from asyncio import Queue
import logging
from typing import Dict, Literal, Optional, Set, Tuple

from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import (
    MessageType,
    Task,
    TaskCancelEcho,
    TaskEcho,
    TaskEchoStatus,
    TaskResult,
    TaskStatus,
)
from scaled.scheduler.mixins import FunctionManager, Looper, TaskManager, WorkerManager


class VanillaTaskManager(TaskManager, Looper):
    def __init__(self):
        self._binder: Optional[AsyncBinder] = None
        self._function_manager: Optional[FunctionManager] = None
        self._worker_manager: Optional[WorkerManager] = None

        self._task_id_to_client: Dict[bytes, bytes] = dict()
        self._task_id_to_task: Dict[bytes, Task] = dict()

        self._running: Set[bytes] = set()
        self._canceling: Set[bytes] = set()
        self._unassigned: Queue[Tuple[bytes, Task]] = Queue()

        self._failed_count: int = 0
        self._canceled_count: int = 0

    def hook(self, binder: AsyncBinder, function_manager: FunctionManager, worker_manager: WorkerManager):
        self._binder = binder
        self._function_manager = function_manager
        self._worker_manager = worker_manager

    async def loop(self):
        logging.info(f"{self.__class__.__name__}: started")
        while True:
            await self.__routine()
            await asyncio.sleep(0)

    async def statistics(self) -> Dict:
        return {
            "running": len(self._running),
            "canceling": len(self._canceling),
            "unassigned": self._unassigned.qsize(),
            "failed": self._failed_count,
            "canceled": self._canceled_count,
        }

    async def on_task_new(self, client: bytes, task: Task):
        if not await self._function_manager.has_function(task.function_id):
            await self._binder.send(
                client, MessageType.TaskEcho, TaskEcho(task.task_id, TaskEchoStatus.FunctionNotExists)
            )
            return

        await self._binder.send(client, MessageType.TaskEcho, TaskEcho(task.task_id, TaskEchoStatus.SubmitOK))
        await self._function_manager.on_task_use_function(task.task_id, task.function_id)
        await self._unassigned.put((client, task))

    async def on_task_reroute(self, task_id: bytes):
        assert task_id in self._task_id_to_client

        client = self._task_id_to_client.pop(task_id)
        task = self._task_id_to_task.pop(task_id)
        self._running.remove(task_id)

        await self._unassigned.put((client, task))

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
                client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.Duplicated)
            )
            return

        self._running.remove(task_id)
        self._canceling.add(task_id)
        await self._binder.send(client, MessageType.TaskCancelEcho, TaskCancelEcho(task_id, TaskEchoStatus.CancelOK))
        await self._worker_manager.on_task_cancel(task_id)

    async def on_task_done(self, result: TaskResult):
        """job done can be success or failed"""
        if result.status == TaskStatus.Success:
            await self.__on_task_success(result)
            return

        if result.status == TaskStatus.Failed:
            await self.__on_task_failed(self._running, "fail", result)
            return

        if result.status == TaskStatus.Canceled:
            await self.__on_task_failed(self._canceling, "cancel", result)
            return

        raise ValueError(f"unknown TaskResult status: {result.status}")

    async def __routine(self):
        client, task = await self._unassigned.get()

        if not await self._worker_manager.assign_task_to_worker(task):
            await self._unassigned.put((client, task))
            return

        self._task_id_to_client[task.task_id] = client
        self._task_id_to_task[task.task_id] = task
        self._running.add(task.task_id)

    async def __on_task_success(self, result: TaskResult):
        if result.task_id not in self._running:
            return

        self._running.remove(result.task_id)
        task = self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        await self._binder.send(client, MessageType.TaskResult, result)
        await self._function_manager.on_task_done_function(task.task_id, task.function_id)

    async def __on_task_failed(
        self, processing_set: Set[bytes], fail_type: Literal["cancel", "fail"], result: TaskResult
    ):
        if result.task_id not in processing_set:
            return

        processing_set.remove(result.task_id)
        task = self._task_id_to_task.pop(result.task_id)
        client = self._task_id_to_client.pop(result.task_id)

        if fail_type == "cancel":
            self._canceled_count += 1
        elif fail_type == "fail":
            self._failed_count += 1

        await self._binder.send(client, MessageType.TaskResult, result)
        await self._function_manager.on_task_done_function(task.task_id, task.function_id)

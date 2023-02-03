import enum
import threading
import logging
import time
from typing import Dict, Optional

from scaled.protocol.python.objects import MessageType
from scaled.scheduler.mixins import Binder, TaskManager, WorkerManager
from scaled.scheduler.worker_manager.allocators.one_to_one import OneToOneAllocator
from scaled.protocol.python.message import Heartbeat, Task, TaskResult, TaskCancel
from scaled.scheduler.worker_manager.allocators.queued import QueuedAllocator

POLLING_TIME = 1


class AllocatorType(enum.Enum):
    OneToOne = "one_to_one"
    Queued = "queued"

    def __repr__(self):
        return self.value


class SimpleWorkerManager(WorkerManager):
    def __init__(self, stop_event: threading.Event, allocator_type: AllocatorType, timeout_seconds: int):
        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[Binder] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since = {}

        if allocator_type == AllocatorType.OneToOne:
            self._allocator = OneToOneAllocator()
        elif allocator_type == AllocatorType.Queued:
            self._allocator = QueuedAllocator(1000)
        else:
            raise TypeError(f"received invalid allocator type: {allocator_type}")

    def hook(self, binder: Binder, task_manager: TaskManager):
        self._binder = binder
        self._task_manager = task_manager

    async def on_heartbeat(self, source: bytes, info: Heartbeat):
        self._allocator.add_worker(source)
        self._worker_alive_since[source] = time.time()

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = self._allocator.assign_task(task)
        if worker is None:
            return False

        # send to worker
        await self._binder.send(worker, MessageType.Task, task)
        return True

    async def on_task_cancel(self, task_id: bytes):
        worker = self._allocator.get_assigned_worker(task_id)
        if worker is None:
            logging.error(f"cannot find {task_id=} in task workers")
            return

        await self._binder.send(worker, MessageType.TaskCancel, TaskCancel(task_id))

    async def on_task_done(self, task_result: TaskResult):
        worker = self._allocator.remove_task(task_result.task_id)
        if worker is None:
            logging.error(f"received unknown task_id={task_result.task_id}")
            return

        await self._task_manager.on_task_done(task_result)

    async def routine(self):
        await self.__clean_workers()

    async def statistics(self) -> Dict:
        return self._allocator.status()

    async def __clean_workers(self):
        now = time.time()

        dead_workers = [
            worker
            for worker, alive_since in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
        ]

        for dead_worker in dead_workers:
            logging.info(f"worker {dead_worker} disconnected")
            self._worker_alive_since.pop(dead_worker)
            for task in self._allocator.remove_worker(dead_worker):
                logging.info(f"rerouting {task=}")
                await self._task_manager.on_task(task)

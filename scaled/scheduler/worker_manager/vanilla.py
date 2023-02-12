import enum
import threading
import logging
import time
from typing import Dict, Optional

from scaled.io.async_binder import AsyncBinder
from scaled.scheduler.mixins import TaskManager, WorkerManager
from scaled.protocol.python.message import Heartbeat, MessageType, Task, TaskResult, TaskCancel
from scaled.scheduler.worker_manager.allocators.queued import QueuedAllocator

POLLING_TIME = 1


class AllocatorType(enum.Enum):
    OneToOne = "one_to_one"
    Queued = "queued"

    def __repr__(self):
        return self.value


class VanillaWorkerManager(WorkerManager):
    def __init__(self, stop_event: threading.Event, per_worker_queue_size: int, timeout_seconds: int):
        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[AsyncBinder] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since = dict()
        self._allocator = QueuedAllocator(per_worker_queue_size)

    def hook(self, binder: AsyncBinder, task_manager: TaskManager):
        self._binder = binder
        self._task_manager = task_manager

    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        if self._allocator.add_worker(worker):
            logging.info(f"worker {worker} connected")

        self._worker_alive_since[worker] = time.time()

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = self._allocator.assign_task(task.task_id)
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
            logging.error(
                f"received task_id={task_result.task_id} not known to any worker, might due to worker get disconnected"
            )
            return

        await self._task_manager.on_task_done(task_result)

    async def routine(self):
        await self.__clean_workers()

    async def statistics(self) -> Dict:
        return self._allocator.statistics()

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            w for w, alive_since in self._worker_alive_since.items() if now - alive_since > self._timeout_seconds
        ]
        for dead_worker in dead_workers:
            logging.info(f"worker {dead_worker} disconnected")
            self._worker_alive_since.pop(dead_worker)

            task_ids = self._allocator.remove_worker(dead_worker)
            if not task_ids:
                continue

            logging.info(f"rerouting {len(task_ids)} tasks")
            for task_id in task_ids:
                await self._task_manager.on_task_reroute(task_id)

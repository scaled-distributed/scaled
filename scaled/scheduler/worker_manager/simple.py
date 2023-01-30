import threading
import logging
import time
from typing import Optional

from scaled.protocol.python.objects import MessageType
from scaled.scheduler.mixins import Binder, TaskManager, WorkerManager
from scaled.scheduler.worker_manager.worker_collection import WorkerCollection
from scaled.protocol.python.message import Heartbeat, Task, TaskResult, TaskCancel


POLLING_TIME = 1


class SimpleWorkerManager(WorkerManager):
    def __init__(self, stop_event: threading.Event, timeout_seconds: int):
        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[Binder] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since = {}
        self._task_to_worker = {}
        self._worker_to_task: WorkerCollection = WorkerCollection()

    def hook(self, binder: Binder, task_manager: TaskManager):
        self._binder = binder
        self._task_manager = task_manager

    async def on_heartbeat(self, source: bytes, info: Heartbeat):
        if not self._worker_to_task.has_worker(info.identity):
            logging.info(f"worker {info.identity} connected")
            self._worker_to_task[info.identity] = None

        self._worker_alive_since[info.identity] = time.time()

    async def assign_task_to_worker(self, task: Task) -> bool:
        if self._worker_to_task.full():
            return False

        worker = self._worker_to_task.get_unused_worker()
        self._worker_to_task[worker] = task
        self._task_to_worker[task.task_id] = worker

        # send to worker
        await self._binder.send(worker, MessageType.Task, task)
        return True

    async def on_task_cancel(self, task_id: bytes):
        if task_id not in self._task_to_worker:
            logging.error(f"cannot find {task_id=} in task workers")
            return

        worker = self._task_to_worker[task_id]
        await self._binder.send(worker, MessageType.TaskCancel, TaskCancel(task_id))

    async def on_task_done(self, task_result: TaskResult):
        if task_result.task_id not in self._task_to_worker:
            logging.error(f"received unknown task_id={task_result.task_id}")
            return

        worker = self._task_to_worker.pop(task_result.task_id)
        self._worker_to_task[worker] = None
        await self._task_manager.on_task_done(task_result)

    async def routine(self):
        await self.__clean_workers()

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
            task = self._worker_to_task.pop(dead_worker)
            if task is None:
                continue

            logging.info(f"rerouting {task=}")
            await self._task_manager.on_task(task)

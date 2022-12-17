import asyncio
import logging
import time
from typing import Optional

from scaled.protocol.python.objects import MessageType
from scaled.scheduler.mixins import Binder, JobDispatcher, WorkerManager
from scaled.scheduler.worker_manager.worker_collection import WorkerCollection
from scaled.protocol.python.message import Heartbeat, Task, TaskResult, TaskCancel


class VanillaWorkerManager(WorkerManager):
    def __init__(self, stop_event: asyncio.Event, timeout_seconds: int):
        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[Binder] = None
        self._job_dispatcher: Optional[JobDispatcher] = None

        self._alive_since = {}
        self._task_to_worker = {}
        self._worker_to_task: WorkerCollection = WorkerCollection()

    def hook(self, binder: Binder, job_dispatcher: JobDispatcher):
        self._binder = binder
        self._job_dispatcher = job_dispatcher

    async def on_task_new(self, task: Task):
        while not self._worker_to_task.full():
            await asyncio.sleep(0)

        # get available worker
        worker = self._worker_to_task.get_worker()
        self._worker_to_task[worker] = task
        self._task_to_worker[task.task_id] = worker

        # send to worker
        await self._binder.on_send(worker, MessageType.Task, task)

    async def on_task_cancel(self, task_id: bytes):
        if task_id not in self._task_to_worker:
            logging.error(f"cannot find {task_id=} in task workers")
            return

        worker = self._task_to_worker[task_id]
        await self._binder.on_send(worker, MessageType.TaskCancel, TaskCancel(task_id))

    async def on_task_done(self, task_result: TaskResult):
        worker = self._task_to_worker.pop(task_result.task_id)
        self._worker_to_task[worker] = None

    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        if worker not in self._worker_to_task:
            logging.info(f"connecting new worker {worker}")
            self._worker_to_task[worker] = None

        self._alive_since[worker] = time.time()

    async def loop(self):
        while not self._stop_event.is_set():
            await self._clean_workers()

    async def _clean_workers(self):
        now = time.time()
        for dead_worker, task in filter(lambda k, x: now - x > self._timeout_seconds, self._alive_since.items()):
            task = self._worker_to_task.pop(dead_worker)
            logging.info(f"removing worker {dead_worker} with {task=}")
            if task is None:
                continue

            await self.on_task_new(task)

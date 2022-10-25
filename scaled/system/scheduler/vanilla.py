import asyncio
import logging
import time

from scaled.system.scheduler.mixins import Scheduler
from scaled.system.io.binder import Binder
from scaled.system.scheduler.tools.worker_collection import WorkerCollection
from scaled.system.objects import HeartbeatInfo, Task


class VanillaScheduler(Scheduler):
    def __init__(self, backend: Binder, timeout_seconds: int):
        self._backend = backend
        self._timeout_seconds = timeout_seconds

        self._alive_since = {}
        self._task_to_worker = {}
        self._worker_to_task: WorkerCollection = WorkerCollection()

    async def on_task(self, task: Task):
        while not self._worker_to_task.full():
            await asyncio.sleep(0.01)

        # get available worker
        worker = self._worker_to_task.get_worker()
        self._worker_to_task[worker] = task
        self._task_to_worker[task.task_id] = worker

    async def on_task_done(self, task_id: int):
        worker = self._task_to_worker.pop(task_id)
        self._worker_to_task[worker] = None

    async def on_heartbeat(self, worker: bytes, info: HeartbeatInfo):
        if worker not in self._worker_to_task:
            logging.info(f"connecting new worker {worker}")
            self._worker_to_task[worker] = None

        self._alive_since[worker] = time.time()

    async def on_check(self):
        now = time.time()
        for dead_worker, task in filter(lambda k, x: now - x > self._timeout_seconds, self._alive_since.items()):
            task = self._worker_to_task.pop(dead_worker)
            logging.info(f"removing worker {dead_worker} with {task=}")
            if task is None:
                continue

            await self.on_task(task)

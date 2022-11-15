import asyncio
import logging
import time
from typing import Optional

from scaled.scheduler.mixins import Binder, JobDispatcher, WorkerManager
from scaled.scheduler.worker_manager.worker_collection import WorkerCollection
from scaled.protocol.python.message import Heartbeat, Job


class VanillaWorkerManager(WorkerManager):
    def __init__(self, timeout_seconds: int):
        self._binder: Optional[Binder] = None
        self._job_dispatcher: Optional[JobDispatcher] = None

        self._timeout_seconds = timeout_seconds

        self._alive_since = {}
        self._job_to_worker = {}
        self._worker_to_job: WorkerCollection = WorkerCollection()

    def hook(self, binder: Binder, job_dispatcher: JobDispatcher):
        self._binder = binder
        self._job_dispatcher = job_dispatcher

    async def on_task_new(self, job: Job) -> bytes:
        while not self._worker_to_job.full():
            await asyncio.sleep(0)

        # get available worker
        worker = self._worker_to_job.get_worker()
        self._worker_to_job[worker] = job
        self._job_to_worker[job.job_id] = worker
        return worker

    async def on_task_done(self, task_id: int):
        worker = self._job_to_worker.pop(task_id)
        self._worker_to_job[worker] = None

    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        if worker not in self._worker_to_job:
            logging.info(f"connecting new worker {worker}")
            self._worker_to_job[worker] = None

        self._alive_since[worker] = time.time()

    async def loop(self):
        while True:
            now = time.time()
            for dead_worker, task in filter(lambda k, x: now - x > self._timeout_seconds, self._alive_since.items()):
                task = self._worker_to_job.pop(dead_worker)
                logging.info(f"removing worker {dead_worker} with {task=}")
                if task is None:
                    continue

                await self.on_task_new(task)

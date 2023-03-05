import asyncio
import logging
import time
from typing import Dict, Optional, Tuple

from scaled.io.async_binder import AsyncBinder
from scaled.io.config import CLEANUP_INTERVAL_SECONDS
from scaled.scheduler.mixins import Looper, TaskManager, WorkerManager
from scaled.protocol.python.message import Heartbeat, MessageType, Task, TaskResult, TaskCancel
from scaled.scheduler.worker_manager.allocators.queued import QueuedAllocator

POLLING_TIME = 1


class VanillaWorkerManager(WorkerManager, Looper):
    def __init__(self, per_worker_queue_size: int, timeout_seconds: int):
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[AsyncBinder] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since: Dict[bytes, Tuple[float, Heartbeat]] = dict()
        self._allocator = QueuedAllocator(per_worker_queue_size)

    def hook(self, binder: AsyncBinder, task_manager: TaskManager):
        self._binder = binder
        self._task_manager = task_manager

    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        if self._allocator.add_worker(worker):
            logging.info(f"worker {worker} connected")

        self._worker_alive_since[worker] = (time.time(), info)

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
            logging.error(f"cannot find task_id={task_id.hex()} in task workers")
            return

        await self._binder.send(worker, MessageType.TaskCancel, TaskCancel(task_id))

    async def on_task_done(self, task_result: TaskResult):
        worker = self._allocator.remove_task(task_result.task_id)
        if worker is None:
            logging.error(
                f"received task_id={task_result.task_id.hex()} not known to any worker, might due to worker get "
                f"disconnected"
            )
            return

        await self._task_manager.on_task_done(task_result)

    async def has_available_worker(self) -> bool:
        return self._allocator.has_available_worker()

    async def loop(self):
        logging.info(f"{self.__class__.__name__}: started")
        while True:
            await self.__routine()
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

    async def statistics(self) -> Dict:
        worker_to_task_numbers = self._allocator.statistics()
        return {
            "scheduler_total_free": sum(worker["scheduler_free"] for worker in worker_to_task_numbers.values()),
            "scheduler_total_running": sum(worker["scheduler_running"] for worker in worker_to_task_numbers.values()),
            "worker_total_queued_tasks": sum(info.queued_tasks for _, (_, info) in self._worker_alive_since.items()),
            "workers": {
                worker.decode(): {
                    "worker_cpu": round(info.cpu_usage, 2),
                    "worker_rss": info.rss_size,
                    "worker_queued_tasks": info.queued_tasks,
                    **worker_to_task_numbers[worker],
                }
                for worker, (last, info) in self._worker_alive_since.items()
            },
        }

    async def __routine(self):
        await self.__clean_workers()

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
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

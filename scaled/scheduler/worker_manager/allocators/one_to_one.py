from typing import Dict, List, Optional
import logging

from scaled.protocol.python.message import Task
from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator
from scaled.scheduler.worker_manager.allocators.worker_collection import WorkerCollection


class OneToOneAllocator(TaskAllocator):
    def __init__(self):
        self._workers = WorkerCollection()
        self._task_to_worker = {}

    def add_worker(self, worker: bytes):
        if self._workers.has_worker(worker):
            return

        logging.info(f"worker {worker} connected")
        self._workers[worker] = None

    def remove_worker(self, worker: bytes) -> List[Task]:
        task = self._workers.pop(worker)
        if task is None:
            return []

        return [task]

    def assign_task(self, task: Task) -> Optional[bytes]:
        if self._workers.full():
            return None

        worker = self._workers.get_one_unused_worker()
        self._workers[worker] = task
        self._task_to_worker[task.task_id] = worker
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        return self._task_to_worker.get(task_id, None)

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_to_worker:
            logging.error(f"received {task_id=} not in workers")
            return None

        worker = self._task_to_worker.pop(task_id)
        self._workers[worker] = None
        return worker

    def status(self) -> Dict:
        return {
            "type": "one_to_one",
            "unused_workers": [worker.decode() for worker in self._workers.get_unused_workers()],
            "used_workers": {k.decode(): v.task_id.decode() for k, v in self._workers.get_used_workers().items()},
        }

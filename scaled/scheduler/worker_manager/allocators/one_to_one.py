from typing import Dict, List, Optional
import logging

from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator
from scaled.scheduler.worker_manager.allocators.worker_collection import WorkerCollection


class OneToOneAllocator(TaskAllocator):
    def __init__(self):
        self._workers = WorkerCollection()
        self._task_to_worker = {}

    def add_worker(self, worker: bytes) -> bool:
        if self._workers.has_worker(worker):
            return False

        self._workers[worker] = None
        return True

    def remove_worker(self, worker: bytes) -> List[bytes]:
        task_id = self._workers.pop(worker)
        if task_id is None:
            return []

        return [task_id]

    def assign_task(self, task_id: bytes) -> Optional[bytes]:
        if self._workers.full():
            return None

        worker = self._workers.get_one_unused_worker()
        self._workers[worker] = task_id
        self._task_to_worker[task_id] = worker
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

    def statistics(self) -> Dict:
        return {
            "type": "one_to_one",
            "unused_workers": [worker.decode() for worker in self._workers.get_unused_workers()],
            "used_workers": {k.decode(): v.decode() for k, v in self._workers.get_used_workers().items()},
        }

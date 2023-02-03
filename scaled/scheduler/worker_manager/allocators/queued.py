from collections import deque
from typing import Deque, Dict, List, Optional

from scaled.protocol.python.message import Task
from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator


class QueuedAllocator(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker
        self._workers: Dict[bytes, Dict[bytes, Task]] = dict()
        self._capacity: Deque[bytes] = deque()
        self._task_to_worker = {}

    def add_worker(self, worker: bytes):
        if worker in self._workers:
            return

        self._workers[worker] = dict()
        self._capacity.extend([worker] * self._max_tasks_per_worker)

    def remove_worker(self, worker: bytes) -> List[Task]:
        if worker not in self._workers:
            return []

        new_capacity = deque()
        new_capacity.extend([w for w in self._capacity if w != worker])
        self._capacity = new_capacity
        return list(self._workers.pop(worker).values())

    def assign_task(self, task: Task) -> Optional[bytes]:
        if not self._capacity:
            return None

        if task.task_id in self._task_to_worker:
            return self._task_to_worker[task.task_id]

        worker = self._capacity.popleft()
        self._workers[worker][task.task_id] = task
        self._task_to_worker[task.task_id] = worker
        return worker

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_to_worker:
            return None

        worker = self._task_to_worker.pop(task_id)
        self._workers[worker].pop(task_id)
        self._capacity.append(worker)
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_to_worker:
            return None

        return self._task_to_worker[task_id]

    def status(self) -> Dict:
        return {
            "type": "queued",
            "worker_to_tasks": {worker.decode(): len(tasks) for worker, tasks in self._workers.items()},
        }

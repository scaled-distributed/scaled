from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Set

from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator


class QueuedAllocator(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker
        self._workers_to_task_ids: Dict[bytes, Set[bytes]] = dict()
        self._capacity: Deque[bytes] = deque()
        self._task_to_worker = {}

    def add_worker(self, worker: bytes) -> bool:
        if worker in self._workers_to_task_ids:
            return False

        self._workers_to_task_ids[worker] = set()
        self._capacity.extend([worker] * self._max_tasks_per_worker)
        self.__reorganize_capacity()
        return True

    def remove_worker(self, worker: bytes) -> List[bytes]:
        if worker not in self._workers_to_task_ids:
            return []

        new_capacity = deque()
        new_capacity.extend([w for w in self._capacity if w != worker])
        self._capacity = new_capacity

        tasks = self._workers_to_task_ids.pop(worker)
        for task in tasks:
            self._task_to_worker.pop(task)
        return list(tasks)

    def assign_task(self, task_id: bytes) -> Optional[bytes]:
        if not self._capacity:
            return None

        if task_id in self._task_to_worker:
            return self._task_to_worker[task_id]

        worker = self._capacity.popleft()
        self._workers_to_task_ids[worker].add(task_id)
        self._task_to_worker[task_id] = worker
        return worker

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_to_worker:
            return None

        worker = self._task_to_worker.pop(task_id)
        self._workers_to_task_ids[worker].remove(task_id)
        self._capacity.append(worker)
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_to_worker:
            return None

        return self._task_to_worker[task_id]

    def statistics(self) -> Dict:
        return {
            "type": "queued",
            "worker_to_tasks": {worker.decode(): len(tasks) for worker, tasks in self._workers_to_task_ids.items()},
        }

    def __reorganize_capacity(self):
        worker_to_count = defaultdict(int)

        total_capacity = len(self._capacity)
        while self._capacity:
            worker_to_count[self._capacity.popleft()] += 1

        while total_capacity > 0:
            for worker in worker_to_count.keys():
                if worker_to_count[worker] == 0:
                    continue

                worker_to_count[worker] -= 1
                total_capacity -= 1
                self._capacity.append(worker)
                if total_capacity == 0:
                    break

        for worker, count in worker_to_count.items():
            assert count == 0, f"failed to reorganize {worker=}, capacity={count}"

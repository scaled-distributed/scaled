import math
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from scaled.scheduler.allocators.mixins import TaskAllocator
from scaled.utility.queues.async_priority_queue import PriorityQueue


class QueuedAllocator(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker
        self._workers_to_task_ids: Dict[bytes, Set[bytes]] = dict()
        self._task_id_to_worker: Dict[bytes, bytes] = {}

        self._worker_queue: PriorityQueue = PriorityQueue()

    async def add_worker(self, worker: bytes) -> bool:
        if worker in self._workers_to_task_ids:
            return False

        self._workers_to_task_ids[worker] = set()
        await self._worker_queue.put([0, worker])
        return True

    def remove_worker(self, worker: bytes) -> List[bytes]:
        if worker not in self._workers_to_task_ids:
            return []

        self._worker_queue.remove(worker)

        task_ids = self._workers_to_task_ids.pop(worker)
        for task_id in task_ids:
            self._task_id_to_worker.pop(task_id)
        return list(task_ids)

    def get_worker_ids(self) -> Set[bytes]:
        return set(self._workers_to_task_ids.keys())

    def balance(self) -> Dict[bytes, int]:
        balance_result: Dict[bytes, int] = dict()

        worker_task_count = {worker: len(tasks) for worker, tasks in self._workers_to_task_ids.items()}
        any_worker_has_task = any(worker_task_count.values())
        has_idle_workers = not all(worker_task_count.values())

        if not any_worker_has_task:
            return balance_result

        if not has_idle_workers:
            return balance_result

        mean = sum(worker_task_count.values()) / len(worker_task_count)
        return {worker: math.ceil(count - mean) for worker, count in worker_task_count.items() if count > mean}

    async def assign_task(self, task_id: bytes) -> Optional[bytes]:
        count, worker = await self._worker_queue.get()
        if count == self._max_tasks_per_worker:
            await self._worker_queue.put([count, worker])
            return None

        if task_id in self._task_id_to_worker:
            await self._worker_queue.put([count, worker])
            return self._task_id_to_worker[task_id]

        await self._worker_queue.put([count + 1, worker])
        self._workers_to_task_ids[worker].add(task_id)
        self._task_id_to_worker[task_id] = worker
        return worker

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        worker = self._task_id_to_worker.pop(task_id)
        self._workers_to_task_ids[worker].remove(task_id)

        self._worker_queue.decrease_priority(worker)
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        return self._task_id_to_worker[task_id]

    def has_available_worker(self) -> bool:
        if not len(self._worker_queue):
            return False

        count = self._worker_queue.max_priority()
        if count == self._max_tasks_per_worker:
            return False

        return True

    def statistics(self) -> Dict:
        return {
            worker: {"free": self._max_tasks_per_worker - len(tasks), "sent": len(tasks)}
            for worker, tasks in self._workers_to_task_ids.items()
        }

import heapq
import math
from typing import Dict, List, Optional, Set, Tuple, Union

from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator


class QueuedAllocator(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker
        self._workers_to_task_ids: Dict[bytes, Set[bytes]] = dict()
        self._task_id_to_worker: Dict[bytes, bytes] = {}

        self._capacity: CapacityPriorityQueue = CapacityPriorityQueue()

    def add_worker(self, worker: bytes) -> bool:
        if worker in self._workers_to_task_ids:
            return False

        self._workers_to_task_ids[worker] = set()
        self._capacity.push_worker(0, worker)
        return True

    def remove_worker(self, worker: bytes) -> List[bytes]:
        if worker not in self._workers_to_task_ids:
            return []

        self._capacity.remove_worker(worker)

        task_ids = self._workers_to_task_ids.pop(worker)
        for task_id in task_ids:
            self._task_id_to_worker.pop(task_id)
        return list(task_ids)

    def balance(self) -> Dict[bytes, int]:
        balance_result = dict()

        worker_task_count = {worker: len(tasks) for worker, tasks in self._workers_to_task_ids.items()}
        any_worker_has_task = any(worker_task_count.values())
        has_idle_workers = not all(worker_task_count.values())

        if not any_worker_has_task:
            return balance_result

        if not has_idle_workers:
            return balance_result

        mean = sum(worker_task_count.values()) / len(worker_task_count)
        return {worker: math.ceil(count - mean) for worker, count in worker_task_count.items() if count > mean}

    def assign_task(self, task_id: bytes) -> Optional[bytes]:
        if not self._capacity.queue_size():
            return None

        count, worker = self._capacity.pop_worker()
        if count == self._max_tasks_per_worker:
            self._capacity.push_worker(count, worker)
            return None

        if task_id in self._task_id_to_worker:
            self._capacity.push_worker(count, worker)
            return self._task_id_to_worker[task_id]

        self._capacity.push_worker(count + 1, worker)
        self._workers_to_task_ids[worker].add(task_id)
        self._task_id_to_worker[task_id] = worker
        return worker

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        worker = self._task_id_to_worker.pop(task_id)
        self._workers_to_task_ids[worker].remove(task_id)

        self._capacity.decrease_worker_count(worker)
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        return self._task_id_to_worker[task_id]

    def has_available_worker(self):
        if not self._capacity.queue_size():
            return False

        count, worker = self._capacity.pop_worker()
        self._capacity.push_worker(count, worker)

        if count == self._max_tasks_per_worker:
            return False

        return True

    def statistics(self) -> Dict:
        return {
            worker: {"free": self._max_tasks_per_worker - len(tasks), "sent": len(tasks)}
            for worker, tasks in self._workers_to_task_ids.items()
        }


class CapacityPriorityQueue:
    def __init__(self):
        self._capacity: List[List[Union[int, bytes]]] = []
        self._locator: Dict[bytes, List[Union[int, bytes]]] = {}

    def queue_size(self):
        return len(self._capacity)

    def push_worker(self, count: int, worker: bytes):
        entry = [count, worker]
        heapq.heappush(self._capacity, entry)
        self._locator[worker] = entry

    def pop_worker(self) -> Tuple[int, bytes]:
        count, worker = heapq.heappop(self._capacity)
        self._locator.pop(worker)
        return count, worker

    def remove_worker(self, worker: bytes):
        # this operation is O(log(n)), first change priority to -1 and pop from top of the heap, mark it as invalid
        # entry in the heap is not good idea as those invalid, entry will never get removed, so we used heapq internal
        # function _siftdown to maintain min heap invariant
        entry = self._locator.pop(worker)
        i = self._capacity.index(entry)
        entry[0] = -1
        heapq._siftdown(self._capacity, 0, i)
        assert heapq.heappop(self._capacity) == entry

    def decrease_worker_count(self, worker):
        # this operation should be O(log(n)), mark it as invalid entry in the heap is not good idea as those invalid
        # entry will never get removed, so we used heapq internal function _siftdown to maintain min heap invariant
        entry = self._locator[worker]
        i = self._capacity.index(entry)
        entry[0] -= 1
        heapq._siftdown(self._capacity, 0, i)

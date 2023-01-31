from typing import Dict, List, Optional

from scaled.protocol.python.message import Task
from scaled.scheduler.worker_manager.allocators.mixins import TaskAllocator
from scaled.scheduler.worker_manager.allocators.worker_collection import WorkerCollection


class QueuedAllocator(TaskAllocator):
    def __init__(self):
        self._workers = WorkerCollection()
        self._task_to_worker = {}

    def add_worker(self, worker: bytes):
        pass

    def remove_worker(self, worker: bytes) -> List[Task]:
        pass

    def assign_task(self, task: Task) -> Optional[bytes]:
        pass

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        pass

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        pass

    def status(self) -> Dict:
        pass
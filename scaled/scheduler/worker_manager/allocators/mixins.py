import abc
from typing import Dict, List, Optional

from scaled.protocol.python.message import Task


class TaskAllocator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: bytes):
        """add worker to worker collection"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: bytes) -> List[Task]:
        """remove worker to worker collection, and return list of tasks of removed worker"""
        raise NotImplementedError()

    @abc.abstractmethod
    def assign_task(self, task: Task) -> Optional[bytes]:
        """assign task in allocator, return None means no available worker, otherwise will return worker been
        assigned to"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        """remove task in allocator, return None means not found any worker, otherwise will return worker associate
        with the removed task_id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        """get worker that been assigned to this task_id, return None means cannot find the worker assigned to this
        task id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def status(self) -> Dict:
        raise NotImplementedError()

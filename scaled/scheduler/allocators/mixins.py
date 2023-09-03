import abc
from typing import Dict, List, Optional


class TaskAllocator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def add_worker(self, worker: bytes) -> bool:
        """add worker to worker collection"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: bytes) -> List[bytes]:
        """remove worker to worker collection, and return list of task_ids of removed worker"""
        raise NotImplementedError()

    @abc.abstractmethod
    def balance(self) -> Dict[bytes, int]:
        """balance worker, it should return the number of tasks for over burdened worker, represented as worker
        identity to number of tasks dictionary"""
        raise NotImplementedError()

    @abc.abstractmethod
    async def assign_task(self, task_id: bytes) -> Optional[bytes]:
        """assign task_id in allocator, return None means no available worker, otherwise will return worker been
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
    def has_available_worker(self) -> bool:
        """has available worker or not"""
        raise NotImplementedError()

    @abc.abstractmethod
    def statistics(self) -> Dict:
        raise NotImplementedError()

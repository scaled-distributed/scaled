import abc
from typing import Dict

from scaled.protocol.python.message import FunctionRequest, Heartbeat, Task, TaskResult


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()


class FunctionManager(Looper):
    @abc.abstractmethod
    async def on_function(self, source: bytes, request: FunctionRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_use_function(self, task_id: bytes, function_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done_function(self, task_id: bytes, function_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def has_function(self, function_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()


class ClientManager(Looper):
    @abc.abstractmethod
    async def on_task_new(self, client: bytes, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: bytes, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()


class TaskManager(Looper):
    @abc.abstractmethod
    async def on_task_new(self, client: bytes, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_reroute(self, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: bytes, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()


class WorkerManager(Looper):
    @abc.abstractmethod
    async def assign_task_to_worker(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()

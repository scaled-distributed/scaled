import abc
from typing import Awaitable, Callable, List, Tuple

from scaled.protocol.python.message import Heartbeat, Message, Task, TaskResult
from scaled.protocol.python.objects import MessageType


class Looper(metaclass=abc.ABCMeta):
    async def routine(self):
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

    async def routine(self):
        raise NotImplementedError()


class TaskManager(Looper):
    @abc.abstractmethod
    async def on_task_new(self, client: bytes, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: bytes, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    async def routine(self):
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


class Binder(Looper):
    @abc.abstractmethod
    def register(self, callback: Callable[[bytes, bytes, List[bytes]], Awaitable[None]]):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, to: bytes, message_type: MessageType, data: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()

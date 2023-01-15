import abc
from typing import Awaitable, Callable, List, Tuple

from scaled.protocol.python.objects import MessageType
from scaled.protocol.python.job_handler import AsyncJobHandler
from scaled.protocol.python.message import Task, TaskResult, Heartbeat
from scaled.protocol.python.serializer import Serializer


class TaskManager(AsyncJobHandler):
    @abc.abstractmethod
    async def on_task(self, client: bytes, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, job_id: int, message: Tuple[bytes, ...]):
        raise NotImplementedError()


class WorkerManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
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
    async def loop(self):
        raise NotImplementedError()


class Binder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def register(self, callback: Callable[[bytes, bytes, List[bytes]], Awaitable[None]]):
        raise NotImplementedError()

    @abc.abstractmethod
    async def loop(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_send(self, to: bytes, message_type: MessageType, data: Serializer):
        raise NotImplementedError()

import abc
from typing import Awaitable, Callable, List, Tuple

from scaled.protocol.python.objects import MessageType
from scaled.protocol.python.job_handler import AsyncJobHandler
from scaled.protocol.python.message import (
    Job,
    GraphJob,
    JobResult,
    Heartbeat,
)
from scaled.protocol.python.serializer import Serializer


class JobDispatcher(AsyncJobHandler):
    @abc.abstractmethod
    async def on_job(self, client: bytes, job: Job):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_job_done(self, job_result: JobResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_job(self, job_id: int, message: Tuple[bytes, ...]):
        raise NotImplementedError()


class WorkerManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, job: Job) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, job_id: int):
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

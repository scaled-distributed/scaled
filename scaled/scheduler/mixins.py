import abc
from typing import Dict, Optional, Set

from scaled.protocol.python.message import (
    BalanceResponse,
    DisconnectRequest,
    FunctionRequest,
    Heartbeat,
    Task,
    TaskCancel,
    TaskCancelEcho,
    TaskResult,
)


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()


class Reporter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def statistics(self) -> Dict:
        raise NotImplementedError()


class FunctionManager(metaclass=abc.ABCMeta):
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


class ClientManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_client_task_ids(self, client: bytes) -> Set[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_id(self, task_id: bytes) -> Optional[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_new(self, client: bytes, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_id: bytes) -> bytes:
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
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
    async def on_task_cancel_echo(self, worker: bytes, task_cancel_echo: TaskCancelEcho):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()


class WorkerManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def assign_task_to_worker(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def has_available_worker(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel_echo(self, worker: bytes, task_cancel_echo: TaskCancelEcho):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_balance_response(self, response: BalanceResponse):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_disconnect(self, source: bytes, request: DisconnectRequest):
        raise NotImplementedError()

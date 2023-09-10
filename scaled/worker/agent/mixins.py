import abc

from scaled.protocol.python.message import BalanceRequest
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionResponse
from scaled.protocol.python.message import HeartbeatEcho
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskCancel
from scaled.protocol.python.message import TaskResult


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        pass


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_processor_pid(self, process_id: int):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat_echo(self, heartbeat: HeartbeatEcho):
        raise NotImplementedError()


class TimeoutManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_last_seen_time(self):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_task(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_balance_request(self, balance_request: BalanceRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_queued_size(self):
        raise NotImplementedError()


class ProcessorManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_function_request(self, request: FunctionRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_function_response(self, response: FunctionResponse):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_task(self, task_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def restart_processor(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def initialized(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def current_task(self) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def task_lock(self) -> bool:
        raise NotImplementedError()

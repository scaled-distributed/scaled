import abc
from typing import List

from scaled.protocol.python.message import BalanceRequest, FunctionResponse, Task, TaskCancel, TaskResult


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        pass


class FunctionCacheManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_new_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_task(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_balance_request(self, request: BalanceRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_new_function(self, response: FunctionResponse):
        raise NotImplementedError()


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_processor_pid(self, process_id: int):
        raise NotImplementedError()


class ProcessorManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_add_function(self, function_id: bytes, function_content: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_delete_function(self, function_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_task(self, task_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def restart_processor(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def shutdown(self):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_queue_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_task(self, task_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_balance_remove_tasks(self, number_of_tasks: int) -> List[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_queued_size(self):
        raise NotImplementedError()

import abc

from scaled.protocol.python.message import BalanceRequest, FunctionResponse, Task, TaskCancel, TaskResult


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        pass


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_processor_pid(self, process_id: int):
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
    def on_add_function(self, function_response: FunctionResponse):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_delete_function(self, function_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, task_id: bytes):
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

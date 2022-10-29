import abc
from typing import List, AsyncGenerator, Tuple

from scaled.protocol.python import (
    WorkerTask,
    ClientMapJob,
    ClientGraphJob,
    FunctionRequest,
    AddFunction,
    ClientJobResult,
    WorkerTaskResult,
)


class JobManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_new_map_job(self, client: bytes, job: ClientMapJob) -> List[WorkerTask]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_new_graph_job(self, client: bytes, job: ClientGraphJob) -> List[WorkerTask]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_job(self, job_id: int, error_result: WorkerTaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_get_function(self, job: FunctionRequest) -> AddFunction:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_start(self, task: WorkerTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: WorkerTaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_routine(self) -> AsyncGenerator[Tuple[bytes, ClientJobResult], None, None]:
        raise NotImplementedError()

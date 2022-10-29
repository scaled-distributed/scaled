import abc

from scaled.protocol.python import WorkerTask, Heartbeat


class WorkerManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, task: WorkerTask) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_id: int):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def loop(self):
        raise NotImplementedError()

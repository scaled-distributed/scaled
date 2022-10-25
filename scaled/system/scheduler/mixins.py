import abc

from scaled.system.objects import HeartbeatInfo, Task


class Scheduler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_id: int):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: bytes, info: HeartbeatInfo):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_check(self):
        raise NotImplementedError()

"""
JobHandler interface came with 2 edition, one is normal, another is async version

Both scheduler or end worker need both implement this interface
- scheduler might divide job into multiple jobs and scatter to multiple workers
- worker need implement those interfaces to actually handle finish jobs

The purpose of used same interfaces for those are because we might in the future will have sub-scheduler, so the job
will be sent to main scheduler and then be split into sub jobs to sub-schedulers
"""
import abc
from typing import List, Tuple

from scaled.protocol.python.message import JobResult, Job


class JobHandler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_job(self, client: bytes, job: Job) -> List[Job]:
        """when received new map job instruction"""
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_job(self, job_id: int, message: Tuple[bytes, ...]):
        """when job received cancel instruction"""
        raise NotImplementedError()

    @abc.abstractmethod
    def on_job_done(self, job_result: JobResult) -> None:
        """job done can be success or failed"""
        raise NotImplementedError()


class AsyncJobHandler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_job(self, client: bytes, job: Job) -> List[Job]:
        """when received new map job instruction"""
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_job(self, job_id: int, message: Tuple[bytes, ...]):
        """when job received cancel instruction"""
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_job_done(self, job_result: JobResult) -> None:
        """job done can be success or failed"""
        raise NotImplementedError()

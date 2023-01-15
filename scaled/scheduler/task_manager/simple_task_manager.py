import asyncio
import uuid
from typing import List, Optional, Tuple

from scaled.protocol.python.objects import TaskStatus, MessageType
from scaled.protocol.python.message import Task, TaskResult
from scaled.scheduler.mixins import Binder, TaskManager, WorkerManager


class SimpleTaskManager(TaskManager):
    def __init__(self, stop_event: asyncio.Event):
        self._stop_event = stop_event

        self._binder: Optional[Binder] = None
        self._worker_manager: Optional[WorkerManager] = None

    def hook(self, binder: Binder, worker_manager: WorkerManager):
        self._binder = binder
        self._worker_manager = worker_manager

    async def loop(self):
        while not self._stop_event.is_set():
            if not self._job_id_func_to_job_result:
                await asyncio.sleep(0)
                continue

            (job_id, function_name), results = self._job_id_func_to_job_result.popitem()
            client = self._job_id_to_client.pop(job_id)
            await self._binder.on_send(
                client, MessageType.TaskResult, TaskResult(job_id, function_name, TaskStatus.Success, results)
            )

    async def on_task(self, client: bytes, task: Task) -> List[Task]:
        sub_jobs = []
        for args in task.function_args:
            sub_jobs.append(self._create_sub_job(task.task_id, task.function_name, args))

        self._job_id_to_sub_job_count[task.task_id] = 0
        self._job_id_to_client[task.task_id] = client
        return sub_jobs

    async def on_task_done(self, result: TaskResult):
        """job done can be success or failed"""
        match result.status:
            case TaskStatus.Success:
                self._on_sub_job_done(result)
            case TaskStatus.Failed:
                await self.on_task_cancel(result.task_id, (result.result,))
            case _:
                raise ValueError(f"unknown TaskResult status: {result.status}")

    async def on_task_cancel(self, job_id: bytes, message: Tuple[bytes, ...]):
        # TODO: implement it
        #  - cancel all sub jobs belong to a job
        #  - clean memory
        #  - mo
        raise NotImplementedError()

    def _create_sub_job(self, job_id: bytes, function_name: bytes, args: bytes) -> Task:
        sub_job_id = uuid.uuid1().bytes
        sub_job = Task(sub_job_id, function_name, (args,))
        self._sub_job_id_to_job[sub_job_id] = sub_job
        self._sub_job_id_to_job_id[sub_job_id] = job_id
        self._job_id_to_sub_job_ids[job_id].append(sub_job_id)
        return sub_job

    def _on_sub_job_done(self, sub_job_result: TaskResult):
        self._sub_job_id_to_result[sub_job_result.task_id] = sub_job_result.result

        sub_job_id = sub_job_result.task_id
        self._sub_job_id_to_job.pop(sub_job_id)
        job_id = self._sub_job_id_to_job_id.pop(sub_job_id)

        self._job_id_to_sub_job_count[job_id] += 1

        # not all tasks are done
        if self._job_id_to_sub_job_count[job_id] < len(self._job_id_to_sub_job_ids[job_id]):
            return

        self._job_id_to_sub_job_count.pop(job_id)

        self._job_id_func_to_job_result[(job_id, sub_job_result.function_name)] = tuple(
            result
            for sub_job_id in self._job_id_to_sub_job_ids.pop(job_id)
            for results in self._sub_job_id_to_result.pop(sub_job_id)
            for result in results
        )


def _create_function_key(job_id: bytes, function_name: bytes) -> bytes:
    return job_id + b"|" + function_name

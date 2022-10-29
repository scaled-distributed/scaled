import functools
from asyncio import Future
from collections import defaultdict
from typing import List, Dict, Tuple, AsyncGenerator

from scaled.io.objects import TaskStatus
from scaled.protocol.python import (
    WorkerTask,
    ClientMapJob,
    ClientGraphJob,
    AddFunction,
    FunctionRequest,
    ClientJobResult,
    WorkerTaskResult,
)
from scaled.scheduler.job_manager.mixins import JobManager


class SimpleJobManager(JobManager):
    def __init__(self):
        self._current_job_id = 0
        self._current_task_id = 0

        self._function_map: Dict[int, Dict[bytes, bytes]] = defaultdict(dict)

        self._job_id_to_task_ids: Dict[int, List[int]] = defaultdict(list)
        self._job_id_to_task_count: Dict[int, int] = {}
        self._job_id_to_client: Dict[int, bytes] = {}

        self._task_id_to_job_id: Dict[int, int] = {}
        self._task_id_to_task: Dict[int, WorkerTask] = {}
        self._task_id_to_future: Dict[int, Future] = {}

        self._job_id_to_result: Dict[int, Tuple[bytes, ...]] = {}

    async def on_new_map_job(self, client: bytes, job: ClientMapJob) -> List[WorkerTask]:
        self._add_function(job)

        tasks = []
        for args in job.list_of_args:
            tasks.append(self._create_task(client, job.job_id, job.function_name, args))

        self._job_id_to_task_count[job.job_id] = 0
        self._job_id_to_client[job.job_id] = client

        for task in tasks:
            await self.on_task_start(task)

        return tasks

    async def on_new_graph_job(self, client: bytes, job: ClientGraphJob) -> List[WorkerTask]:
        # TODO: implement it later
        pass

    async def on_cancel_job(self, job_id: int, error_result: WorkerTaskResult):
        # TODO: handle task failure
        #  - cancel all tasks belong to a job
        #  - clean memory
        #  - mo
        pass

    async def on_task_start(self, task: WorkerTask):
        future = Future()
        future.add_done_callback(functools.partial(self._on_task_done, task.task_id))
        self._task_id_to_future[task.task_id] = future

    async def on_task_done(self, task: WorkerTaskResult):
        if task.status == TaskStatus.Success:
            future = self._task_id_to_future[task.task_id]
            future.set_result(task.task_result)
            return

        job_id = self._task_id_to_job_id[task.task_id]
        await self.on_cancel_job(job_id, task)

    async def on_get_function(self, job: FunctionRequest) -> AddFunction:
        function_map = self._function_map[job.job_id]
        return AddFunction(job.function_name, function_map[job.function_name])

    async def on_routine(self) -> AsyncGenerator[Tuple[bytes, ClientJobResult], None, None]:
        for job_id, results in self._job_id_to_result.items():
            client = self._job_id_to_client.pop(job_id)
            yield client, ClientJobResult(job_id, results)

    def _add_function(self, job: ClientMapJob):
        self._function_map[job.job_id][job.function_name] = job.function

    def _create_task(self, client: bytes, job_id: int, function_name: bytes, args: bytes) -> WorkerTask:
        self._current_task_id += 1
        task_id = self._current_task_id
        task = WorkerTask(task_id, client, function_name, args)
        self._task_id_to_task[task_id] = task
        self._job_id_to_task_ids[job_id].append(task_id)
        self._task_id_to_job_id[task_id] = job_id
        return task

    def _on_task_done(self, task_id: int):
        self._task_id_to_task.pop(task_id)
        job_id = self._task_id_to_job_id.pop(task_id)
        self._on_job_done(job_id)

    def _on_job_done(self, job_id: int):
        self._job_id_to_task_count[job_id] += 1

        # not all tasks are done
        if self._job_id_to_task_count[job_id] < len(self._job_id_to_task_ids[job_id]):
            return

        self._job_id_to_task_count.pop(job_id)
        self._job_id_to_result[job_id] = tuple(
            future.result()
            for task_id in self._job_id_to_task_ids.pop(job_id) for future in self._task_id_to_future.pop(task_id)
        )

        self._function_map.pop(job_id)

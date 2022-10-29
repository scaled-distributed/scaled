import logging
from typing import List

from scaled.scheduler.binder import Binder
from scaled.scheduler.job_manager.mixins import JobManager
from scaled.io.objects import MessageType
from scaled.protocol.python import (
    PROTOCOL,
    Heartbeat,
    WorkerTask,
    AddFunction,
    ClientJobResult,
    ClientJobEcho,
    WorkerTaskResult,
)
from scaled.scheduler.worker_manager.mixins import WorkerManager


class Engine:
    def __init__(self, binder: Binder, job_manager: JobManager, worker_scheduler: WorkerManager):
        self._binder = binder
        self._job_manager = job_manager
        self._worker_scheduler = worker_scheduler

    async def on_receive_message(self, source: bytes, message_type: bytes, data: List[bytes]):
        obj = PROTOCOL[message_type].deserialize(*data)
        match message_type:
            case MessageType.ClientJobMap.value:
                await self._on_send_map_tasks(source, obj.job_id, await self._job_manager.on_new_map_job(source, obj))
            case MessageType.ClientJobGraph.value:
                await self._on_send_graph_tasks(source, obj.job_id, await self._job_manager.on_new_graph_job(source, obj))
            case MessageType.WorkerFunctionRequest.value:
                await self._on_send_function(source, await self._job_manager.on_get_function(obj))
            case MessageType.WorkerHeartbeat.value:
                await self._on_heartbeat(source, obj)
            case MessageType.WorkerTaskResult.value:
                await self._on_task_done(source, obj)
            case _:
                logging.error(f"unknown {message_type} from {source=}: {data}")

    async def loop(self):
        while True:
            async for client, result in await self._job_manager.on_routine():
                await self._on_handle_result(client, result)

    async def _on_heartbeat(self, worker: bytes, obj: Heartbeat):
        await self._worker_scheduler.on_heartbeat(worker, obj)

    async def _on_send_map_tasks(self, client: bytes, job_id: int, tasks: List[WorkerTask]):
        for task in tasks:
            worker = await self._worker_scheduler.on_task_new(task)
            await self._binder.send(worker, MessageType.WorkerTask, task.serialize())

        await self._binder.send(client, MessageType.ClientJobEcho, ClientJobEcho(job_id).serialize())

    async def _on_send_graph_tasks(self, client: bytes, job_id: int, tasks: List[WorkerTask]):
        pass

    async def _on_send_function(self, worker: bytes, add_function: AddFunction):
        await self._binder.send(worker, MessageType.WorkerFunctionAdd, add_function.serialize())

    async def _on_handle_result(self, client: bytes, result: ClientJobResult):
        await self._binder.send(client, MessageType.ClientJobResult, result.serialize())

    async def _on_task_done(self, worker: bytes, result: WorkerTaskResult):
        await self._job_manager.on_task_done(result)
        await self._worker_scheduler.on_task_done(result.task_id)

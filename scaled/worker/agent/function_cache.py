import time
from collections import defaultdict
from typing import Dict, List, Set

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import (
    BalanceRequest,
    BalanceResponse,
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
    Task,
    TaskCancel,
    TaskCancelEcho,
    TaskEchoStatus,
    TaskResult,
)
from scaled.worker.agent.worker_task_manager import WorkerTaskManager


class FunctionCache:
    def __init__(
        self,
        connector_external: AsyncConnector,
        connector_internal: AsyncConnector,
        task_manager: WorkerTaskManager,
        function_retention_seconds: int,
    ):
        self._connector_external = connector_external
        self._connector_internal = connector_internal
        self._task_manager = task_manager
        self._function_retention_seconds = function_retention_seconds

        self._task_id_to_function_id: Dict[bytes, bytes] = dict()
        self._function_id_to_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)
        self._cached_functions_alive_since: Dict[bytes, float] = dict()

        self._pending_tasks: Dict[bytes, List[Task]] = defaultdict(list)

    async def on_new_task(self, task: Task):
        if task.function_id not in self._cached_functions_alive_since:
            self._pending_tasks[task.function_id].append(task)
            await self._connector_external.send(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
            )
            return

        await self.__queue_task(task)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        if task_cancel.task_id not in self._task_id_to_function_id:
            await self._connector_external.send(
                MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelFailed)
            )
            return

        if not self._task_manager.remove_one_queued_task(task_cancel.task_id):
            await self._connector_external.send(
                MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelFailed)
            )
            return

        self.__remove_one_task_id(task_cancel.task_id)
        await self._connector_external.send(
            MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelOK)
        )

    async def on_task_result(self, result: TaskResult):
        self._task_manager.on_task_result()
        self.__remove_one_task_id(result.task_id)
        await self._connector_external.send(MessageType.TaskResult, result)

    async def on_balance_request(self, request: BalanceRequest):
        task_ids = self._task_manager.on_balance_remove_tasks(request.number_of_tasks)
        for task_id in task_ids:
            self.__remove_one_task_id(task_id)

        await self._connector_external.send(MessageType.BalanceResponse, BalanceResponse(task_ids))

    async def on_new_function(self, response: FunctionResponse):
        if response.function_id in self._cached_functions_alive_since:
            return

        assert response.status == FunctionResponseType.OK
        function_content = response.content

        self._cached_functions_alive_since[response.function_id] = time.time()
        await self._connector_internal.send(
            MessageType.FunctionRequest,
            FunctionRequest(FunctionRequestType.Add, response.function_id, function_content),
        )

        for task in self._pending_tasks.pop(response.function_id):
            await self.__queue_task(task)

    async def routine(self):
        now = time.time()
        idle_functions = [
            function_id
            for function_id, alive_since in self._cached_functions_alive_since.items()
            if now - alive_since > self._function_retention_seconds and not self._function_id_to_task_ids[function_id]
        ]
        for function_id in idle_functions:
            self._function_id_to_task_ids.pop(function_id)
            self._cached_functions_alive_since.pop(function_id)
            await self._connector_internal.send(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Delete, function_id, b"")
            )

    async def __queue_task(self, task: Task):
        self.__add_one_task(task)
        await self._task_manager.on_queue_task(task)

    def __add_one_task(self, task: Task):
        self._task_id_to_function_id[task.task_id] = task.function_id
        self._function_id_to_task_ids[task.function_id].add(task.task_id)
        self._cached_functions_alive_since[task.function_id] = time.time()

    def __remove_one_task_id(self, task_id: bytes):
        function_id = self._task_id_to_function_id.pop(task_id)
        self._function_id_to_task_ids[function_id].remove(task_id)

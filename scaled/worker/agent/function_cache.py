import asyncio
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
    Task, TaskResult,
)
from scaled.worker.agent.worker_task_manager import WorkerTaskManager


class FunctionCache:
    def __init__(
        self, connector_external: AsyncConnector, task_manager: WorkerTaskManager, function_retention_seconds: int
    ):
        self._connector_external = connector_external
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

        await self.__queue_new_task(task)

    async def on_task_result(self, task: TaskResult):
        function_id = self._task_id_to_function_id.pop(task.task_id)
        self._function_id_to_task_ids[function_id].remove(task.task_id)
        await self._connector_external.send(MessageType.TaskResult, task)

    async def on_balance_request(self, request: BalanceRequest):
        tasks = await self._task_manager.balance_remove_tasks(request.number_of_tasks)

        task_ids = []
        for task in tasks:
            function_id = self._task_id_to_function_id.pop(task.task_id)
            self._function_id_to_task_ids[function_id].remove(task.task_id)
            task_ids.append(task.task_id)

        await self._connector_external.send(MessageType.BalanceResponse, BalanceResponse(task_ids))


    async def on_new_function(self, response: FunctionResponse):
        if response.function_id in self._cached_functions_alive_since:
            return

        assert response.status == FunctionResponseType.OK
        function_content = response.content

        self._cached_functions_alive_since[response.function_id] = time.time()
        await self._task_manager.on_queue_message(
            MessageType.FunctionRequest,
            FunctionRequest(FunctionRequestType.Add, response.function_id, function_content),
        )

        for task in self._pending_tasks.pop(response.function_id):
            await self.__queue_new_task(task)

    async def loop(self):
        while True:
            await self.__routine()
            await asyncio.sleep(self._function_retention_seconds)

    async def __routine(self):
        now = time.time()
        idle_functions = [
            function_id
            for function_id, alive_since in self._cached_functions_alive_since.items()
            if now - alive_since > self._function_retention_seconds and not self._function_id_to_task_ids[function_id]
        ]
        for function_id in idle_functions:
            self._function_id_to_task_ids.pop(function_id)
            self._cached_functions_alive_since.pop(function_id)
            await self._task_manager.on_queue_message(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Delete, function_id, b"")
            )

    async def __queue_new_task(self, task):
        self._task_id_to_function_id[task.task_id] = task.function_id
        self._function_id_to_task_ids[task.function_id].add(task.task_id)
        self._cached_functions_alive_since[task.function_id] = time.time()
        await self._task_manager.on_queue_message(MessageType.Task, task)
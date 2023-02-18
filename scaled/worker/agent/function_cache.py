import time
from collections import defaultdict
from typing import Dict, List

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
    Task,
)
from scaled.worker.agent.task_queue import TaskQueue


class FunctionCache:
    def __init__(self, connector_external: AsyncConnector, task_queue: TaskQueue, function_retention_seconds: int):
        self._connector_external = connector_external
        self._task_queue = task_queue
        self._function_retention_seconds = function_retention_seconds

        self._cached_functions: Dict[bytes, bytes] = dict()
        self._cached_functions_alive_since: Dict[bytes, float] = dict()
        self._pending_tasks: Dict[bytes, List[Task]] = defaultdict(list)

    async def on_new_task(self, task: Task):
        if task.function_id in self._cached_functions:
            task.function_content = self._cached_functions[task.function_id]
            self._cached_functions_alive_since[task.function_id] = time.time()
            await self._task_queue.on_receive_task(task)
            return

        self._pending_tasks[task.function_id].append(task)
        await self._connector_external.send(
            MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
        )

    async def on_new_function(self, response: FunctionResponse):
        if response.function_id in self._cached_functions:
            return

        assert response.status == FunctionResponseType.OK
        function_content = response.content

        self._cached_functions[response.function_id] = function_content
        self._cached_functions_alive_since[response.function_id] = time.time()

        for task in self._pending_tasks.pop(response.function_id):
            task.function_content = function_content
            await self._task_queue.on_receive_task(task)

    async def routine(self):
        now = time.time()
        idle_functions = [
            function_id
            for function_id, alive_since in self._cached_functions_alive_since.items()
            if now - alive_since > self._function_retention_seconds
        ]
        for function_id in idle_functions:
            self._cached_functions_alive_since.pop(function_id)
            self._cached_functions.pop(function_id)

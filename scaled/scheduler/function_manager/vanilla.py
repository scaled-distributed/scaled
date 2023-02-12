import logging
from collections import defaultdict
import time
from typing import Dict, Optional, Set

from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
)
from scaled.scheduler.mixins import FunctionManager


class VanillaFunctionManager(FunctionManager):
    def __init__(self, function_retention_seconds: int):
        self._function_id_to_function: Dict[bytes, bytes] = dict()
        self._function_id_to_alive_since: Dict[bytes, float] = dict()

        self._function_id_to_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)

        self._function_retention_seconds = function_retention_seconds

        self._binder: Optional[AsyncBinder] = None

    def hook(self, binder: AsyncBinder):
        self._binder = binder

    async def on_function(self, source: bytes, request: FunctionRequest):
        if request.type == FunctionRequestType.Check:
            await self.__on_function_check(source, request.function_id)
            return

        if request.type == FunctionRequestType.Add:
            await self.__on_function_add(source, request.function_id, request.content)
            return

        if request.type == FunctionRequestType.Request:
            await self.__on_function_request(source, request)
            return

        if request.type == FunctionRequestType.Delete:
            await self.__on_function_scheduler_delete(source, request.function_id)
            return

        logging.error(f"received unknown function request type {request=} from {source=}")

    async def __on_function_request(self, source: bytes, function_request: FunctionRequest):
        if function_request.function_id not in self._function_id_to_function:
            await self._binder.send(
                source,
                MessageType.FunctionResponse,
                FunctionResponse(FunctionResponseType.NotExists, function_request.function_id, b""),
            )
            return

        await self._binder.send(
            source,
            MessageType.FunctionResponse,
            FunctionResponse(
                FunctionResponseType.OK,
                function_request.function_id,
                self._function_id_to_function[function_request.function_id],
            ),
        )

    async def has_function(self, function_id: bytes) -> bool:
        return function_id in self._function_id_to_function

    async def on_task_use_function(self, task_id: bytes, function_id: bytes):
        self._function_id_to_alive_since[function_id] = time.time()
        self._function_id_to_task_ids[function_id].add(task_id)

    async def on_task_done_function(self, task_id: bytes, function_id: bytes):
        task_ids = self._function_id_to_task_ids[function_id]
        task_ids.remove(task_id)
        if not task_ids:
            self._function_id_to_task_ids.pop(function_id)

    async def __on_function_check(self, client: bytes, function_id: bytes):
        if function_id in self._function_id_to_function:
            await self.__send_function_response(client, function_id, FunctionResponseType.OK)
            return

        await self.__send_function_response(client, function_id, FunctionResponseType.NotExists)

    async def __on_function_add(self, client: bytes, function_id: bytes, function: bytes):
        self._function_id_to_alive_since[function_id] = time.time()
        self._function_id_to_function[function_id] = function

        if function_id in self._function_id_to_function:
            await self.__send_function_response(client, function_id, FunctionResponseType.Duplicated)

        await self.__send_function_response(client, function_id, FunctionResponseType.OK)

    async def __on_function_scheduler_delete(self, client: bytes, function_id: bytes):
        if function_id not in self._function_id_to_function:
            await self.__send_function_response(client, function_id, FunctionResponseType.NotExists)
            return

        if len(self._function_id_to_task_ids[function_id]) > 0:
            await self.__send_function_response(client, function_id, FunctionResponseType.StillHaveTask)
            return

        self._function_id_to_function.pop(function_id)
        self._function_id_to_alive_since.pop(function_id)

        self._function_id_to_task_ids.pop(function_id)

        await self.__send_function_response(client, function_id, FunctionResponseType.StillHaveTask)

    async def __send_function_response(self, client: bytes, function_id: bytes, response_type: FunctionResponseType):
        await self._binder.send(client, MessageType.FunctionResponse, FunctionResponse(response_type, function_id, b""))

    async def routine(self):
        now = time.time()
        dead_functions = [
            function_id
            for function_id, alive_since in self._function_id_to_alive_since.items()
            if now - alive_since > self._function_retention_seconds and function_id not in self._function_id_to_task_ids
        ]

        for function_id in dead_functions:
            logging.info(f"remove function cache {function_id=}")
            self._function_id_to_function.pop(function_id)
            self._function_id_to_alive_since.pop(function_id)

    async def statistics(self) -> Dict:
        return {"function_id_to_tasks": {k.decode(): len(v) for k, v in self._function_id_to_task_ids.items()}}

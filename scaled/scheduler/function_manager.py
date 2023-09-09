import dataclasses
import logging
import time
from typing import Dict
from typing import Optional

from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionRequestType
from scaled.protocol.python.message import FunctionResponse
from scaled.protocol.python.message import FunctionResponseType
from scaled.scheduler.mixins import FunctionManager
from scaled.scheduler.mixins import Looper
from scaled.scheduler.mixins import Reporter
from scaled.scheduler.mixins import WorkerManager
from scaled.utility.formatter import format_bytes
from scaled.utility.one_to_many_dict import OneToManyDict


@dataclasses.dataclass
class _FuncInfo:
    function_name: bytes
    content: bytes
    alive_since: float


class VanillaFunctionManager(FunctionManager, Looper, Reporter):
    def __init__(self, function_retention_seconds: int):
        self._function_retention_seconds = function_retention_seconds

        self._function_id_to_func_info: Dict[bytes, _FuncInfo] = dict()
        self._function_id_to_task_ids: OneToManyDict[bytes, bytes] = OneToManyDict()

        self._binder: Optional[AsyncBinder] = None
        self._worker_manager: Optional[WorkerManager] = None

    def register(self, binder: AsyncBinder, worker_manager: WorkerManager):
        self._binder = binder
        self._worker_manager = worker_manager

    async def on_function(self, source: bytes, request: FunctionRequest):
        if request.type == FunctionRequestType.Check:
            await self.__on_function_check(source, request.function_id)
            return

        if request.type == FunctionRequestType.Add:
            await self.__on_function_add(source, request.function_id, request.function_name, request.content)
            return

        if request.type == FunctionRequestType.Request:
            await self.__on_function_request(source, request)
            return

        if request.type == FunctionRequestType.Delete:
            await self.__on_function_scheduler_delete(source, request.function_id)
            return

        logging.error(f"received unknown function request type {request=} from {source=}")

    def has_function(self, function_id: bytes) -> bool:
        return function_id in self._function_id_to_func_info

    def get_function_name(self, function_id: bytes) -> bytes:
        return self._function_id_to_func_info[function_id].function_name

    async def on_task_use_function(self, task_id: bytes, function_id: bytes):
        self._function_id_to_func_info[function_id].alive_since = time.time()
        self._function_id_to_task_ids.add(function_id, task_id)

    async def on_task_done_function(self, task_id: bytes):
        self._function_id_to_task_ids.remove_value(task_id)

    async def routine(self):
        now = time.time()
        dead_functions = [
            function_id
            for function_id, func_info in self._function_id_to_func_info.items()
            if now - func_info.alive_since > self._function_retention_seconds
            and function_id not in self._function_id_to_task_ids
        ]

        for function_id in dead_functions:
            func_info = self._function_id_to_func_info.pop(function_id)
            logging.info(
                f"remove function cache function_id={function_id.hex()}, "
                f"func_name={func_info.function_name!r}, "
                f"size={format_bytes(len(func_info.content))}"
            )
            await self._worker_manager.on_delete_function(function_id)

    async def statistics(self) -> Dict:
        return {
            "function_manager": {
                self._function_id_to_func_info[k].function_name.decode(): len(v)
                for k, v in self._function_id_to_task_ids.items()
            }
        }

    async def __on_function_request(self, source: bytes, function_request: FunctionRequest):
        if function_request.function_id not in self._function_id_to_func_info:
            await self._binder.send(
                source, FunctionResponse(FunctionResponseType.NotExists, function_request.function_id, b"", b"")
            )
            return

        info = self._function_id_to_func_info[function_request.function_id]
        await self._binder.send(
            source,
            FunctionResponse(FunctionResponseType.OK, function_request.function_id, info.function_name, info.content),
        )

    async def __on_function_check(self, client: bytes, function_id: bytes):
        if function_id in self._function_id_to_func_info:
            await self.__send_function_response(client, function_id, FunctionResponseType.OK)
            return

        await self.__send_function_response(client, function_id, FunctionResponseType.NotExists)

    async def __on_function_add(self, client: bytes, function_id: bytes, function_name: bytes, function: bytes):
        if function_id in self._function_id_to_func_info:
            self._function_id_to_func_info[function_id].alive_since = time.time()
            await self.__send_function_response(client, function_id, FunctionResponseType.Duplicated)
            return

        logging.info(
            f"add function cache function_id={function_id.hex()}, "
            f"func_name={function_name!r}, "
            f"size={format_bytes(len(function))}"
        )
        self._function_id_to_func_info[function_id] = _FuncInfo(function_name, function, time.time())
        await self.__send_function_response(client, function_id, FunctionResponseType.OK)

    async def __on_function_scheduler_delete(self, client: bytes, function_id: bytes):
        if function_id not in self._function_id_to_func_info:
            await self.__send_function_response(client, function_id, FunctionResponseType.NotExists)
            return

        if len(self._function_id_to_task_ids.get_values(function_id)) > 0:
            await self.__send_function_response(client, function_id, FunctionResponseType.StillHaveTask)
            return

        self._function_id_to_func_info.pop(function_id)
        self._function_id_to_task_ids.remove_key(function_id)

        await self.__send_function_response(client, function_id, FunctionResponseType.StillHaveTask)

    async def __send_function_response(self, client: bytes, function_id: bytes, response_type: FunctionResponseType):
        await self._binder.send(client, FunctionResponse(response_type, function_id, b"", b""))

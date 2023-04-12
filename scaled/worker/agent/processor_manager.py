import os
import signal
import tempfile
import uuid

from typing import Optional

import asyncio

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import FunctionRequest, FunctionRequestType, MessageType, MessageVariant, Task
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.worker.agent.mixins import Looper, FunctionCacheManager, HeartbeatManager, ProcessorManager
from scaled.worker.agent.processor.processor import Processor


class VanillaProcessorManager(Looper, ProcessorManager):
    def __init__(
        self,
        event_loop: str,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: FunctionSerializerType,
    ):
        self._event_loop = event_loop
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer
        self._lock = asyncio.Lock()

        self._address_path = os.path.join(tempfile.gettempdir(), f"scaled_worker_{uuid.uuid4().hex}")
        self._address = ZMQConfig(ZMQType.ipc, host=self._address_path)

        self._heartbeat: Optional[HeartbeatManager] = None
        self._function_cache: Optional[FunctionCacheManager] = None
        self._processor: Optional[Processor] = None
        self._current_task_id: Optional[bytes] = None

        self._connector: AsyncConnector = AsyncConnector(
            context=zmq.asyncio.Context(),
            prefix="IM",
            socket_type=zmq.PAIR,
            bind_or_connect="bind",
            address=self._address,
            callback=self.__on_receive_internal,
        )

    def register(self, heartbeat: HeartbeatManager, function_cache: FunctionCacheManager):
        self._heartbeat = heartbeat
        self._function_cache = function_cache

    async def routine(self):
        await self._connector.routine()

    async def on_add_function(self, function_id: bytes, function_content: bytes):
        await self._connector.send(
            MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Add, function_id, function_content)
        )

    async def on_delete_function(self, function_id: bytes):
        await self._connector.send(
            MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Delete, function_id, b"")
        )

    async def on_task(self, task: Task):
        await self._lock.acquire()
        self._current_task_id = task.task_id
        await self._connector.send(MessageType.Task, task)

    def on_task_result(self, task_id: bytes):
        if task_id != self._current_task_id:
            raise ValueError(
                f"get cancel on task={task_id.hex()}, but current running task={self._current_task_id.hex()}"
            )

        self._current_task_id = None
        self._lock.release()

    def on_cancel_task(self, task_id: bytes) -> bool:
        # TODO: fix bug
        # if task_id == self._current_task_id:
        #     self.restart_processor()
        #     return True

        return False

    def restart_processor(self):
        self.__kill()
        self.__start_new_processor()

    def shutdown(self):
        self.__kill()
        self._connector.shutdown()
        os.remove(self._address_path)

    def __kill(self):
        if self._processor is None:
            return

        os.kill(self._processor.ident, signal.SIGTERM)

    def __start_new_processor(self):
        self._processor = Processor(
            event_loop=self._event_loop,
            address=self._address,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            serializer=self._serializer,
        )
        self._processor.start()
        self._heartbeat.set_processor_pid(self._processor.pid)
        self._current_task_id = None

        if self._lock.locked():
            self._lock.release()

    async def __on_receive_internal(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.TaskResult:
            await self._function_cache.on_task_result(message)
            return

        raise TypeError(f"Unknown {message=}")

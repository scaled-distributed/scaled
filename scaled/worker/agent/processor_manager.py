import asyncio
import dataclasses
import logging
import os
import signal
import tempfile
import uuid
from typing import Optional

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionRequestType
from scaled.protocol.python.message import FunctionResponse
from scaled.protocol.python.message import FunctionResponseType
from scaled.protocol.python.message import MessageVariant
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskResult
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.logging.utility import setup_logger
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.zmq_config import ZMQType
from scaled.worker.agent.mixins import HeartbeatManager
from scaled.worker.agent.mixins import Looper
from scaled.worker.agent.mixins import ProcessorManager
from scaled.worker.agent.mixins import TaskManager
from scaled.worker.agent.processor.processor import Processor


@dataclasses.dataclass
class _ProcessorHolder:
    processor: Optional[Processor]
    task: Optional[Task]
    initialized: asyncio.Event
    task_wait_lock: asyncio.Lock


class VanillaProcessorManager(Looper, ProcessorManager):
    def __init__(
        self,
        event_loop: str,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        function_retention_seconds: int,
        serializer: Serializer,
    ):
        self._event_loop = event_loop
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._function_retention_seconds = function_retention_seconds
        self._serializer = serializer

        self._address_path = os.path.join(tempfile.gettempdir(), f"scaled_worker_{uuid.uuid4().hex}")
        self._address = ZMQConfig(ZMQType.ipc, host=self._address_path)

        self._heartbeat: Optional[HeartbeatManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._connector_external: Optional[AsyncConnector] = None

        self._processor_holder: _ProcessorHolder = _ProcessorHolder(None, None, asyncio.Event(), asyncio.Lock())

        self._connector_internal: AsyncConnector = AsyncConnector(
            context=zmq.asyncio.Context(),
            socket_type=zmq.PAIR,
            bind_or_connect="bind",
            address=self._address,
            callback=self.__on_receive_internal,
        )

    def register(self, heartbeat: HeartbeatManager, task_manager: TaskManager, connector_external: AsyncConnector):
        self._heartbeat = heartbeat
        self._task_manager = task_manager
        self._connector_external = connector_external

    def initialize(self):
        setup_logger()
        self.__start_new_processor()

    async def routine(self):
        await self._connector_internal.routine()

    async def on_function_request(self, request: FunctionRequest):
        if request.type == FunctionRequestType.Delete:
            await self._connector_internal.send(
                FunctionRequest(FunctionRequestType.Delete, request.function_id, b"", b"")
            )
            return

        raise TypeError(f"unknown function request type: {request.type}")

    async def on_task(self, task: Task) -> bool:
        await self._processor_holder.task_wait_lock.acquire()

        await self._processor_holder.initialized.wait()
        assert self._processor_holder.task is None
        self._processor_holder.task = task
        await self._connector_internal.send(self._processor_holder.task)
        return True

    async def on_function_response(self, response: FunctionResponse):
        if not self._processor_holder.initialized.is_set():
            return

        if self._processor_holder.task.function_id != response.function_id:
            return

        if response.status == FunctionResponseType.NotExists:
            logging.info(f"cannot get function for task={self._processor_holder.task.task_id.hex()}, must be canceled")
            return

        await self._connector_internal.send(response)

    async def on_cancel_task(self, task_id: bytes) -> bool:
        if not self._processor_holder.initialized.is_set():
            return False

        if self._processor_holder.task.task_id != task_id:
            return False

        self.restart_processor()
        self._processor_holder.task = None
        self._processor_holder.task_wait_lock.release()
        return True

    def initialized(self) -> bool:
        return self._processor_holder.initialized.is_set()

    def current_task(self) -> bytes:
        return self._processor_holder.task.task_id if self._processor_holder.task else b""

    def task_lock(self) -> bool:
        return self._processor_holder.task_wait_lock.locked()

    def restart_processor(self):
        self.__kill()
        self.__start_new_processor()

    def destroy(self):
        self.__kill()
        self._connector_internal.destroy()
        os.remove(self._address_path)

    def __kill(self):
        if self._processor_holder.processor is None:
            self._processor_holder.initialized.clear()
            return

        logging.info(f"Worker[{os.getpid()}]: stop Processor[{self._processor_holder.processor.pid}]")
        os.kill(self._processor_holder.processor.pid, signal.SIGTERM)

        self._processor_holder.processor = None
        self._processor_holder.initialized.clear()

    def __start_new_processor(self):
        self._processor_holder.processor = Processor(
            event_loop=self._event_loop,
            address=self._address,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            function_retention_seconds=self._function_retention_seconds,
            serializer=self._serializer,
        )

        self._processor_holder.processor.start()
        self._heartbeat.set_processor_pid(self._processor_holder.processor.pid)

        logging.info(f"Worker[{os.getpid()}]: start Processor[{self._processor_holder.processor.pid}]")

    async def __on_internal_task_result(self, task_result: TaskResult):
        if not self._processor_holder.initialized.is_set():
            await self.__on_internal_processor_initialized(task_result)
            return

        if self._processor_holder.task is None:
            return

        if self._processor_holder.task.task_id != task_result.task_id:
            return

        await self._task_manager.on_task_result(task_result)

        assert self._processor_holder.task is not None
        self._processor_holder.task = None
        self._processor_holder.task_wait_lock.release()

    async def __on_internal_processor_initialized(self, task_result: TaskResult):
        if task_result.task_id != b"":
            return

        self._processor_holder.initialized.set()
        if self._processor_holder.task is not None:
            await self._connector_internal.send(self._processor_holder.task)

    async def __on_receive_internal(self, message: MessageVariant):
        if isinstance(message, FunctionRequest):
            await self.__on_internal_function_request(message)
            return

        if isinstance(message, TaskResult):
            await self.__on_internal_task_result(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __on_internal_function_request(self, request: FunctionRequest):
        if not self._processor_holder.initialized.is_set():
            return

        if self._processor_holder.task is None:
            return

        if self._processor_holder.task.function_id != request.function_id:
            return

        assert request.type == FunctionRequestType.Request
        await self._connector_external.send(request)

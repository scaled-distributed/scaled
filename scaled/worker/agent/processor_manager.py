import multiprocessing

from typing import Optional

import asyncio

import psutil

from scaled.protocol.python.message import FunctionRequest, FunctionRequestType, MessageVariant, Task, TaskResult
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.queues.async_multiprocessing_queue import AsyncMultiprocessingQueue
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

        self._heartbeat: Optional[HeartbeatManager] = None
        self._function_cache: Optional[FunctionCacheManager] = None

        self._send_queue = multiprocessing.get_context("spawn").Queue()
        self._receive_queue = multiprocessing.get_context("spawn").Queue()
        self._async_receive_queue = AsyncMultiprocessingQueue(self._receive_queue)

        self._processor: Optional[Processor] = None
        self._current_task_id: Optional[bytes] = None

    def register(self, heartbeat: HeartbeatManager, function_cache: FunctionCacheManager):
        self._heartbeat = heartbeat
        self._function_cache = function_cache

    async def routine(self):
        message = await self._async_receive_queue.get()
        await self.__on_receive_internal(message)

    def on_add_function(self, function_id: bytes, function_content: bytes):
        self._send_queue.put(FunctionRequest(FunctionRequestType.Add, function_id, function_content))

    def on_delete_function(self, function_id: bytes):
        self._send_queue.put(FunctionRequest(FunctionRequestType.Delete, function_id, b""))

    async def on_task(self, task: Task):
        await self._lock.acquire()
        self._current_task_id = task.task_id
        self._send_queue.put(task)

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

    def __kill(self):
        if self._processor is None:
            return

        psutil.Process(self._processor.ident).kill()

    def __start_new_processor(self):
        self._processor = Processor(
            event_loop=self._event_loop,
            send_queue=self._receive_queue,
            receive_queue=self._send_queue,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            serializer=self._serializer,
        )
        self._processor.start()
        self._heartbeat.set_processor_pid(self._processor.pid)
        self._current_task_id = None

        if self._lock.locked():
            self._lock.release()

    async def __on_receive_internal(self, message: MessageVariant):
        if isinstance(message, TaskResult):
            await self._function_cache.on_task_result(message)
            return

        raise TypeError(f"Unknown {message=}")

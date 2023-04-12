import logging
import multiprocessing
import pickle
import time
from typing import Callable, Dict, Optional

import tblib.pickling_support

from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    MessageVariant,
    Task,
    TaskResult,
    TaskStatus,
)
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.worker.agent.processor.memory_cleaner import MemoryCleaner


class Processor(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        event_loop: str,
        send_queue: multiprocessing.Queue,
        receive_queue: multiprocessing.Queue,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: FunctionSerializerType,
    ):
        multiprocessing.Process.__init__(self, name="Processor")

        self._event_loop = event_loop

        self._send_queue = send_queue
        self._receive_queue = receive_queue

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer

        self._memory_cleaner: Optional[MemoryCleaner] = None
        self._cached_functions: Dict[bytes, Callable] = {}

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        tblib.pickling_support.install()

        self._memory_cleaner = MemoryCleaner(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._memory_cleaner.start()

    def __run_forever(self):
        while True:
            message = self._receive_queue.get()
            self.__on_connector_receive(message)

    def __on_connector_receive(self, message: MessageVariant):
        if isinstance(message, FunctionRequest):
            self.__on_receive_function_request(message)
            return

        if isinstance(message, Task):
            self.__on_received_task(message)
            return

        logging.error(f"unknown {message=}")

    def __on_receive_function_request(self, request: FunctionRequest):
        if request.type == FunctionRequestType.Add:
            self._cached_functions[request.function_id] = self._serializer.deserialize_function(request.content)
            return

        if request.type == FunctionRequestType.Delete:
            self._cached_functions.pop(request.function_id)
            return

        logging.error(f"unknown request function request type {request=}")

    def __on_received_task(self, task: Task):
        begin = time.monotonic()
        try:
            function = self._cached_functions[task.function_id]
            args = self._serializer.deserialize_arguments(tuple(arg.data for arg in task.function_args))
            result = function(*args)
            result_bytes = self._serializer.serialize_result(result)
            self._send_queue.put(TaskResult(task.task_id, TaskStatus.Success, time.monotonic() - begin, result_bytes))

        except Exception as e:
            logging.exception(f"exception when processing task_id={task.task_id.hex()}:")
            self._send_queue.put(
                TaskResult(
                    task.task_id,
                    TaskStatus.Failed,
                    time.monotonic() - begin,
                    pickle.dumps(e, protocol=pickle.HIGHEST_PROTOCOL),
                )
            )

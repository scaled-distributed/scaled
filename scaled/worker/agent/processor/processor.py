import logging
import multiprocessing
import pickle
import signal
import threading
import time
from typing import Optional

import tblib.pickling_support
import zmq

from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    MessageType,
    MessageVariant,
    ProcessorInitialize, Task,
    TaskResult,
    TaskStatus,
)
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.processor.cache_cleaner import CacheCleaner


class Processor(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        function_retention_seconds: int,
        serializer: FunctionSerializerType,
    ):
        multiprocessing.Process.__init__(self, name="Processor")

        self._event_loop = event_loop
        self._address = address

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._function_retention_seconds = function_retention_seconds
        self._serializer = serializer

        self._cache_cleaner: Optional[CacheCleaner] = None
        self._onhold_task: Optional[Task] = None
        self._initialized: bool = False

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        tblib.pickling_support.install()

        self._connector = SyncConnector(
            stop_event=threading.Event(),
            prefix="IP",
            context=zmq.Context(),
            socket_type=zmq.PAIR,
            bind_or_connect="connect",
            address=self._address,
            callback=self.__on_connector_receive,
            exit_callback=None,
            daemonic=False,
        )

        self._cache_cleaner = CacheCleaner(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            function_retention_seconds=self._function_retention_seconds,
        )
        self._cache_cleaner.start()

    def __run_forever(self):
        try:
            self._connector.send_immediately(MessageType.ProcessorInitialize, ProcessorInitialize())
            self._connector.run()
        except KeyboardInterrupt:
            pass

    def __on_connector_receive(self, message_type: MessageType, message: MessageVariant):
        if not self._initialized:
            if message_type == MessageType.ProcessorInitialize:
                self._initialized = True
            return

        if message_type == MessageType.FunctionRequest:
            self.__on_receive_function_request(message)
            return

        if message_type == MessageType.Task:
            self.__on_received_task(message)
            return

        logging.error(f"unknown {message=}")

    def __on_receive_function_request(self, request: FunctionRequest):
        if request.type == FunctionRequestType.Add:
            self._cache_cleaner.add_function(
                request.function_id, self._serializer.deserialize_function(request.content)
            )
            task = self._onhold_task
            self._onhold_task = None
            self.__process_task(task)
            return

        if request.type == FunctionRequestType.Delete:
            self._cache_cleaner.del_function(request.function_id)
            return

        logging.error(f"unknown request function request type {request=}")

    def __on_received_task(self, task: Task):
        function = self._cache_cleaner.get_function(task.function_id)
        if function is None:
            assert self._onhold_task is None
            self._onhold_task = task
            self._connector.send_immediately(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
            )
            return

        self.__process_task(task)

    def __process_task(self, task: Task):
        begin = time.monotonic()
        try:
            function = self._cache_cleaner.get_function(task.function_id)
            args = self._serializer.deserialize_arguments(tuple(arg.data for arg in task.function_args))
            result = function(*args)
            result_bytes = self._serializer.serialize_result(result)
            self._connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(task.task_id, TaskStatus.Success, time.monotonic() - begin, result_bytes),
            )

        except Exception as e:
            logging.exception(f"exception when processing task_id={task.task_id.hex()}:")
            self._connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(
                    task.task_id,
                    TaskStatus.Failed,
                    time.monotonic() - begin,
                    pickle.dumps(e, protocol=pickle.HIGHEST_PROTOCOL),
                ),
            )

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__destroy)
        signal.signal(signal.SIGTERM, self.__destroy)

    def __destroy(self):
        self._connector.close()

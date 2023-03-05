import logging
import multiprocessing
import pickle
import signal
import time
from typing import Callable, Dict, Optional

import tblib.pickling_support
import zmq
import zmq.asyncio

from scaled.io.config import DEFAULT_WORKER_PROCESSING_QUEUE_SIZE
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    MessageType,
    MessageVariant,
    Task,
    TaskResult,
    TaskStatus,
)
from scaled.worker.agent.agent_thread import AgentThread
from scaled.worker.memory_cleaner import MemoryCleaner


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        index: int,
        address: ZMQConfig,
        stop_event: multiprocessing.Event,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        processing_queue_size: int,
        event_loop: str,
        serializer: FunctionSerializerType,
    ):
        multiprocessing.Process.__init__(self, name="Worker")

        self._index = index
        self._address = address
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._function_retention_seconds = function_retention_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._processing_queue_size = processing_queue_size
        self._event_loop = event_loop
        self._stop_event = stop_event
        self._serializer = serializer

        self._agent: Optional[AgentThread] = None
        self._internal_connector: Optional[SyncConnector] = None
        self._cleaner: Optional[MemoryCleaner] = None
        self._ready_event = multiprocessing.get_context("spawn").Event()

        self._cached_functions: Dict[bytes, Callable] = {}

    def wait_till_ready(self):
        while not self._ready_event.is_set():
            continue

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def shutdown(self, *args):
        assert args is not None
        self._stop_event.set()
        self._agent.terminate()

    def __initialize(self):
        self.__register_signal()
        tblib.pickling_support.install()

        context = zmq.Context()
        internal_address = ZMQConfig(type=ZMQType.inproc, host="memory")

        self._internal_connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="AW",
            context=context,
            socket_type=zmq.PAIR,
            bind_or_connect="connect",
            address=internal_address,
            callback=self.__on_connector_receive,
            daemonic=False,
            receive_high_watermark=self._processing_queue_size,
        )

        self._agent = AgentThread(
            external_address=self._address,
            internal_context=zmq.asyncio.Context.shadow(context.underlying),
            internal_address=internal_address,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            function_retention_seconds=self._function_retention_seconds,
            per_worker_processing_queue_size=self._processing_queue_size,
            event_loop=self._event_loop,
        )
        self._agent.start()

        self._cleaner = MemoryCleaner(
            stop_event=self._stop_event,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._cleaner.start()

        # worker is ready
        self._ready_event.set()

    def __run_forever(self):
        self._internal_connector.run()
        self.shutdown()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __on_connector_receive(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.FunctionRequest:
            self.__on_receive_function_request(message)
            return

        if message_type == MessageType.Task:
            self.__on_received_task(message)
            return

        raise TypeError(f"unknown {message_type=}")

    def __on_receive_function_request(self, request: FunctionRequest):
        if request.type == FunctionRequestType.Add:
            self._cached_functions[request.function_id] = self._serializer.deserialize_function(request.content)
            return

        if request.type == FunctionRequestType.Delete:
            self._cached_functions.pop(request.function_id)
            return

        raise TypeError(f"unknown request {request=}")

    def __on_received_task(self, task: Task):
        begin = time.monotonic()
        try:
            function = self._cached_functions[task.function_id]
            args, kwargs = self._serializer.deserialize_arguments(task.function_args)

            result = function(*args, **kwargs)

            result_bytes = self._serializer.serialize_result(result)
            self._internal_connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(task.task_id, TaskStatus.Success, time.monotonic() - begin, result_bytes),
            )

        except Exception as e:
            logging.exception(f"exception when processing task_id={task.task_id.hex()}:")
            self._internal_connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(
                    task.task_id,
                    TaskStatus.Failed,
                    time.monotonic() - begin,
                    pickle.dumps(e, protocol=pickle.HIGHEST_PROTOCOL),
                ),
            )

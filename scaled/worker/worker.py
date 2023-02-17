import logging
import multiprocessing
import queue
import signal
import time
from queue import Queue
from typing import Callable, Dict, Optional

import zmq
import zmq.asyncio

from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import TaskResult, TaskStatus
from scaled.worker.agent.agent_sync import AgentSync
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
        self._event_loop = event_loop
        self._stop_event = stop_event
        self._serializer = serializer

        self._receive_task_queue: Optional[Queue] = None
        self._send_task_queue: Optional[Queue] = None

        self._agent: Optional[AgentSync] = None
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
        self._agent.join()

    def __initialize(self):
        self.__register_signal()

        self._receive_task_queue = Queue()
        self._send_task_queue = Queue()

        context = zmq.Context.instance()
        self._agent = AgentSync(
            stop_event=self._stop_event,
            context=zmq.asyncio.Context.shadow(context.underlying),
            address=self._address,
            receive_task_queue=self._receive_task_queue,
            send_task_queue=self._send_task_queue,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            function_retention_seconds=self._function_retention_seconds,
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
        while not self._stop_event.is_set():
            self.__process_queued_tasks()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __process_queued_tasks(self):
        try:
            task = self._receive_task_queue.get(timeout=1)
        except queue.Empty:
            return

        begin = time.monotonic()
        try:
            if task.function_id not in self._cached_functions:
                self._cached_functions[task.function_id] = self._serializer.deserialize_function(task.function_content)

            function = self._cached_functions[task.function_id]
            args, kwargs = self._serializer.deserialize_arguments(task.function_args)

            result = function(*args, **kwargs)

            result_bytes = self._serializer.serialize_result(result)
            self._send_task_queue.put(
                TaskResult(task.task_id, TaskStatus.Success, time.monotonic() - begin, result_bytes)
            )

        except Exception as e:
            logging.exception(f"error when processing {task=}:")
            self._send_task_queue.put(
                TaskResult(task.task_id, TaskStatus.Failed, time.monotonic() - begin, str(e).encode())
            )

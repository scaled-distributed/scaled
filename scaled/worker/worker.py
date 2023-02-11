import asyncio
import logging
import multiprocessing
import signal
import threading
import time
from queue import Queue
from collections import defaultdict
from typing import Callable, Dict, List, Optional

import zmq
import zmq.asyncio

from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.protocol.python.message import (
    FunctionRequestType,
    FunctionRequest,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
    MessageVariant,
    Task,
    TaskCancel,
    TaskResult,
    TaskStatus,
)
from scaled.worker.async_agent import AsyncAgent


class Agent(threading.Thread):
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.Context,
        address: ZMQConfig,
        address_internal: ZMQConfig,
        heartbeat_interval_seconds: int,
        event_loop: str,
    ):
        threading.Thread.__init__(self)

        self._stop_event: threading.Event = stop_event
        self._address: ZMQConfig = address
        self._address_internal: ZMQConfig = address_internal

        self._heartbeat_interval_seconds: int = heartbeat_interval_seconds

        self._event_loop = event_loop

        self._agent = AsyncAgent(
            stop_event=self._stop_event,
            context=context,
            address=self._address,
            address_internal=self._address_internal,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )

    def run(self) -> None:
        register_event_loop(self._event_loop)
        asyncio.run(self._agent.loop())


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: multiprocessing.Event,
        heartbeat_interval_seconds: int,
        event_loop: str,
        serializer: FunctionSerializerType,
    ):
        multiprocessing.Process.__init__(self, name="Worker")

        self._address = address
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._event_loop = event_loop
        self._stop_event = stop_event
        self._serializer = serializer

        self._task_queue: Optional[Queue] = None
        self._agent_connector: Optional[SyncConnector] = None
        self._agent: Optional[Agent] = None
        self._ready_event = multiprocessing.get_context("spawn").Event()

        self._cached_functions: Dict[bytes, Callable] = {}
        self._pending_function_to_tasks: Dict[bytes, List[Task]] = defaultdict(list)

    def wait_till_ready(self):
        while not self._ready_event.is_set():
            continue

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def shutdown(self, *args):
        assert args is not None
        self._agent_connector.join()
        self._agent.join()

    def get_prefix(self):
        return f"{self.__class__.__name__}[{self._agent_connector.identity.decode()}]:"

    def __initialize(self):
        self.__register_signal()

        self._task_queue = Queue()

        context = zmq.Context.instance()
        internal_channel = ZMQConfig(type=ZMQType.inproc, host="memory")
        self._agent_connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="A",
            context=context,
            socket_type=zmq.PAIR,
            bind_or_connect="connect",
            address=internal_channel,
            callback=self.__on_connector_receive,
        )
        self._agent = Agent(
            stop_event=self._stop_event,
            context=zmq.asyncio.Context.shadow(context.underlying),
            address=self._address,
            address_internal=internal_channel,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            event_loop=self._event_loop,
        )
        self._agent.start()

        # worker is ready
        self._ready_event.set()

    def __run_forever(self):
        while not self._stop_event.is_set():
            self.__process_queued_tasks()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __process_queued_tasks(self):
        if self._task_queue.empty():
            time.sleep(0.1)
            return

        task = self._task_queue.get()
        if task.function_id not in self._cached_functions:
            self._agent_connector.send(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
            )
            self._pending_function_to_tasks[task.function_id].append(task)
            return

        try:
            function = self._cached_functions[task.function_id]
            if function is None:
                raise ValueError(f"{self.get_prefix()}cannot get function for {task=}")

            args = self._serializer.deserialize_arguments(task.function_args)
            result = self._serializer.serialize_result(function(*args))
            self._agent_connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Success, result))
        except Exception as e:
            logging.exception(f"{self.get_prefix()} error when processing {task=}:")
            self._agent_connector.send(
                MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Failed, str(e).encode())
            )

    def __on_connector_receive(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.FunctionResponse:
            self.__connector_process_function_response(message)
            return

        if message_type == MessageType.Task:
            self.__connector_process_task(message)
            return

        if message_type == MessageType.TaskCancel:
            self.__connector_process_task_cancel(message)
            return

        logging.error(f"{self.get_prefix()} unsupported {message_type=} {message=}")

    def __connector_process_function_response(self, function_response: FunctionResponse):
        if function_response.status == FunctionResponseType.NotExists:
            return

        assert function_response.status == FunctionResponseType.OK
        self._cached_functions[function_response.function_id] = self._serializer.deserialize_function(
            function_response.content
        )

        if function_response.function_id not in self._pending_function_to_tasks:
            return

        for task in self._pending_function_to_tasks.pop(function_response.function_id):
            self.__connector_process_task(task)

    def __connector_process_task(self, task: Task):
        self._task_queue.put(task)

    def __connector_process_task_cancel(self, task_cancel: TaskCancel):
        # TODO: implement this
        raise NotImplementedError()

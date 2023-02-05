import asyncio
import logging
import multiprocessing
import signal
import threading
import time
from collections import defaultdict, deque
from typing import Callable, Deque, Dict, List

import uvloop
import zmq
import zmq.asyncio

from scaled.io.sync_connector import SyncConnector
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
from scaled.utility.logging.utility import setup_logger
from scaled.worker.agent_runner import AgentRunner
from scaled.protocol.python.function import FunctionSerializer


class Agent(threading.Thread):
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.Context,
        address: ZMQConfig,
        address_internal: ZMQConfig,
        heartbeat_interval_seconds: int,
    ):
        threading.Thread.__init__(self)

        self._stop_event: threading.Event = stop_event
        self._address: ZMQConfig = address
        self._address_internal: ZMQConfig = address_internal

        self._heartbeat_interval_seconds: int = heartbeat_interval_seconds

        self._agent = AgentRunner(
            stop_event=self._stop_event,
            context=context,
            address=self._address,
            address_internal=self._address_internal,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )

    def run(self) -> None:
        uvloop.install()
        asyncio.run(self._agent.loop())


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(self, address: ZMQConfig, stop_event: multiprocessing.Event, heartbeat_interval_seconds: int):
        multiprocessing.Process.__init__(self, name="Worker")

        self._address = address
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self._stop_event = stop_event
        self._agent_connector = None
        self._agent = None

        self._cached_functions: Dict[bytes, Callable] = {}
        self._task_queue: Deque[Task] = deque()
        self._pending_function_to_tasks: Dict[bytes, List[Task]] = defaultdict(list)

    def run(self) -> None:
        setup_logger()
        self.__initialize()
        self.__run_forever()

    def shutdown(self, *args):
        assert args is not None
        self._agent_connector.join()
        self._agent.join()
        logging.info(f"{self.__get_prefix()} quited")

    def __initialize(self):
        self.__register_signal()

        context = zmq.Context.instance()
        internal_channel = ZMQConfig(type=ZMQType.inproc, host="memory")
        self._agent_connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="A",
            context=context,
            socket_type=zmq.PAIR,
            bind_or_connect="bind",
            address=internal_channel,
            callback=self.__on_connector_receive,
        )
        self._agent = Agent(
            stop_event=self._stop_event,
            context=zmq.asyncio.Context.shadow(context.underlying),
            address=self._address,
            address_internal=internal_channel,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )
        self._agent.start()

    def __run_forever(self):
        logging.info(f"{self.__get_prefix()} started")
        while not self._stop_event.is_set():
            self.__process_queued_tasks()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __process_queued_tasks(self):
        if not self._task_queue:
            time.sleep(0.1)
            return

        task = self._task_queue.popleft()
        if task.function_id not in self._cached_functions:
            self._agent_connector.send(
                MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Request, task.function_id, b"")
            )
            self._pending_function_to_tasks[task.function_id].append(task)
            return

        try:
            function = self._cached_functions[task.function_id]
            if function is None:
                raise ValueError(f"cannot get function for {task=}")

            args = FunctionSerializer.deserialize_arguments(task.function_args)
            result = FunctionSerializer.serialize_result(function(*args))
            self._agent_connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Success, result))
        except Exception as e:
            logging.exception(f"{self.__get_prefix()} error when processing {task=}:")
            self._agent_connector.send(
                MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Failed, str(e).encode())
            )

    def __on_connector_receive(self, message_type: MessageType, message: MessageVariant):
        match message_type:
            case MessageType.FunctionResponse:
                self.__connector_process_function_response(message)
            case MessageType.Task:
                self.__connector_process_task(message)
            case MessageType.TaskCancel:
                self.__connector_process_task_cancel(message)
            case _:
                logging.exception(f"{self.__get_prefix()} unsupported {message_type=} {message=}")

    def __connector_process_function_response(self, function_response: FunctionResponse):
        if function_response.status == FunctionResponseType.NotExists:
            return

        assert function_response.status == FunctionResponseType.OK
        self._cached_functions[function_response.function_id] = FunctionSerializer.deserialize_function(
            function_response.content
        )

        if function_response.function_id not in self._pending_function_to_tasks:
            return

        self._task_queue.extend(self._pending_function_to_tasks.pop(function_response.function_id))

    def __connector_process_task(self, task: Task):
        self._task_queue.append(task)

    def __connector_process_task_cancel(self, task_cancel: TaskCancel):
        # TODO: implement this
        raise NotImplementedError()

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._agent_connector.identity.decode()}]:"

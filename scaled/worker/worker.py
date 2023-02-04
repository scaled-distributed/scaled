import asyncio
import logging
import multiprocessing
import signal
import threading
import time
from typing import Any, Callable, Dict

import uvloop
import zmq
import zmq.asyncio

from scaled.io.sync_connector import SyncConnector
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.protocol.python.message import MessageVariant, Task, TaskCancel, TaskResult
from scaled.protocol.python.objects import MessageType, TaskStatus
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

        self._stop_event = stop_event
        self._address = address
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self._thread_stop_event = None
        self._agent = None

        self._cached_functions: Dict[bytes, Any] = {}

    def run(self) -> None:
        setup_logger()
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        self.__register_signal()
        self._thread_stop_event = threading.Event()

        context = zmq.Context.instance()
        async_context = zmq.asyncio.Context.shadow(context.underlying)
        address_in_memory = ZMQConfig(type=ZMQType.inproc, host="memory")
        self._agent_connector = SyncConnector(
            stop_event=self._thread_stop_event,
            prefix="A",
            context=context,
            socket_type=zmq.PAIR,
            bind_or_connect="bind",
            address=address_in_memory,
            callback=self.__on_receive,
        )
        self._agent = Agent(
            stop_event=self._thread_stop_event,
            context=async_context,
            address=self._address,
            address_internal=address_in_memory,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )
        self._agent.start()

    def __run_forever(self):
        logging.info(f"{self.__get_prefix()} started")
        while not self._stop_event.is_set():
            time.sleep(0.01)

        self.__shutdown()

    def __shutdown(self, *args):
        assert args is not None
        self._thread_stop_event.set()
        self._agent_connector.join()
        self._agent.join()
        logging.info(f"{self.__get_prefix()} quited")

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__shutdown)
        signal.signal(signal.SIGTERM, self.__shutdown)

    def __on_receive(self, message_type: MessageType, message: MessageVariant):
        match message:
            case Task():
                self.__process_task(message)
            case TaskCancel():
                self.__process_task_cancel(message)
            case _:
                logging.exception(f"{self.__get_prefix()} unsupported {message_type=} {message=}")

    def __process_task(self, task: Task):
        # noinspection PyBroadException
        try:
            function = self.__get_function(task.function_id)
            result = FunctionSerializer.serialize_result(
                function(*FunctionSerializer.deserialize_arguments(task.function_args))
            )
            self._agent_connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Success, result))
        except Exception as e:
            logging.exception(f"{self.__get_prefix()} error when processing {task=}:")
            self._agent_connector.send(
                MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Failed, str(e).encode())
            )

    def __process_task_cancel(self, task: Task):
        # TODO: implement this
        pass

    def __get_prefix(self):
        return f"Worker[{self._agent_connector.identity.decode()}]:"

    def __get_function(self, function_name: bytes) -> Callable:
        if function_name in self._cached_functions:
            return self._cached_functions[function_name]

        app = FunctionSerializer.deserialize_function(function_name)
        self._cached_functions[function_name] = app
        return app

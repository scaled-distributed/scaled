import hashlib
import threading
import time
import uuid
from concurrent.futures import Future

from typing import Callable, Dict, Optional

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.function import FunctionSerializer
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
    MessageVariant,
    MonitorRequest,
    MonitorResponse,
    Task,
    TaskEcho,
    TaskResult,
)


class Client:
    def __init__(self, config: ZMQConfig):
        self._stop_event = threading.Event()
        self._connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="C",
            context=zmq.Context.instance(),
            socket_type=zmq.DEALER,
            bind_or_connect="connect",
            address=config,
            callback=self.__on_receive,
        )

        self._futures: Dict[bytes, Future] = dict()
        self._function_id_usage_count: Dict[bytes, int] = dict()
        self._statistics_future: Optional[Future] = None

    def __del__(self):
        self.disconnect()

    def submit(self, fn: Callable, *args) -> Future:
        function_id = self.__get_function_id(fn)

        task_id = uuid.uuid1().bytes
        task = Task(task_id, function_id, FunctionSerializer.serialize_arguments(args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        self._futures[task_id] = future
        return future

    def statistics(self):
        self._statistics_future = Future()
        self._connector.send(MessageType.MonitorRequest, MonitorRequest(b""))
        return self._statistics_future.result()

    def disconnect(self):
        self._stop_event.set()

    def __get_function_id(self, fn: Callable) -> bytes:
        function_bytes = FunctionSerializer.serialize_function(fn)
        function_id = hashlib.md5(function_bytes).hexdigest().encode()

        if function_id in self._function_id_usage_count:
            return function_id

        self._connector.send(
            MessageType.FunctionRequest, FunctionRequest(FunctionRequestType.Add, function_id, function_bytes)
        )

        while function_id not in self._function_id_usage_count:
            time.sleep(0.1)

        return function_id

    def __on_receive(self, message_type: MessageType, message: MessageVariant):
        match message_type:
            case MessageType.FunctionResponse:
                self.__on_function_response(message)
            case MessageType.TaskEcho:
                self.__on_task_echo(message)
            case MessageType.TaskResult:
                self.__on_task_result(message)
            case MessageType.MonitorResponse:
                self.__on_statistics_response(message)
            case _:
                raise TypeError(f"Unknown {message_type=}")

    def __on_function_response(self, response: FunctionResponse):
        if response.status == FunctionResponseType.Duplicated:
            return

        assert response.status == FunctionResponseType.OK
        if response.function_id in self._function_id_usage_count:
            return

        self._function_id_usage_count[response.function_id] = 0

    def __on_task_echo(self, task_echo: TaskEcho):
        if task_echo.task_id not in self._futures:
            return
        future = self._futures[task_echo.task_id]
        future.set_running_or_notify_cancel()

    def __on_task_result(self, result: TaskResult):
        if result.task_id not in self._futures:
            return

        future = self._futures.pop(result.task_id)
        future.set_result(FunctionSerializer.deserialize_result(result.result))

    def __on_statistics_response(self, monitor_response: MonitorResponse):
        self._statistics_future.set_result(monitor_response.data)

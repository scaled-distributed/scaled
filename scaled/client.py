import hashlib
import json
import logging
import pickle
import threading
import uuid
from collections import defaultdict
from concurrent.futures import Future

from typing import Callable, Dict, List, Optional, Tuple

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.protocol.python.serializer.default import DefaultSerializer
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
    TaskEchoStatus,
    TaskResult,
    TaskStatus,
)


class Client:
    def __init__(self, address: str, serializer: FunctionSerializerType = DefaultSerializer()):
        self._address = address
        self._serializer = serializer

        self._stop_event = threading.Event()
        self._connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="C",
            context=zmq.Context.instance(),
            socket_type=zmq.DEALER,
            bind_or_connect="connect",
            address=ZMQConfig.from_string(address),
            callback=self.__on_receive,
            daemonic=True,
        )
        self._connector.start()
        logging.info(f"ScaledClient: connect to {address}")

        self._function_to_function_id_cache: Dict[Callable, Tuple[bytes, bytes]] = dict()

        self._task_id_to_task_function: Dict[bytes, Tuple[Task, bytes]] = dict()
        self._task_id_to_future: Dict[bytes, Future] = dict()

        self._function_id_to_not_ready_tasks: Dict[bytes, List[Task]] = defaultdict(list)
        self._statistics_future: Optional[Future] = None

    def __del__(self):
        logging.info(f"ScaledClient: disconnect from {self._address}")
        self.disconnect()

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        function_id, function_bytes = self.__get_function_id(fn)

        task_id = uuid.uuid1().bytes
        task = Task(task_id, function_id, self._serializer.serialize_arguments(args, kwargs))
        self._task_id_to_task_function[task_id] = (task, function_bytes)

        self.__on_buffer_task_send(task)

        future = Future()
        self._task_id_to_future[task_id] = future
        return future

    def scheduler_status(self):
        self._statistics_future = Future()
        self._connector.send(MessageType.MonitorRequest, MonitorRequest())
        statistics = json.loads(self._statistics_future.result())
        return statistics

    def disconnect(self):
        self._stop_event.set()

    def __on_receive(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.TaskEcho:
            self.__on_task_echo(message)
            return

        if message_type == MessageType.FunctionResponse:
            self.__on_function_response(message)
            return

        if message_type == MessageType.TaskResult:
            self.__on_task_result(message)
            return

        if message_type == MessageType.MonitorResponse:
            self.__on_statistics_response(message)
            return

        raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, task_echo: TaskEcho):
        if task_echo.task_id not in self._task_id_to_future:
            return

        if task_echo.status == TaskEchoStatus.Duplicated:
            return

        if task_echo.status == TaskEchoStatus.FunctionNotExists:
            task, _ = self._task_id_to_task_function[task_echo.task_id]
            self.__on_buffer_task_send(task)
            return

        if task_echo.status == TaskEchoStatus.NoWorker:
            raise NotImplementedError(f"please implement that handles no worker error")

        assert task_echo.status == TaskEchoStatus.SubmitOK, f"Unknown task status: " f"{task_echo=}"

        future = self._task_id_to_future[task_echo.task_id]
        future.set_running_or_notify_cancel()

    def __on_buffer_task_send(self, task):
        if task.function_id not in self._function_id_to_not_ready_tasks:
            _, function_bytes = self._task_id_to_task_function[task.task_id]
            self._connector.send(
                MessageType.FunctionRequest,
                FunctionRequest(FunctionRequestType.Add, function_id=task.function_id, content=function_bytes),
            )

        self._function_id_to_not_ready_tasks[task.function_id].append(task)

    def __on_function_response(self, response: FunctionResponse):
        assert response.status in {FunctionResponseType.OK, FunctionResponseType.Duplicated}
        if response.function_id not in self._function_id_to_not_ready_tasks:
            return

        for task in self._function_id_to_not_ready_tasks.pop(response.function_id):
            self._connector.send(MessageType.Task, task)

    def __on_task_result(self, result: TaskResult):
        if result.task_id not in self._task_id_to_future:
            return

        self._task_id_to_task_function.pop(result.task_id)
        future = self._task_id_to_future.pop(result.task_id)

        if result.status == TaskStatus.Success:
            future.set_result(self._serializer.deserialize_result(result.result))
            return

        if result.status == TaskStatus.Failed:
            future.set_exception(pickle.loads(result.result))
            return

        if result.status == TaskStatus.Canceled:
            return

    def __on_statistics_response(self, monitor_response: MonitorResponse):
        self._statistics_future.set_result(monitor_response.data)

    def __get_function_id(self, fn: Callable) -> Tuple[bytes, bytes]:
        if fn in self._function_to_function_id_cache:
            return self._function_to_function_id_cache[fn]

        function_id, function_bytes = self.__generate_function_id_bytes(fn)
        self._function_to_function_id_cache[fn] = (function_id, function_bytes)
        return function_id, function_bytes

    def __generate_function_id_bytes(self, fn) -> Tuple[bytes, bytes]:
        function_bytes = self._serializer.serialize_function(fn)
        function_id = hashlib.md5(function_bytes).digest()
        return function_id, function_bytes

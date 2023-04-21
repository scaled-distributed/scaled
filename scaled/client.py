import hashlib
import logging
import pickle
import threading
import uuid
from collections import defaultdict
from concurrent.futures import Future
from inspect import signature

from typing import Any, Callable, Dict, List, Optional, Tuple

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.message import (
    Argument,
    ArgumentType,
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    MessageType,
    MessageVariant,
    Task,
    TaskCancel,
    TaskEcho,
    TaskEchoStatus,
    TaskResult,
    TaskStatus,
)


class ScaledDisconnect(Exception):
    pass


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
            exit_callback=self.__on_exit,
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
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task = Task(
            task_id,
            function_id,
            [Argument(ArgumentType.Data, data) for data in self._serializer.serialize_arguments(all_args)],
        )
        self._task_id_to_task_function[task_id] = (task, function_bytes)

        self.__on_buffer_task_send(task)

        future = Future()
        self._task_id_to_future[task_id] = future
        return future

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

        raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, task_echo: TaskEcho):
        if task_echo.task_id not in self._task_id_to_task_function:
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

        self._task_id_to_task_function.pop(task_echo.task_id)
        self._task_id_to_future[task_echo.task_id].set_running_or_notify_cancel()

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

        future = self._task_id_to_future.pop(result.task_id)
        if result.status == TaskStatus.Success:
            future.set_result(self._serializer.deserialize_result(result.result))
            return

        if result.status == TaskStatus.Failed:
            future.set_exception(pickle.loads(result.result))
            return

    def __on_exit(self):
        if not self._task_id_to_future:
            return

        logging.info(f"canceling {len(self._task_id_to_future)} tasks")
        for task_id, future in self._task_id_to_future.items():
            self._connector.send_immediately(MessageType.TaskCancel, TaskCancel(task_id))
            future.set_exception(ScaledDisconnect(f"disconnected from {self._address}"))

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

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args) : number_of_required]:
            try:
                args.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args) :]:
            args.append(kwargs.pop(p.name, p.default))

        return tuple(args)

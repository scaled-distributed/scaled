import hashlib
import logging
import pickle
import threading
import uuid
from collections import defaultdict
from concurrent.futures import Future
from graphlib import TopologicalSorter
from inspect import signature
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple
from typing import Union

import zmq

from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.message import Argument
from scaled.protocol.python.message import ArgumentType
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionRequestType
from scaled.protocol.python.message import FunctionResponse
from scaled.protocol.python.message import FunctionResponseType
from scaled.protocol.python.message import GraphTask
from scaled.protocol.python.message import GraphTaskCancel
from scaled.protocol.python.message import GraphTaskResult
from scaled.protocol.python.message import MessageVariant
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskCancel
from scaled.protocol.python.message import TaskEcho
from scaled.protocol.python.message import TaskEchoStatus
from scaled.protocol.python.message import TaskResult
from scaled.protocol.python.message import TaskStatus
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.exceptions import ScaledDisconnect
from scaled.utility.zmq_config import ZMQConfig


class Client:
    def __init__(self, address: str, serializer: Serializer = DefaultSerializer()):
        self._address = address
        self._serializer = serializer

        self._stop_event = threading.Event()
        self._connector = SyncConnector(
            stop_event=self._stop_event,
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

        self._function_to_function_id_cache: Dict[Callable, Tuple[bytes, Tuple[bytes, bytes]]] = dict()

        self._task_id_to_task: Dict[bytes, Task] = dict()
        self._task_id_to_function: Dict[bytes, Tuple[bytes, bytes]] = dict()
        self._task_id_to_future: Dict[bytes, Future] = dict()

        self._function_id_to_not_ready_tasks: Dict[bytes, List[Task]] = defaultdict(list)

        self._graph_task_id_to_futures: Dict[bytes, List[Future]] = dict()

    def __del__(self):
        logging.info(f"ScaledClient: disconnect from {self._address}")
        self.disconnect()

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        function_id, (function_name, function_bytes) = self.__get_function_id(fn)

        task_id = uuid.uuid1().bytes
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task = Task(
            task_id,
            function_id,
            [Argument(ArgumentType.Data, self._serializer.serialize_argument(data)) for data in all_args],
        )
        self._task_id_to_task[task_id] = task
        self._task_id_to_function[task_id] = (function_name, function_bytes)

        self.__on_buffer_task_send(task)

        future: Future = Future()
        self._task_id_to_future[task_id] = future
        return future

    def map(self, fn: Callable, iterable: Iterable[Tuple[Any, ...]]) -> List[Any]:
        if not all(isinstance(args, (tuple, list)) for args in iterable):
            raise TypeError("iterable should be list of arguments(list or tuple-like) of function")

        futures = [self.submit(fn, *args) for args in iterable]

        try:
            results = [fut.result() for fut in futures]
        except Exception as e:
            self.disconnect()
            self._connector.join()
            raise e

        return results

    def get(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]], keys: List[str], block: bool = True
    ) -> Dict[str, Union[Any, Future]]:
        """
        graph = {
            "a": 1,
            "b": 2,
            "c": (inc, "a"),
            "d": (inc, "b"),
            "d": (add, "c", "d")
        }
        """

        node_name_to_data_argument, graph = self.__split_data_and_graph(graph)
        self.__check_graph(node_name_to_data_argument, graph, keys)

        graph_task, futures = self.__construct_graph(node_name_to_data_argument, graph, keys)
        self._connector.send(graph_task)
        self._graph_task_id_to_futures[graph_task.task_id] = futures

        return_futures: Dict[str, Future] = dict(zip(keys, futures))
        if not block:
            # just return futures
            return return_futures

        try:
            results = {k: v.result() for k, v in return_futures.items()}
        except Exception as e:
            self.disconnect()
            self._connector.join()
            raise e

        return results

    def disconnect(self):
        self._stop_event.set()

    def __on_receive(self, message: MessageVariant):
        if isinstance(message, TaskEcho):
            self.__on_task_echo(message)
            return

        if isinstance(message, FunctionResponse):
            self.__on_function_response(message)
            return

        if isinstance(message, TaskResult):
            self.__on_task_result(message)
            return

        if isinstance(message, GraphTaskResult):
            self.__on_graph_task_result(message)
            return

        raise TypeError(f"Unknown {message=}")

    def __on_task_echo(self, task_echo: TaskEcho):
        if task_echo.task_id not in self._task_id_to_task:
            return

        if task_echo.status == TaskEchoStatus.Duplicated:
            return

        if task_echo.status == TaskEchoStatus.FunctionNotExists:
            task = self._task_id_to_task[task_echo.task_id]
            self.__on_buffer_task_send(task)
            return

        if task_echo.status == TaskEchoStatus.NoWorker:
            raise NotImplementedError("please implement that handles no worker error")

        assert task_echo.status == TaskEchoStatus.SubmitOK, f"Unknown task status: " f"{task_echo=}"

        self._task_id_to_task.pop(task_echo.task_id)
        self._task_id_to_function.pop(task_echo.task_id)
        self._task_id_to_future[task_echo.task_id].set_running_or_notify_cancel()

    def __on_buffer_task_send(self, task):
        if task.function_id not in self._function_id_to_not_ready_tasks:
            function_name, function_bytes = self._task_id_to_function[task.task_id]
            self._connector.send(
                FunctionRequest(FunctionRequestType.Add, task.function_id, function_name, function_bytes)
            )

        self._function_id_to_not_ready_tasks[task.function_id].append(task)

    def __on_function_response(self, response: FunctionResponse):
        assert response.status in {FunctionResponseType.OK, FunctionResponseType.Duplicated}
        if response.function_id not in self._function_id_to_not_ready_tasks:
            return

        for task in self._function_id_to_not_ready_tasks.pop(response.function_id):
            self._connector.send(task)

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

    def __on_graph_task_result(self, graph_result: GraphTaskResult):
        if graph_result.task_id not in self._graph_task_id_to_futures:
            return

        futures = self._graph_task_id_to_futures.pop(graph_result.task_id)
        if graph_result.status == TaskStatus.Success:
            for i, result in enumerate(graph_result.results):
                futures[i].set_result(self._serializer.deserialize_result(result))
            return

        if graph_result.status == TaskStatus.Failed:
            exception = pickle.loads(graph_result.results[0])
            for i, result in enumerate(graph_result.results):
                futures[i].set_exception(exception)
            return

    def __on_exit(self):
        if self._task_id_to_future:
            logging.info(f"canceling {len(self._task_id_to_future)} task(s)")
            for task_id, future in self._task_id_to_future.items():
                self._connector.send_immediately(TaskCancel(task_id))
                future.set_exception(ScaledDisconnect(f"disconnected from {self._address}"))

        if self._graph_task_id_to_futures:
            logging.info(f"canceling {len(self._graph_task_id_to_futures)} graph task(s)")
            for task_id, futures in self._graph_task_id_to_futures.items():
                self._connector.send_immediately(GraphTaskCancel(task_id))
                for future in futures:
                    future.set_exception(ScaledDisconnect(f"disconnected from {self._address}"))

    def __get_function_id(self, fn: Callable) -> Tuple[bytes, Tuple[bytes, bytes]]:
        if fn in self._function_to_function_id_cache:
            return self._function_to_function_id_cache[fn]

        function_id, (function_name, function_bytes) = self.__generate_function_id_bytes(fn)
        self._function_to_function_id_cache[fn] = function_id, (function_name, function_bytes)
        return function_id, (function_name, function_bytes)

    def __generate_function_id_bytes(self, fn: Callable) -> Tuple[bytes, Tuple[bytes, bytes]]:
        function_bytes = self._serializer.serialize_function(fn)
        function_id = hashlib.md5(function_bytes).digest()
        function_name = str(getattr(fn, "__name__", "<anonymous func>")).encode()
        return function_id, (function_name, function_bytes)

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args_list = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args_list) : number_of_required]:
            try:
                args_list.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args_list) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args_list) :]:
            args_list.append(kwargs.pop(p.name, p.default))

        return tuple(args_list)

    def __split_data_and_graph(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]]
    ) -> Tuple[Dict[str, Argument], Dict[str, Tuple[Union[Callable, Any], ...]]]:
        graph = graph.copy()
        node_name_to_data_argument = {}
        for node_name, node in graph.items():
            if isinstance(node, tuple) and len(node) > 0 and callable(node[0]):
                continue

            node_name_to_data_argument[node_name] = Argument(
                ArgumentType.Data, self._serializer.serialize_argument(node)
            )

        for node_name in node_name_to_data_argument.keys():
            graph.pop(node_name)

        return node_name_to_data_argument, graph

    @staticmethod
    def __check_graph(
        node_to_argument: Dict[str, Argument],
        graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]],
        keys: List[str],
    ):
        # sanity check graph
        for key in keys:
            if key not in graph:
                raise KeyError(f"key {key} has to be in graph")

        sorter: TopologicalSorter = TopologicalSorter()
        for node_name, node in graph.items():
            assert isinstance(node, tuple) and len(node) > 0 and callable(node[0]), (
                "node has to be tuple and first " "item should be function"
            )

            for arg in node[1:]:
                if arg not in node_to_argument and arg not in graph:
                    raise KeyError(f"argument {arg} in node '{node_name}': {tuple(node)} is not defined in graph")

            sorter.add(node_name, *node[1:])

        # check cyclic dependencies
        sorter.prepare()

    def __construct_graph(
        self, data_arguments: Dict[str, Argument], graph: Dict[str, Tuple[Union[Callable, Any], ...]], keys: List[str]
    ) -> Tuple[GraphTask, List[Future]]:
        node_name_to_task_id = {node_name: uuid.uuid1().bytes for node_name in graph.keys()}
        task_id_to_future: Dict[bytes, Future] = {node_name_to_task_id[node_name]: Future() for node_name in keys}

        functions = {}
        tasks = []
        for node_name, node in graph.items():
            task_id = node_name_to_task_id[node_name]

            function, *args = node
            function_id, (function_name, function_bytes) = self.__get_function_id(function)
            functions[function_id] = (function_name, function_bytes)

            arguments = []
            for arg in args:
                assert arg in graph or arg in data_arguments
                assert isinstance(arg, str)
                if arg in graph:
                    argument = Argument(ArgumentType.Task, node_name_to_task_id[arg])
                else:
                    argument = data_arguments[arg]

                arguments.append(argument)

            tasks.append(Task(task_id, function_id, arguments))

        # send graph task
        graph_task = GraphTask(uuid.uuid1().bytes, functions, list(task_id_to_future.keys()), tasks)
        return graph_task, list(task_id_to_future.values())

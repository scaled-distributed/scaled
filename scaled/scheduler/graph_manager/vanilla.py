from asyncio import Queue
from graphlib import TopologicalSorter
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import (
    Argument,
    ArgumentType,
    FunctionRequest,
    FunctionRequestType,
    GraphTask,
    GraphTaskCancel,
    GraphTaskCancelEcho,
    GraphTaskResult,
    MessageType,
    Task,
    TaskCancel,
    TaskEchoStatus,
    TaskResult,
    TaskStatus,
)
from scaled.scheduler.mixins import FunctionManager, GraphTaskManager, Looper, Reporter, TaskManager


class GraphManager(GraphTaskManager, Looper, Reporter):
    """
    A = func()
    B = func2(A)
    C = func3(A)
    D = func4(B, C)

    graph
    A = Task(func)
    B = Task(func2, A)
    C = Task(func3, A)
    D = Task(func4, B, C)

    dependencies
    {"A": {B, C}
     "B": {D},
     "C": {D},
     "D": {},
    }
    """

    def __init__(self):
        self._binder: Optional[AsyncBinder] = None
        self._function_manager: Optional[FunctionManager] = None
        self._task_manager: Optional[TaskManager] = None

        self._unassigned: Queue = Queue()

        self._function_id_to_functions: Dict[bytes, bytes] = dict()
        self._task_id_to_function_id: Dict[bytes, bytes] = dict()
        self._function_id_to_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)

        self._graph_task_id_to_target_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)
        self._graph_task_id_to_sorter: Dict[bytes, TopologicalSorter] = dict()
        self._graph_task_id_to_client: Dict[bytes, bytes] = dict()
        self._graph_task_id_to_inactive: Dict[bytes, Set[bytes]] = defaultdict(set)
        self._graph_task_id_to_running: Dict[bytes, Set[bytes]] = defaultdict(set)

        self._task_id_to_dependencies: Dict[bytes, Set[bytes]] = defaultdict(set)

        self._task_id_to_task: Dict[bytes, Task] = dict()
        self._task_id_to_result: Dict[bytes, bytes] = dict()
        self._task_id_to_graph_id: Dict[bytes, bytes] = dict()

    def register(self, binder: AsyncBinder, function_manager: FunctionManager, task_manager: TaskManager):
        self._binder = binder
        self._function_manager = function_manager
        self._task_manager = task_manager

    async def on_graph_task(self, client: bytes, graph_task: GraphTask):
        await self._unassigned.put((client, graph_task))

    async def on_graph_task_cancel(self, graph_task_cancel: GraphTaskCancel):
        client, _ = await self.__clean_graph(graph_task_cancel.task_id)
        await self._binder.send(
            client,
            MessageType.GraphTaskCancelEcho,
            GraphTaskCancelEcho(graph_task_cancel.task_id, TaskEchoStatus.CancelOK),
        )

    async def on_task_done(self, result: TaskResult) -> bool:
        if result.task_id not in self._task_id_to_task:
            return False

        if result.status == TaskStatus.Success:
            await self.__finish_node_success(result)
            return True

        if result.status == TaskStatus.Failed:
            await self.__finish_node_fail(result)
            return True

    async def routine(self):
        client, graph_task = await self._unassigned.get()
        await self.__add_new_graph(client, graph_task)

    async def statistics(self) -> Dict:
        return {"graph_manager": {"unassigned": self._unassigned.qsize()}}

    async def __add_new_graph(self, client: bytes, graph_task: GraphTask):
        graph = {}

        self._function_id_to_functions.update(graph_task.functions)

        for task in graph_task.graph:
            self._function_id_to_task_ids[task.function_id].add(task.task_id)
            self._task_id_to_function_id[task.task_id] = task.function_id

            self._task_id_to_task[task.task_id] = task
            self._task_id_to_graph_id[task.task_id] = graph_task.task_id

            required_task_ids = {arg.data for arg in task.function_args if arg.type == ArgumentType.Task}
            for task_id in required_task_ids:
                self._task_id_to_dependencies[task_id].add(task.task_id)

            graph[task.task_id] = required_task_ids
            self._graph_task_id_to_inactive[graph_task.task_id].add(task.task_id)

        sorter = TopologicalSorter(graph)
        sorter.prepare()
        self._graph_task_id_to_sorter[graph_task.task_id] = sorter
        self._graph_task_id_to_target_task_ids[graph_task.task_id] = set(graph_task.targets)
        self._graph_task_id_to_client[graph_task.task_id] = client

        await self.__check_one_graph(graph_task.task_id)

    async def __check_one_graph(self, graph_task_id: bytes):
        sorter = self._graph_task_id_to_sorter[graph_task_id]

        if not sorter.is_active():
            await self.__finish_one_graph(graph_task_id)
            return

        ready_task_ids = sorter.get_ready()
        if not ready_task_ids:
            return

        client = self._graph_task_id_to_client[graph_task_id]
        for task_id in ready_task_ids:
            task = self._task_id_to_task[task_id]
            await self._function_manager.on_function(
                client,
                FunctionRequest(
                    FunctionRequestType.Add, task.function_id, self._function_id_to_functions[task.function_id]
                ),
            )
            task.function_args = [self.__get_argument(graph_task_id, task_id, arg) for arg in task.function_args]
            self._graph_task_id_to_inactive[graph_task_id].remove(task_id)
            self._graph_task_id_to_running[graph_task_id].add(task_id)
            await self._task_manager.on_task_new(client, task)

        return

    async def __finish_node_success(self, task_result: TaskResult):
        self.__clean_task_function(task_result.task_id)
        graph_task_id = self.__done_node(task_result.task_id, task_result.result)
        await self.__check_one_graph(graph_task_id)

    async def __finish_node_fail(self, task_result: TaskResult):
        self.__clean_task_function(task_result.task_id)
        graph_task_id = self.__done_node(task_result.task_id, task_result.result)
        client, _ = await self.__clean_graph(graph_task_id)
        await self._binder.send(
            client, MessageType.GraphTaskResult, GraphTaskResult(graph_task_id, TaskStatus.Failed, [task_result.result])
        )

    async def __finish_one_graph(self, graph_task_id: bytes):
        client, target_task_ids = await self.__clean_graph(graph_task_id)
        results = self.__get_target_results(target_task_ids)
        await self._binder.send(
            client, MessageType.GraphTaskResult, GraphTaskResult(graph_task_id, TaskStatus.Success, results)
        )

    async def __clean_graph(self, graph_task_id: bytes) -> Tuple[bytes, Set[bytes]]:
        for task_id in self._graph_task_id_to_inactive.pop(graph_task_id):
            self.__clean_task_function(task_id)

        client = self._graph_task_id_to_client.pop(graph_task_id)
        for task_id in self._graph_task_id_to_running.pop(graph_task_id):
            self.__clean_task_function(task_id)
            await self._task_manager.on_task_cancel(client, TaskCancel(task_id))

        target_task_ids = self._graph_task_id_to_target_task_ids.pop(graph_task_id)
        self._graph_task_id_to_sorter.pop(graph_task_id)
        return client, target_task_ids

    def __clean_task_function(self, task_id: bytes):
        function_id = self._task_id_to_function_id.pop(task_id)
        self._function_id_to_task_ids[function_id].remove(task_id)
        if not self._function_id_to_task_ids[function_id]:
            self._function_id_to_task_ids.pop(function_id)
            self._function_id_to_functions.pop(function_id)

    def __done_node(self, task_id: bytes, result: bytes) -> bytes:
        self._task_id_to_task.pop(task_id)
        self._task_id_to_result[task_id] = result

        graph_task_id = self._task_id_to_graph_id.pop(task_id)
        sorter = self._graph_task_id_to_sorter[graph_task_id]
        sorter.done(task_id)

        self._graph_task_id_to_running[graph_task_id].remove(task_id)

        return graph_task_id

    def __get_target_results(self, target_task_ids: Set[bytes]) -> List[bytes]:
        return [self._task_id_to_result.pop(task_id) for task_id in target_task_ids]

    def __get_argument(self, graph_task_id: bytes, task_id: bytes, argument: Argument) -> Argument:
        if argument.type == ArgumentType.Data:
            return argument

        assert argument.type == ArgumentType.Task
        dependent_task_id = argument.data

        result = self._task_id_to_result[dependent_task_id]
        argument = Argument(ArgumentType.Data, result)

        self._task_id_to_dependencies[dependent_task_id].remove(task_id)
        if not self._task_id_to_dependencies[dependent_task_id]:
            self._task_id_to_dependencies.pop(dependent_task_id)

            if dependent_task_id not in self._graph_task_id_to_target_task_ids[graph_task_id]:
                self._task_id_to_result.pop(dependent_task_id)

        return argument

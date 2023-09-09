import dataclasses
import enum
from asyncio import Queue
from graphlib import TopologicalSorter
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from scaled.io.async_binder import AsyncBinder
from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Argument
from scaled.protocol.python.message import ArgumentType
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionRequestType
from scaled.protocol.python.message import GraphTask
from scaled.protocol.python.message import GraphTaskCancel
from scaled.protocol.python.message import GraphTaskResult
from scaled.protocol.python.message import GraphTaskState
from scaled.protocol.python.message import NodeTaskType
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskCancel
from scaled.protocol.python.message import TaskResult
from scaled.protocol.python.message import TaskStatus
from scaled.scheduler.mixins import ClientManager
from scaled.scheduler.mixins import FunctionManager
from scaled.scheduler.mixins import GraphTaskManager
from scaled.scheduler.mixins import Looper
from scaled.scheduler.mixins import Reporter
from scaled.scheduler.mixins import TaskManager
from scaled.utility.key_value_set import KeyValueDictSet


class _TaskState(enum.Enum):
    Inactive = enum.auto()
    Running = enum.auto()
    Canceled = enum.auto()
    Failed = enum.auto()
    Success = enum.auto()


@dataclasses.dataclass
class _TaskInfo:
    state: _TaskState
    task: Task
    result: Optional[bytes] = None


@dataclasses.dataclass
class _Graph:
    target_task_ids: List[bytes]
    task_ids: Set[bytes]
    sorter: TopologicalSorter
    tasks: Dict[bytes, _TaskInfo]
    dependencies: KeyValueDictSet[bytes, bytes]
    client: bytes


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
        self._binder_monitor: Optional[AsyncConnector] = None
        self._client_manager: Optional[ClientManager] = None
        self._function_manager: Optional[FunctionManager] = None
        self._task_manager: Optional[TaskManager] = None

        self._unassigned: Queue = Queue()

        self._graph_task_id_to_graph: Dict[bytes, _Graph] = dict()
        self._task_id_to_graph_task_id: Dict[bytes, bytes] = dict()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        function_manager: FunctionManager,
        task_manager: TaskManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._client_manager = client_manager
        self._function_manager = function_manager
        self._task_manager = task_manager

    async def on_graph_task(self, client: bytes, graph_task: GraphTask):
        await self._unassigned.put((client, graph_task))

    async def on_graph_task_cancel(self, graph_task_cancel: GraphTaskCancel):
        await self.__clean_graph(graph_task_cancel.task_id)

    async def on_task_done(self, result: TaskResult) -> bool:
        if result.task_id not in self._task_id_to_graph_task_id:
            return False

        graph_task_id = self._task_id_to_graph_task_id[result.task_id]
        self.__mark_node_done(graph_task_id, result)

        if result.status == TaskStatus.Success:
            await self.__check_one_graph(graph_task_id)
            return True

        assert result.status in {TaskStatus.Canceled, TaskStatus.Failed, TaskStatus.NotFound}
        info = await self.__clean_graph(graph_task_id)
        await self._binder.send(info.client, GraphTaskResult(graph_task_id, result.status, [result.result]))
        return True

    async def routine(self):
        client, graph_task = await self._unassigned.get()
        await self.__add_new_graph(client, graph_task)

    async def statistics(self) -> Dict:
        return {"graph_manager": {"unassigned": self._unassigned.qsize()}}

    async def __add_new_graph(self, client: bytes, graph_task: GraphTask):
        graph = {}

        for function_id, (function_name, content) in graph_task.functions.items():
            await self._function_manager.on_function(
                client, FunctionRequest(FunctionRequestType.Add, function_id, function_name, content)
            )

        await self._client_manager.on_task_new(client, graph_task.task_id)

        task_ids: Set[bytes] = set()
        tasks: Dict[bytes, _TaskInfo] = dict()
        dependencies: KeyValueDictSet[bytes, bytes] = KeyValueDictSet()
        for task in graph_task.graph:
            await self._function_manager.on_task_use_function(task.task_id, task.function_id)

            self._task_id_to_graph_task_id[task.task_id] = graph_task.task_id
            tasks[task.task_id] = _TaskInfo(_TaskState.Inactive, task)

            required_task_ids = {arg.data for arg in task.function_args if arg.type == ArgumentType.Task}
            for required_task_id in required_task_ids:
                dependencies.add(required_task_id, task.task_id)

            graph[task.task_id] = required_task_ids
            task_ids.add(task.task_id)

            await self._binder_monitor.send(
                GraphTaskState(
                    graph_task.task_id,
                    task.task_id,
                    NodeTaskType.Target if task.task_id in graph_task.targets else NodeTaskType.Normal,
                    required_task_ids,
                )
            )

        sorter = TopologicalSorter(graph)
        sorter.prepare()

        self._graph_task_id_to_graph[graph_task.task_id] = _Graph(
            graph_task.targets, task_ids, sorter, tasks, dependencies, client
        )
        await self.__check_one_graph(graph_task.task_id)

    async def __check_one_graph(self, graph_task_id: bytes):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if not graph_info.sorter.is_active():
            await self.__finish_one_graph(graph_task_id)
            return

        ready_task_ids = graph_info.sorter.get_ready()
        if not ready_task_ids:
            return

        for task_id in ready_task_ids:
            task_info = graph_info.tasks[task_id]
            task_info.task.function_args = [
                self.__get_argument(graph_task_id, task_id, arg) for arg in task_info.task.function_args
            ]
            task_info.state = _TaskState.Running
            await self._task_manager.on_task_new(graph_info.client, task_info.task)

    async def __finish_one_graph(self, graph_task_id: bytes):
        results = self.__get_target_results(graph_task_id)
        info = await self.__clean_graph(graph_task_id)
        await self._binder.send(info.client, GraphTaskResult(graph_task_id, TaskStatus.Success, results))

    async def __clean_graph(self, graph_task_id: bytes) -> _Graph:
        client = await self._client_manager.on_task_done(graph_task_id)
        graph_info = self._graph_task_id_to_graph.pop(graph_task_id)

        for task_id in graph_info.task_ids:
            if task_id not in graph_info.tasks:
                continue

            task_info = graph_info.tasks.pop(task_id)
            if task_info.state == _TaskState.Inactive:
                await self._function_manager.on_task_done_function(task_id)
            elif task_info.state == _TaskState.Running:
                await self._task_manager.on_task_cancel(client, TaskCancel(task_id))
            else:
                pass

            self._task_id_to_graph_task_id.pop(task_id, None)

        return graph_info

    def __mark_node_done(self, graph_task_id: bytes, result: TaskResult):
        graph_info = self._graph_task_id_to_graph[graph_task_id]

        task_info = graph_info.tasks[result.task_id]
        task_info.result = result.result

        if result.status == TaskStatus.Success:
            task_info.state = _TaskState.Success
        elif result.status == TaskStatus.Canceled:
            task_info.state = _TaskState.Canceled
        elif result.status == TaskStatus.Failed:
            task_info.state = _TaskState.Failed
        else:
            raise ValueError(f"received unexpected task result {result}")

        graph_info.sorter.done(result.task_id)

    def __get_target_results(self, graph_task_id: bytes) -> List[bytes]:
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        return [graph_info.tasks[task_id].result for task_id in graph_info.target_task_ids]

    def __get_argument(self, graph_task_id: bytes, task_id: bytes, argument: Argument) -> Argument:
        if argument.type == ArgumentType.Data:
            return argument

        assert argument.type == ArgumentType.Task
        argument_task_id = argument.data

        result = self.__get_task_result(graph_task_id, task_id, argument_task_id)
        return Argument(ArgumentType.Data, result)

    def __get_task_result(self, graph_task_id: bytes, task_id: bytes, argument_task_id: bytes) -> bytes:
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        graph_info.dependencies.remove_value(argument_task_id, task_id)

        task_info = graph_info.tasks[argument_task_id]
        if argument_task_id in graph_info.dependencies:
            return task_info.result

        if argument_task_id not in graph_info.target_task_ids:
            self._task_id_to_graph_task_id.pop(argument_task_id)
            graph_info.tasks.pop(argument_task_id)

        return task_info.result

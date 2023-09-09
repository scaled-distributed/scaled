import logging
from typing import Dict
from typing import Optional
from typing import Set

from scaled.io.async_binder import AsyncBinder
from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskCancel
from scaled.protocol.python.message import TaskEcho
from scaled.protocol.python.message import TaskEchoStatus
from scaled.protocol.python.message import TaskResult
from scaled.protocol.python.message import TaskState
from scaled.protocol.python.message import TaskStatus
from scaled.scheduler.graph_manager import GraphManager
from scaled.scheduler.mixins import ClientManager
from scaled.scheduler.mixins import FunctionManager
from scaled.scheduler.mixins import Looper
from scaled.scheduler.mixins import Reporter
from scaled.scheduler.mixins import TaskManager
from scaled.scheduler.mixins import WorkerManager
from scaled.utility.queues.async_indexed_queue import IndexedQueue


class VanillaTaskManager(TaskManager, Looper, Reporter):
    def __init__(self, max_number_of_tasks_waiting: int):
        self._max_number_of_tasks_waiting = max_number_of_tasks_waiting
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._client_manager: Optional[ClientManager] = None
        self._function_manager: Optional[FunctionManager] = None
        self._worker_manager: Optional[WorkerManager] = None
        self._graph_manager: Optional[GraphManager] = None

        self._task_id_to_task: Dict[bytes, Task] = dict()

        self._unassigned: IndexedQueue[bytes] = IndexedQueue()
        self._running: Set[bytes] = set()

        self._success_count: int = 0
        self._failed_count: int = 0
        self._canceled_count: int = 0
        self._not_found_count: int = 0

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        function_manager: FunctionManager,
        worker_manager: WorkerManager,
        graph_manager: GraphManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._client_manager = client_manager
        self._function_manager = function_manager
        self._worker_manager = worker_manager
        self._graph_manager = graph_manager

    async def routine(self):
        task_id = await self._unassigned.get()

        if not await self._worker_manager.assign_task_to_worker(self._task_id_to_task[task_id]):
            await self._unassigned.put(task_id)
            return

        self._running.add(task_id)
        await self.__send_monitor(task_id, TaskStatus.Running)

    async def statistics(self) -> Dict:
        return {
            "task_manager": {
                "unassigned": self._unassigned.qsize(),
                "running": len(self._running),
                "success": self._success_count,
                "failed": self._failed_count,
                "canceled": self._canceled_count,
                "not_found": self._not_found_count,
            }
        }

    async def on_task_new(self, client: bytes, task: Task):
        if not self._function_manager.has_function(task.function_id):
            await self._binder.send(client, TaskEcho(task.task_id, TaskEchoStatus.FunctionNotExists))
            return

        if (
            not self._worker_manager.has_available_worker()
            and 0 <= self._max_number_of_tasks_waiting <= self._unassigned.qsize()
        ):
            await self._binder.send(client, TaskEcho(task.task_id, TaskEchoStatus.NoWorker))
            return

        await self._binder.send(client, TaskEcho(task.task_id, TaskEchoStatus.SubmitOK))
        await self._function_manager.on_task_use_function(task.task_id, task.function_id)
        await self._client_manager.on_task_new(client, task.task_id)

        self._task_id_to_task[task.task_id] = task
        await self._unassigned.put(task.task_id)
        await self.__send_monitor(task.task_id, TaskStatus.Inactive)

    async def on_task_reroute(self, task_id: bytes):
        assert self._client_manager.get_client_id(task_id) is not None

        self._running.remove(task_id)
        await self._unassigned.put(task_id)
        await self.__send_monitor(task_id, TaskStatus.Inactive)

    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        if task_cancel.task_id in self._unassigned:
            await self.on_task_done(TaskResult(task_cancel.task_id, TaskStatus.Canceled, 0, b""))
            return

        if task_cancel.task_id not in self._running:
            logging.warning(f"cannot cancel task is not running: task_id={task_cancel.task_id.hex()}")
            return

        await self._worker_manager.on_task_cancel(client, task_cancel)
        await self.__send_monitor(task_cancel.task_id, TaskStatus.Canceling)

    async def on_task_done(self, result: TaskResult):
        if result.status == TaskStatus.Success:
            self._success_count += 1
        elif result.status == TaskStatus.Failed:
            self._failed_count += 1
        elif result.status == TaskStatus.Canceled:
            self._canceled_count += 1
        elif result.status == TaskStatus.NotFound:
            self._not_found_count += 1

        if result.task_id in self._unassigned:
            self._unassigned.remove(result.task_id)
        elif result.task_id in self._running:
            self._running.remove(result.task_id)

        await self.__send_monitor(result.task_id, result.status)

        self._task_id_to_task.pop(result.task_id)
        await self._function_manager.on_task_done_function(result.task_id)
        client = await self._client_manager.on_task_done(result.task_id)

        if await self._graph_manager.on_task_done(result):
            return

        await self._binder.send(client, result)

    async def __send_monitor(self, task_id: bytes, status: TaskStatus):
        function_name = self._function_manager.get_function_name(self._task_id_to_task[task_id].function_id)
        await self._binder_monitor.send(TaskState(task_id, function_name, status))

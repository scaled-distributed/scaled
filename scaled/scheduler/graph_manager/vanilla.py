from typing import Optional

from scaled.protocol.python.message import GraphTask
from scaled.scheduler.mixins import Looper, TaskManager


class GraphManager(Looper):
    def __init__(self):
        self._task_manager: Optional[TaskManager] = None

    def register(self, task_manager: TaskManager):
        self._task_manager = task_manager

    async def on_graph_task(self, graph_task: GraphTask):
        pass

    async def routine(self):
        pass

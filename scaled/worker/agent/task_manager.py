from typing import Dict, List, Optional

from scaled.protocol.python.message import Task
from scaled.utility.queues.async_indexed_queue import IndexedQueue
from scaled.worker.agent.mixins import Looper, ProcessorManager, TaskManager


class VanillaTaskManager(Looper, TaskManager):
    def __init__(self):
        self._queued_task_id_to_task: Dict[bytes, Task] = dict()
        self._queued_task_ids = IndexedQueue()

        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, processor_manager: ProcessorManager):
        self._processor_manager = processor_manager

    async def on_queue_task(self, task: Task):
        self._queued_task_id_to_task[task.task_id] = task
        self._queued_task_ids.put_nowait(task.task_id)

    async def routine(self):
        await self.__processing_task()

    def on_task_result(self, task_id: bytes):
        self._processor_manager.on_task_result(task_id)

    def on_cancel_task(self, task_id: bytes) -> bool:
        if task_id in self._queued_task_id_to_task:
            self._queued_task_id_to_task.pop(task_id)
            self._queued_task_ids.remove(task_id)
            return True

        if self._processor_manager.on_cancel_task(task_id):
            return True

        return False

    def on_balance_remove_tasks(self, number_of_tasks: int) -> List[bytes]:
        number_of_tasks = min(number_of_tasks, self._queued_task_ids.qsize())
        removed_tasks = []
        while number_of_tasks:
            task_id = self._queued_task_ids.get_nowait()
            removed_tasks.append(task_id)
            self._queued_task_id_to_task.pop(task_id)
            number_of_tasks -= 1

        return removed_tasks

    def get_queued_size(self):
        return self._queued_task_ids.qsize()

    async def __processing_task(self):
        task_id = await self._queued_task_ids.get()
        task = self._queued_task_id_to_task.pop(task_id)
        await self._processor_manager.on_task(task)

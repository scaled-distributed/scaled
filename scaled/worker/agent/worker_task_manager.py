import asyncio
from typing import Dict, List, Optional

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, Task
from scaled.utility.queues.async_indexed_queue import IndexedQueue


class WorkerTaskManager:
    def __init__(self, connector_internal: AsyncConnector, processing_queue_size: int):
        self._connector_internal: Optional[AsyncConnector] = connector_internal
        self._processing_lock = asyncio.Semaphore(processing_queue_size)

        self._queued_task_id_to_task: Dict[bytes, Task] = dict()
        self._queued_task_ids = IndexedQueue()

    async def on_queue_task(self, task: Task):
        self._queued_task_id_to_task[task.task_id] = task
        self._queued_task_ids.put_nowait(task.task_id)

    async def routine(self):
        await self.__processing_task()

    def on_task_result(self):
        self._processing_lock.release()

    def remove_one_queued_task(self, task_id: bytes) -> bool:
        if task_id not in self._queued_task_id_to_task:
            return False

        self._queued_task_id_to_task.pop(task_id)
        self._queued_task_ids.remove(task_id)
        return True

    def on_balance_remove_tasks(self, number_of_tasks: int) -> List[bytes]:
        number_of_tasks = min(number_of_tasks, self._queued_task_ids.qsize())
        removed_tasks = []
        while number_of_tasks:
            task_id = self._queued_task_ids.get_nowait()
            removed_tasks.append(task_id)
            self._queued_task_id_to_task.pop(task_id)
            number_of_tasks -= 1

        return removed_tasks

    def get_queue_size(self):
        return self._queued_task_ids.qsize()

    async def __processing_task(self):
        await self._processing_lock.acquire()
        task_id = await self._queued_task_ids.get()
        task = self._queued_task_id_to_task.pop(task_id)
        await self._connector_internal.send(MessageType.Task, task)

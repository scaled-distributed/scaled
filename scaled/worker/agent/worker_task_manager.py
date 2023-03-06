import asyncio
from asyncio import Queue
from typing import List, Optional

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, Task


class WorkerTaskManager:
    def __init__(self, connector_internal: AsyncConnector, processing_queue_size: int):
        self._queued_tasks = Queue()
        self._processing_lock = asyncio.Semaphore(processing_queue_size)

        # the reason use this instead of len(self._queued_tasks) is because some message types can be FunctionRequest
        self._queue_tasks_size = 0
        self._connector_internal: Optional[AsyncConnector] = connector_internal

    def get_queue_size(self):
        return self._queue_tasks_size

    async def on_queue_message(self, task: Task):
        await self._queued_tasks.put(task)
        self._queue_tasks_size += 1

    def on_task_result(self):
        self._processing_lock.release()

    async def balance_remove_tasks(self, number_of_tasks: int) -> List[Task]:
        removed_tasks = []
        while not self._queued_tasks.empty() or number_of_tasks > 0:
            message = await self._queued_tasks.get()
            removed_tasks.append(message)
            number_of_tasks -= 1

        return removed_tasks

    async def loop(self):
        while True:
            await self.__processing_task()
            await asyncio.sleep(0)

    async def __processing_task(self):
        await self._processing_lock.acquire()
        message = await self._queued_tasks.get()
        self._queue_tasks_size -= 1
        await self._connector_internal.send(MessageType.Task, message)

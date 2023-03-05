import asyncio
from asyncio import Queue
from typing import List

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, MessageVariant, Task


class WorkerTaskManager:
    def __init__(self, connector_internal: AsyncConnector):
        self._queued_tasks = Queue()

        # the reason use this instead of len(self._queue) is because some message types can be FunctionRequest
        self._queue_tasks_size = 0
        self._connector_internal = connector_internal

    def get_queue_size(self):
        return self._queue_tasks_size

    async def on_queue_message(self, message_type: MessageType, message: MessageVariant):
        await self._queued_tasks.put((message_type, message))
        if message_type == MessageType.Task:
            self._queue_tasks_size += 1

    async def balance_remove_tasks(self, number_of_tasks: int) -> List[Task]:
        number_of_tasks = min(number_of_tasks, self._queue_tasks_size)
        if not number_of_tasks:
            return []

        new_queue = Queue()
        removed_tasks = []
        while not self._queued_tasks.empty():
            message_type, message = await self._queued_tasks.get()
            if message_type != MessageType.Task:
                await new_queue.put((message_type, message))
                continue

            if number_of_tasks > 0:
                removed_tasks.append(message)
                number_of_tasks -= 1
                continue
            else:
                await new_queue.put((message_type, message))

        self._queued_tasks = new_queue
        return removed_tasks

    async def loop(self):
        while True:
            message_type, message = await self._queued_tasks.get()
            if message_type == MessageType.Task:
                self._queue_tasks_size -= 1

            await self._connector_internal.send(message_type, message)
            await asyncio.sleep(0)

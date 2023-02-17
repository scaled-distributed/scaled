import queue

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, Task


class TaskQueue:
    def __init__(
        self, receive_task_queue: queue.Queue, send_task_queue: queue.Queue, connector_external: AsyncConnector
    ):
        self._receive_task_queue = receive_task_queue
        self._send_task_queue = send_task_queue
        self._connector_external = connector_external

    async def on_receive_task(self, task: Task):
        self._receive_task_queue.put(task)

    async def routine(self):
        while not self._send_task_queue.empty():
            task = self._send_task_queue.get()
            await self._connector_external.send(MessageType.TaskResult, task)

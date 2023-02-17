import queue

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType


class QueueSender:
    def __init__(self, send_task_queue: queue.Queue, connector_external: AsyncConnector):
        self._send_task_queue = send_task_queue
        self._connector_external = connector_external

    async def routine(self):
        while not self._send_task_queue.empty():
            await self._connector_external.send(MessageType.TaskResult, self._send_task_queue.get())

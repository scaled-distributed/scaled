import asyncio
from typing import Dict, List, Optional

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import (
    BalanceRequest,
    MessageType,
    Task,
    TaskCancel,
    TaskCancelEcho,
    TaskEchoStatus,
    TaskResult,
)
from scaled.utility.queues.async_indexed_queue import IndexedQueue
from scaled.worker.agent.mixins import Looper, ProcessorManager, TaskManager


class VanillaTaskManager(Looper, TaskManager):
    def __init__(self):
        self._queued_task_id_to_task: Dict[bytes, Task] = dict()
        self._queued_task_ids = IndexedQueue()

        self._connector_external: Optional[AsyncConnector] = None
        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, connector: AsyncConnector, processor_manager: ProcessorManager):
        self._connector_external = connector
        self._processor_manager = processor_manager

    async def on_task_new(self, task: Task):
        self._queued_task_id_to_task[task.task_id] = task
        await self._queued_task_ids.put(task.task_id)

    async def routine(self):
        await self.__processing_task()

    async def on_task_result(self, result: TaskResult):
        await self._connector_external.send(MessageType.TaskResult, result)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        if task_cancel.task_id in self._queued_task_id_to_task:
            self._queued_task_id_to_task.pop(task_cancel.task_id)
            self._queued_task_ids.remove(task_cancel.task_id)
            await self._connector_external.send(
                MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelOK)
            )
            return

        if await self._processor_manager.on_cancel_task(task_cancel.task_id):
            await self._connector_external.send(
                MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelOK)
            )
            return

        await self._connector_external.send(
            MessageType.TaskCancelEcho, TaskCancelEcho(task_cancel.task_id, TaskEchoStatus.CancelFailed)
        )

    def on_balance_request(self, balance_request: BalanceRequest) -> List[bytes]:
        number_of_tasks = min(balance_request.number_of_tasks, self._queued_task_ids.qsize())
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
        task = self._queued_task_id_to_task.pop(await self._queued_task_ids.get())
        if await self._processor_manager.on_task(task):
            await asyncio.sleep(0)
            return

        await self._queued_task_ids.put(task.task_id)
        self._queued_task_id_to_task[task.task_id] = task

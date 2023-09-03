import logging
import time
from typing import Dict, List, Optional, Tuple

from scaled.io.async_binder import AsyncBinder
from scaled.scheduler.mixins import Looper, Reporter, TaskManager, WorkerManager
from scaled.protocol.python.message import (
    BalanceRequest,
    BalanceResponse,
    DisconnectRequest,
    DisconnectResponse,
    Heartbeat,
    Task,
    TaskResult,
    TaskCancel,
    FunctionRequest,
    FunctionRequestType,
)
from scaled.scheduler.allocators.queued import QueuedAllocator


class VanillaWorkerManager(WorkerManager, Looper, Reporter):
    def __init__(
        self,
        per_worker_queue_size: int,
        timeout_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
    ):
        self._timeout_seconds = timeout_seconds
        self._load_balance_seconds = load_balance_seconds
        self._load_balance_trigger_times = load_balance_trigger_times

        self._binder: Optional[AsyncBinder] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since: Dict[bytes, Tuple[float, Heartbeat]] = dict()
        self._allocator = QueuedAllocator(per_worker_queue_size)

        self._last_balance_advice = None
        self._load_balance_advice_same_count = 0

    def register(self, binder: AsyncBinder, task_manager: TaskManager):
        self._binder = binder
        self._task_manager = task_manager

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = await self._allocator.assign_task(task.task_id)
        if worker is None:
            return False

        # send to worker
        await self._binder.send(worker, task)
        return True

    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        worker = self._allocator.get_assigned_worker(task_cancel.task_id)
        if worker is None:
            logging.error(f"cannot find task_id={task_cancel.task_id.hex()} in task workers")
            return

        await self._binder.send(worker, TaskCancel(task_cancel.task_id))

    async def on_task_done(self, task_result: TaskResult):
        worker = self._allocator.remove_task(task_result.task_id)
        if worker is None:
            logging.error(
                f"received unknown task result for task_id={task_result.task_id.hex()}, status={task_result.status} "
                f"might due to worker get disconnected or canceled"
            )
            return

        await self._task_manager.on_task_done(task_result)

    async def on_balance_response(self, response: BalanceResponse):
        task_ids = []
        for task_id in response.task_ids:
            worker = self._allocator.remove_task(task_id)
            if worker is None:
                continue

            task_ids.append(task_id)

        await self.__reroute_tasks(task_ids=task_ids)

    async def on_heartbeat(self, worker: bytes, info: Heartbeat):
        if await self._allocator.add_worker(worker):
            logging.info(f"worker {worker} connected")

        self._worker_alive_since[worker] = (time.time(), info)

    async def on_delete_function(self, function_id: bytes):
        for worker in self._allocator.get_worker_ids():
            await self._binder.send(worker, FunctionRequest(FunctionRequestType.Delete, function_id, b"", b""))

    async def on_disconnect(self, source: bytes, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(source, DisconnectResponse(request.worker))

    async def routine(self):
        await self.__balance_request()
        await self.__clean_workers()

    async def statistics(self) -> Dict:
        worker_to_task_numbers = self._allocator.statistics()
        return {
            "worker_manager": [
                {
                    "worker": worker.decode(),
                    "agt_cpu": round(info.agent_cpu, 2),
                    "agt_rss": info.agent_rss,
                    "cpu": round(info.worker_cpu, 2),
                    "rss": info.worker_rss,
                    **worker_to_task_numbers[worker],
                    "queued": info.queued_tasks,
                }
                for worker, (last, info) in self._worker_alive_since.items()
            ]
        }

    def has_available_worker(self) -> bool:
        return self._allocator.has_available_worker()

    async def __balance_request(self):
        if self._load_balance_seconds <= 0:
            return

        current_advice = self._allocator.balance()
        if self._last_balance_advice != current_advice:
            self._last_balance_advice = current_advice
            self._load_balance_advice_same_count = 1
            return

        self._load_balance_advice_same_count += 1
        if self._load_balance_advice_same_count != self._load_balance_trigger_times:
            return

        if not current_advice:
            return

        logging.info(f"balance: {current_advice}")
        self._last_balance_advice = current_advice
        for worker, number_of_tasks in current_advice.items():
            await self._binder.send(worker, BalanceRequest(number_of_tasks))

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
        ]
        for dead_worker in dead_workers:
            await self.__disconnect_worker(dead_worker)

    async def __reroute_tasks(self, task_ids: List[bytes]):
        for task_id in task_ids:
            await self._task_manager.on_task_reroute(task_id)

    async def __disconnect_worker(self, worker: bytes):
        logging.info(f"worker {worker} disconnected")
        self._worker_alive_since.pop(worker)

        task_ids = self._allocator.remove_worker(worker)
        if not task_ids:
            return

        logging.info(f"rerouting {len(task_ids)} tasks")
        await self.__reroute_tasks(task_ids)

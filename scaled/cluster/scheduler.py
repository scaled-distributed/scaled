import asyncio
import multiprocessing
from typing import Literal, Optional

from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.scheduler.main import Scheduler, scheduler_main
from scaled.utility.logging.utility import setup_logger


class SchedulerProcess(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        io_threads: int,
        max_number_of_tasks_waiting: int,
        per_worker_queue_size: int,
        worker_timeout_seconds: int,
        function_retention_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
        event_loop: Literal["builtin", "uvloop"] = "builtin",
    ):
        multiprocessing.Process.__init__(self, name="Scheduler")
        self._address = address
        self._io_threads = io_threads
        self._max_number_of_tasks_waiting = max_number_of_tasks_waiting
        self._per_worker_queue_size = per_worker_queue_size
        self._worker_timeout_seconds = worker_timeout_seconds
        self._function_retention_seconds = function_retention_seconds
        self._load_balance_seconds = load_balance_seconds
        self._load_balance_trigger_times = load_balance_trigger_times

        self._event_loop = event_loop
        self._scheduler: Optional[Scheduler] = None
        self._loop = None
        self._task = None

    def run(self) -> None:
        # scheduler have its own single process
        setup_logger()
        register_event_loop(self._event_loop)

        self._loop = asyncio.get_event_loop()
        self._task = self._loop.create_task(
            scheduler_main(
                address=self._address,
                io_threads=self._io_threads,
                max_number_of_tasks_waiting=self._max_number_of_tasks_waiting,
                per_worker_queue_size=self._per_worker_queue_size,
                worker_timeout_seconds=self._worker_timeout_seconds,
                function_retention_seconds=self._function_retention_seconds,
                load_balance_seconds=self._load_balance_seconds,
                load_balance_trigger_times=self._load_balance_trigger_times,
            )
        )

        self._loop.run_until_complete(self._task)

import logging
import multiprocessing

from scaled.cluster.scheduler import SchedulerProcess
from scaled.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
)
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.zmq_config import ZMQConfig
from scaled.cluster.cluster import ClusterProcess


class SchedulerClusterCombo:
    def __init__(
        self,
        address: str,
        n_workers: int,
        io_threads: int = DEFAULT_IO_THREADS,
        max_number_of_tasks_waiting: int = DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        event_loop: str = "builtin",
        worker_timeout_seconds: int = DEFAULT_WORKER_TIMEOUT_SECONDS,
        function_retention_seconds: int = DEFAULT_FUNCTION_RETENTION_SECONDS,
        load_balance_seconds: int = DEFAULT_LOAD_BALANCE_SECONDS,
        load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        garbage_collect_interval_seconds: int = DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes: int = DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        per_worker_queue_size: int = DEFAULT_PER_WORKER_QUEUE_SIZE,
        serializer: Serializer = DefaultSerializer(),
    ):
        self._cluster = ClusterProcess(
            address=ZMQConfig.from_string(address),
            n_workers=n_workers,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            function_retention_seconds=function_retention_seconds,
            garbage_collect_interval_seconds=garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=trim_memory_threshold_bytes,
            event_loop=event_loop,
            serializer=serializer,
        )
        self._scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(address),
            io_threads=io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            per_worker_queue_size=per_worker_queue_size,
            worker_timeout_seconds=worker_timeout_seconds,
            function_retention_seconds=function_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
        )

        self._cluster.start()
        self._scheduler.start()
        logging.info(f"{self.__get_prefix()} started")

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        logging.info(f"{self.__get_prefix()} shutdown")
        self._cluster.terminate()
        self._scheduler.terminate()

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

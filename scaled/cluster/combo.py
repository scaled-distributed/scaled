import logging
import multiprocessing

from scaled.cluster.scheduler import SchedulerProcess
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.zmq_config import ZMQConfig
from scaled.cluster.cluster import ClusterProcess


class SchedulerClusterCombo:
    def __init__(
        self,
        address: str,
        n_workers: int,
        heartbeat_interval_seconds: int = 1,
        event_loop: str = "builtin",
        worker_timeout_seconds: int = 10,
        function_retention_seconds: int = 60,
        per_worker_queue_size: int = 1000,
        serializer: Serializer = DefaultSerializer(),
    ):
        self._stop_event = multiprocessing.get_context("spawn").Event()
        self._cluster = ClusterProcess(
            stop_event=self._stop_event,
            address=ZMQConfig.from_string(address),
            n_workers=n_workers,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            function_retention_seconds=function_retention_seconds,
            event_loop=event_loop,
            serializer=serializer,
        )
        self._scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(address),
            stop_event=self._stop_event,
            per_worker_queue_size=per_worker_queue_size,
            worker_timeout_seconds=worker_timeout_seconds,
            function_retention_seconds=function_retention_seconds,
        )

        self._cluster.start()
        self._scheduler.start()
        logging.info(f"{self.__get_prefix()} started")

    def __del__(self):
        self.shutdown()
        logging.info(f"{self.__get_prefix()} shutdown")

    def shutdown(self):
        self._stop_event.set()
        self._cluster.join()
        self._scheduler.join()

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

import logging
import multiprocessing

from scaled.cluster.scheduler import SchedulerProcess
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.scheduler.worker_manager.vanilla import AllocatorType
from scaled.utility.zmq_config import ZMQConfig
from scaled.cluster.cluster import ClusterProcess


class SchedulerClusterCombo:
    def __init__(
        self,
        address: str,
        n_workers: int,
        heartbeat_interval: int = 1,
        event_loop: str = "builtin",
        worker_timeout_seconds: int = 10,
        function_timeout_seconds: int = 60,
        allocator_type: AllocatorType = AllocatorType.Queued,
        serializer: Serializer = DefaultSerializer(),
    ):
        self._stop_event = multiprocessing.get_context("spawn").Event()
        self._cluster = ClusterProcess(
            stop_event=self._stop_event,
            address=ZMQConfig.from_string(address),
            n_workers=n_workers,
            heartbeat_interval=heartbeat_interval,
            event_loop=event_loop,
            serializer=serializer,
        )
        self._scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(address),
            stop_event=self._stop_event,
            allocator_type=allocator_type,
            worker_timeout_seconds=worker_timeout_seconds,
            function_timeout_seconds=function_timeout_seconds,
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

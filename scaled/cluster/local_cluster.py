import logging
import multiprocessing

from scaled.cluster.local.local_router import LocalRouter
from scaled.scheduler.worker_manager.vanilla import AllocatorType
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.worker_master import WorkerMaster


class LocalCluster:
    def __init__(
        self,
        address: ZMQConfig,
        n_workers: int,
        heartbeat_interval: int = 1,
        allocator_type: AllocatorType = AllocatorType.Queued,
        worker_timeout_seconds: int = 10,
        function_timeout_seconds: int = 60,
    ):
        self._stop_event = multiprocessing.get_context("spawn").Event()
        self._worker_master = WorkerMaster(
            stop_event=self._stop_event, address=address, n_workers=n_workers, heartbeat_interval=heartbeat_interval
        )
        self._router = LocalRouter(
            address=address,
            stop_event=self._stop_event,
            allocator_type=allocator_type,
            worker_timeout_seconds=worker_timeout_seconds,
            function_timeout_seconds=function_timeout_seconds
        )

        self._worker_master.start()
        self._router.start()
        logging.info(f"{self.__get_prefix()} started")

    def __del__(self):
        self.shutdown()
        logging.info(f"{self.__get_prefix()} shutdown")

    def shutdown(self):
        self._stop_event.set()
        self._worker_master.join()
        self._router.join()

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

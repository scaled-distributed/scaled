import logging
import multiprocessing

from scaled.cluster.local.local_router import LocalRouter
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.worker_master import WorkerMaster

PREFIX = "LocalCluster:"


class LocalCluster:
    def __init__(self, address: ZMQConfig, n_workers: int, polling_time: int = 1, heartbeat_interval: int = 1):
        self._stop_event = multiprocessing.get_context("spawn").Event()
        self._worker_master = WorkerMaster(
            address=address,
            stop_event=self._stop_event,
            n_workers=n_workers,
            polling_time=polling_time,
            heartbeat_interval=heartbeat_interval,
        )
        self._router = LocalRouter(address=address, stop_event=self._stop_event)

        self._worker_master.start()
        self._router.start()
        logging.info(f"{PREFIX} started")

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        logging.info(f"{PREFIX} shutdown")
        self._stop_event.set()

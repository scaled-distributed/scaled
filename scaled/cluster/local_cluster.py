import logging
import multiprocessing

from scaled.cluster.local.local_router import LocalRouter
from scaled.io.config import ZMQConfig
from scaled.worker.worker_master import WorkerMaster


class LocalCluster:
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: multiprocessing.Event,
        n_workers: int,
        polling_time: int = 1,
        heartbeat_interval: int = 5,
    ):
        self._worker_master = WorkerMaster(
            address=address,
            stop_event=stop_event,
            n_workers=n_workers,
            polling_time=polling_time,
            heartbeat_interval=heartbeat_interval,
        )
        self._router = LocalRouter(address=address, stop_event=stop_event)


    def start(self):
        self._worker_master.start()
        self._router.start()


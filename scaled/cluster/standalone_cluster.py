import logging
import multiprocessing

from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.worker_master import WorkerMaster

PREFIX = "StandaloneCluster:"


class StandaloneCluster:
    def __init__(
        self,
        address: ZMQConfig,
        n_workers: int,
        heartbeat_interval: int,
        stop_event: multiprocessing.Event,
    ):
        self._stop_event = stop_event
        self._worker_master = WorkerMaster(
            address=address,
            stop_event=self._stop_event,
            n_workers=n_workers,
            heartbeat_interval=heartbeat_interval,
        )
        self._worker_master.start()
        logging.info(f"{PREFIX} started")

    def __del__(self):
        self.shutdown()

    def join(self):
        self._worker_master.join()

    def shutdown(self):
        logging.info(f"{PREFIX} shutdown")
        self._stop_event.set()

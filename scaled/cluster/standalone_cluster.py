import logging
import multiprocessing

from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.worker_master import WorkerMaster


class StandaloneCluster:
    def __init__(self, address: ZMQConfig, n_workers: int, heartbeat_interval: int, stop_event: multiprocessing.Event):
        self._stop_event = stop_event
        self._worker_master = WorkerMaster(
            stop_event=self._stop_event, address=address, n_workers=n_workers, heartbeat_interval=heartbeat_interval
        )
        self._worker_master.start()
        logging.info(f"{self.__get_prefix()} started")

    def __del__(self):
        self.shutdown()

    def join(self):
        self._worker_master.join()

    def shutdown(self):
        logging.info(f"{self.__get_prefix()} shutdown")
        self._stop_event.set()

    def __get_prefix(self):
        return f"{self.__class__.__name__}"

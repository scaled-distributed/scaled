import logging
import multiprocessing
import signal
from typing import List

from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger
from scaled.worker.worker import Worker


class WorkerMaster(multiprocessing.get_context("spawn").Process):
    def __init__(self, address: ZMQConfig, n_workers: int, stop_event: multiprocessing.Event, heartbeat_interval: int):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = address
        self._n_workers = n_workers

        self._heartbeat_interval = heartbeat_interval

        self._stop_event = stop_event
        self._workers: List[Worker] = []

    def run(self):
        setup_logger()
        self.__register_signal()
        self.__start_workers()

    def wait_for_workers(self):
        for worker in self._workers:
            worker.join()

    def shutdown(self, *args):
        self._stop_event.set()
        self.wait_for_workers()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __start_workers(self):
        logging.info("WorkerMaster: started")
        for i in range(self._n_workers):
            self._workers.append(
                Worker(
                    address=self._address,
                    stop_event=self._stop_event,
                    heartbeat_interval_seconds=self._heartbeat_interval,
                )
            )

        if self._n_workers == 1:
            self._workers[0].run()
            return

        for worker in self._workers:
            worker.start()

        self.wait_for_workers()

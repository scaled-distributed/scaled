import logging
import multiprocessing
import signal
import time
from typing import List

from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger
from scaled.worker.worker import Worker


class WorkerMaster(multiprocessing.get_context("spawn").Process):
    def __init__(self, stop_event: multiprocessing.Event, address: ZMQConfig, n_workers: int, heartbeat_interval: int):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = address
        self._n_workers = n_workers

        self._heartbeat_interval = heartbeat_interval

        self._stop_event = stop_event
        self._workers: List[Worker] = []

    def run(self):
        setup_logger()
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def shutdown(self, *args):
        logging.info(f"received signal, abort")
        self._stop_event.set()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {self._n_workers} workers, heartbeat interval is "
            f"{self._heartbeat_interval} seconds"
        )
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

        for worker in self._workers:
            worker.join()

        while not self._stop_event.is_set():
            time.sleep(0.1)
            continue

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

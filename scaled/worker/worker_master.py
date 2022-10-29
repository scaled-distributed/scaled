import logging
import multiprocessing
import signal
from typing import List

from scaled.io.config import ZMQConfig
from scaled.worker.single_worker import SingleWorker


class WorkerMaster:
    def __int__(self, address: ZMQConfig, n_workers: int, polling_time: int, heartbeat_interval: int):
        self._address = address
        self._n_workers = n_workers

        self._polling_time = polling_time
        self._heartbeat_interval = heartbeat_interval

        self._stop_event = multiprocessing.get_context("spawn").Event()
        self._workers: List[SingleWorker] = []

    def run(self):
        self._register_signal()
        self._start_workers()

    def close(self, sig, frame):
        assert sig is not None
        assert frame is not None
        logging.info(f"exiting program, closing all {len(self._workers)} workers")
        self._stop_event.set()
        self.join()

    def join(self):
        for worker in self._workers:
            worker.join()

    def _register_signal(self):
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

    def _start_workers(self):
        for i in range(self._n_workers):
            self._workers.append(
                SingleWorker(
                    address=self._address,
                    stop_event=self._stop_event,
                    polling_time=self._polling_time,
                    heartbeat_interval=self._heartbeat_interval
                )
            )

        if self._n_workers == 1:
            self._workers[0].run()
            return

        for worker in self._workers:
            worker.start()
        self.join()

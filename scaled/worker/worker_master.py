import multiprocessing
from typing import List

from scaled.io.config import ZMQConfig
from scaled.worker.worker import Worker


class WorkerMaster(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        n_workers: int,
        stop_event: multiprocessing.Event,
        polling_time: int,
        heartbeat_interval: int,
    ):
        multiprocessing.Process.__init__(self, name="worker_master")

        self._address = address
        self._n_workers = n_workers

        self._polling_time = polling_time
        self._heartbeat_interval = heartbeat_interval

        self._stop_event = stop_event
        self._workers: List[Worker] = []

    def run(self):
        self._start_workers()
        self.join()

    def join(self):
        for worker in self._workers:
            worker.join()

        print("WorkerMaster: quiting")

    def _start_workers(self):
        print("WorkerMaster: starting")
        for i in range(self._n_workers):
            self._workers.append(
                Worker(
                    address=self._address,
                    stop_event=self._stop_event,
                    polling_time=self._polling_time,
                    heartbeat_interval=self._heartbeat_interval,
                )
            )

        if self._n_workers == 1:
            self._workers[0].run()
            return

        for worker in self._workers:
            worker.start()


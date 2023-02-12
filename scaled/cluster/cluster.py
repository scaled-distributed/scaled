import logging
import multiprocessing
from multiprocessing.synchronize import Event as EventClass
import signal
import time
from typing import List

from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger
from scaled.worker.worker import Worker


class ClusterProcess(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        stop_event: EventClass,
        address: ZMQConfig,
        n_workers: int,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        event_loop: str,
        serializer: Serializer,
    ):
        multiprocessing.Process.__init__(self, name="WorkerMaster")
        assert isinstance(stop_event, EventClass)
        self._stop_event: multiprocessing.Event = stop_event

        self._address = address
        self._n_workers = n_workers
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._function_retention_seconds = function_retention_seconds
        self._event_loop = event_loop
        self._serializer = serializer

        self._workers: List[Worker] = []

    def run(self):
        setup_logger()
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def __shutdown(self, *args):
        assert args is not None
        logging.info(f"received signal, abort")
        self._stop_event.set()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__shutdown)
        signal.signal(signal.SIGTERM, self.__shutdown)

    def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {self._n_workers} workers, heartbeat_interval_seconds="
            f"{self._heartbeat_interval_seconds}, function_retention_seconds={self._function_retention_seconds}"
        )

        self._workers = [
            Worker(
                address=self._address,
                stop_event=self._stop_event,
                heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                function_retention_seconds=self._function_retention_seconds,
                event_loop=self._event_loop,
                serializer=self._serializer,
            )
            for _ in range(self._n_workers)
        ]

        if self._n_workers == 1:
            self._workers[0].run()
            return

        for worker in self._workers:
            worker.start()

        for i, worker in enumerate(self._workers):
            logging.info(f"Worker[{i}] started")

        while not self._stop_event.is_set():
            time.sleep(0.1)
            continue

        for i, worker in enumerate(self._workers):
            worker.join()
            logging.info(f"Worker[{i}] quited")

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

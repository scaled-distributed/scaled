import logging
import multiprocessing
import os
import signal
from typing import List

from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.logging.utility import setup_logger
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.worker import Worker


class Cluster(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        address: ZMQConfig,
        n_workers: int,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        death_timeout_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        event_loop: str,
        serializer: Serializer,
    ):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = address
        self._n_workers = n_workers
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._function_retention_seconds = function_retention_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._event_loop = event_loop
        self._serializer = serializer

        self._workers: List[Worker] = []

    def run(self):
        setup_logger()
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def __destroy(self, *args):
        assert args is not None
        logging.info(f"{self.__get_prefix()} received signal, shutting down")
        for worker in self._workers:
            logging.info(f"{self.__get_prefix()}: shutting down worker[{worker.pid}]")
            os.kill(worker.pid, signal.SIGINT)

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__destroy)
        signal.signal(signal.SIGTERM, self.__destroy)

    def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {self._n_workers} workers, heartbeat_interval_seconds="
            f"{self._heartbeat_interval_seconds}, function_retention_seconds={self._function_retention_seconds}"
        )

        self._workers = [
            Worker(
                event_loop=self._event_loop,
                address=self._address,
                heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
                serializer=self._serializer,
                function_retention_seconds=self._function_retention_seconds,
                death_timeout_seconds=self._death_timeout_seconds,
            )
            for _ in range(self._n_workers)
        ]

        if self._n_workers == 1:
            self._workers[0].run()
            return

        for worker in self._workers:
            worker.start()

        for i, worker in enumerate(self._workers):
            logging.info(f"Worker[{worker.ident}] started")

        for worker in self._workers:
            worker.join()

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

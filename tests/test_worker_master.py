import time
import unittest
import multiprocessing

from scaled.io.config import ZMQConfig, ZMQType
from scaled.worker.worker_master import WorkerMaster


def sleep_print(sec: int):
    time.sleep(sec)
    print(f"slept {sec}")


class TestWorkerMaster(unittest.TestCase):
    def test_worker_master(self):
        config = ZMQConfig(type=ZMQType.inproc, host="demo")

        event = multiprocessing.get_context("spawn").Event()

        master = WorkerMaster(address=config, n_workers=4, stop_event=event, polling_time=1, heartbeat_interval=5)
        master.start()

        time.sleep(10)
        event.set()

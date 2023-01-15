import time
import unittest
import multiprocessing

from scaled.io.config import ZMQConfig, ZMQType
from scaled.scheduler.router import Router
from scaled.cluster.local.local_cluster import LocalCluster


def sleep_print(sec: int):
    time.sleep(sec)
    print(f"slept {sec}")


class TestWorkerMaster(unittest.TestCase):
    def test_worker_master(self):
        config = ZMQConfig(type=ZMQType.inproc, host="demo")

        event = multiprocessing.get_context("spawn").Event()

        cluster = LocalCluster(config=config, n_workers=4, stop_event=event, polling_time=1, heartbeat_interval=5)
        cluster.start()

        time.sleep(10)
        event.set()

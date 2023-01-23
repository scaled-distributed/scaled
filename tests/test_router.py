import logging
import multiprocessing
import time
import unittest

from scaled.client.client import Client
from scaled.cluster.local_cluster import LocalCluster
from scaled.io.config import ZMQConfig, ZMQType


def sleep_print(sec: int):
    time.sleep(sec)
    print(f"slept {sec}")
    return sec


class TestRouter(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(format="%(asctime)s %(clientip)-15s %(user)-8s %(message)s")

    def test_worker_master(self):
        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12348)

        event = multiprocessing.get_context("spawn").Event()

        cluster = LocalCluster(address=config, n_workers=4, stop_event=event)
        cluster.start()

        client = Client(config=config)
        future = client.submit(sleep_print, 2)
        # print(future.result())

        time.sleep(10)
        event.set()

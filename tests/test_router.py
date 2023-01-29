import time
import unittest

from scaled.client.client import Client
from scaled.cluster.local_cluster import LocalCluster
from scaled.io.config import ZMQConfig, ZMQType
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    # time.sleep(sec)
    return sec * 1


class TestRouter(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_worker_master(self):
        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12348)

        cluster = LocalCluster(address=config, n_workers=4)
        time.sleep(2)

        client = Client(config=config)
        futures = [client.submit(sleep_print, i) for i in range(10000)]

        with ScopedLogger(f"get {len(futures)} results"):
            results = client.gather(futures)

        assert results == list(range(10000))

        cluster.shutdown()

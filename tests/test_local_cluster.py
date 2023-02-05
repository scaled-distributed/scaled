import random
import unittest

from scaled.client.client import Client
from scaled.cluster.local_cluster import LocalCluster
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


class TestLocalCluster(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_local_cluster(self):
        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=2345)

        cluster = LocalCluster(address=config, n_workers=2)
        client = Client(config=config)

        tasks = [random.randint(0, 100) for i in range(10000)]

        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(sleep_print, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)

        cluster.shutdown()
        client.disconnect()

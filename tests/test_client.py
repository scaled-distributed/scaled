import json
import random

import unittest

from scaled.client.client import Client
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    # time.sleep(sec)
    return sec * 1


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_client(self):
        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=2345)
        client = Client(config=config)

        tasks = [random.randint(0, 100) for i in range(100000)]
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(sleep_print, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)
        client.disconnect()

    def test_monitor(self):
        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=2345)
        client = Client(config=config)
        print(json.dumps(client.statistics(), indent=4))
        client.disconnect()

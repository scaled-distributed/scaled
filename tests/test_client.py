import json
import random

import unittest

from scaled.client import Client
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_client(self):
        # need server
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [random.randint(0, 100) for _ in range(100000)]
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(sleep_print, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)
        client.disconnect()

    def test_monitor(self):
        client = Client(address="tcp://127.0.0.1:2345")
        print(json.dumps(client.scheduler_status(), indent=4))
        client.disconnect()

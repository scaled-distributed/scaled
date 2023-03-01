import json
import random

import unittest

from scaled.client import Client
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def noop(sec: int):
    return sec * 1

def raise_exception(foo: int):
    if foo == 11:
        raise ValueError(f"foo cannot be 100")


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_client(self):
        # need server
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [random.randint(0, 100) for _ in range(100000)]
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(noop, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)


    def test_raise_exception(self):
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [i for i in range(100)]
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(raise_exception, i) for i in tasks]

        with self.assertRaises(ValueError), ScopedLogger(f"gather {len(futures)} results"):
            _ = [future.result() for future in futures]

        client.disconnect()

    def test_monitor(self):
        client = Client(address="tcp://127.0.0.1:2345")
        print(json.dumps(client.scheduler_status(), indent=4))
        client.disconnect()

import random
import time

import unittest


from scaled.client import Client
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def raise_exception(foo: int):
    if foo == 11:
        raise ValueError(f"foo cannot be 100")


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_noop(self):
        # need server
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [random.randint(0, 100) for _ in range(100000)]
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(noop, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)

    def test_sleep(self):
        client = Client(address="tcp://127.0.0.1:2345")

        # tasks = [10, 1, 1] * 10
        tasks = [10] * 10
        with ScopedLogger(f"submit {len(tasks)} tasks"):
            futures = [client.submit(noop_sleep, i) for i in tasks]

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

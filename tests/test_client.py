import functools
import random
import time

import unittest
from collections import Counter

from scaled.client import Client
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def heavy_function(sec: int, payload: bytes):
    return len(payload) * sec


def raise_exception(foo: int):
    if foo == 11:
        raise ValueError(f"foo cannot be 100")


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()

    def test_noop(self):
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [random.randint(0, 100) for _ in range(10000)]
        with ScopedLogger(f"submit {len(tasks)} noop tasks"):
            futures = [client.submit(noop, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)

    def test_noop_cancel(self):
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [10, 1, 1] * 10
        with ScopedLogger(f"submit {len(tasks)} noop and cancel tasks"):
            futures = [client.submit(noop_sleep, i) for i in tasks]

        time.sleep(1)
        client.disconnect()
        time.sleep(1)

    def test_heavy_function(self):
        client = Client(address="tcp://127.0.0.1:2345")

        size = 500_000_000
        tasks = [random.randint(0, 100) for _ in range(10000)]
        function = functools.partial(heavy_function, payload=b"1" * size)
        with ScopedLogger(f"submit {len(tasks)} heavy function (500mb) tasks"):
            futures = [client.submit(function, i) for i in tasks]

        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        expected = [task * size for task in tasks]
        self.assertEqual(results, expected)

    def test_sleep(self):
        client = Client(address="tcp://127.0.0.1:2345")

        time.sleep(5)

        tasks = [10, 1, 1] * 10
        # tasks = [10] * 10
        with ScopedLogger(f"submit {len(tasks)} sleep and balance tasks"):
            futures = [client.submit(noop_sleep, i) for i in tasks]

        # time.sleep(60)
        # print(f"number of futures: {len(futures)}")
        # print(f"number of states: {Counter([future._state for future in futures])}")
        # print(f"pending futures: {client._task_id_to_task_function}")
        with ScopedLogger(f"gather {len(futures)} results"):
            results = [future.result() for future in futures]

        self.assertEqual(results, tasks)

    def test_raise_exception(self):
        client = Client(address="tcp://127.0.0.1:2345")

        tasks = [i for i in range(100)]
        with ScopedLogger(f"submit {len(tasks)} 100 tasks, raise 1 of the tasks"):
            futures = [client.submit(raise_exception, i) for i in tasks]

        with self.assertRaises(ValueError), ScopedLogger(f"gather {len(futures)} results"):
            _ = [future.result() for future in futures]

        client.disconnect()

    def test_function(self):
        def func_args(a: int, b: int, c: int, d: int = 0):
            return a, b, c, d

        def func_args2(a: int, b: int, *, c: int, d: int = 0):
            return a, b, c, d

        client = Client(address="tcp://127.0.0.1:2345")

        with ScopedLogger(f"test mix of positional and keyword arguments and with some arguments default value"):
            self.assertEqual(client.submit(func_args, 1, c=4, b=2).result(), (1, 2, 4, 0))

        with ScopedLogger(f"test all keyword arguments"):
            self.assertEqual(client.submit(func_args, d=5, b=3, c=1, a=4).result(), (4, 3, 1, 5))

        with ScopedLogger(f"test mix of positional and keyword arguments with override default value"):
            self.assertEqual(client.submit(func_args, 1, c=4, b=2, d=6).result(), (1, 2, 4, 6))

        with ScopedLogger(f"test partial function"):
            self.assertEqual(client.submit(functools.partial(func_args, 5, 6), 1, 2).result(), (5, 6, 1, 2))

        with ScopedLogger(f"test insufficient arguments"), self.assertRaises(TypeError):
            client.submit(func_args, 1)

        with ScopedLogger(f"test not allow keyword only arguments even assigned"), self.assertRaises(TypeError):
            client.submit(func_args2, 1, c=4, b=2, d=6).result()

        with ScopedLogger(f"test not allow keyword only arguments"), self.assertRaises(TypeError):
            client.submit(func_args2, a=3, b=4).result()

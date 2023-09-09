import graphlib
import time
import unittest

from scaled import Client
from scaled import SchedulerClusterCombo
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


class TestGraph(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        self.address = "tcp://127.0.0.1:2345"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=3, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_graph(self):
        def inc(i):
            return i + 1

        def add(a, b):
            return a + b

        def minus(a, b):
            return a - b

        graph = {"a": 2, "b": 2, "c": (inc, "a"), "d": (add, "a", "b"), "e": (minus, "d", "c")}

        client = Client(self.address)

        with ScopedLogger("test graph"):
            results = client.get(graph, ["e"])
            self.assertEqual(results, {"e": 1})

        with self.assertRaises(graphlib.CycleError):
            client.get({"b": (inc, "c"), "c": (inc, "b")}, ["b", "c"])

    def test_graph_fail(self):
        def inc(_):
            time.sleep(1)
            raise ValueError("Compute Error")

        def add(a, b):
            time.sleep(5)
            return a + b

        def minus(a, b):
            return a - b

        graph = {"a": 2, "b": 2, "c": (inc, "a"), "d": (add, "a", "b"), "e": (minus, "d", "c")}

        client = Client(self.address)

        with self.assertRaises(ValueError):
            client.get(graph, ["e"])

import unittest

from scaled.system.objects import ClientJobType, Task
from scaled.system.worker_manager.tools.worker_collection import WorkerCollection


class TestWorkerCollection(unittest.TestCase):
    def test_worker_collection(self):
        collection = WorkerCollection()
        self.assertEqual(collection.size(), 0)
        self.assertEqual(collection.capacity(), 0)

        collection[b"a"] = None
        self.assertEqual(collection.size(), 1)
        self.assertEqual(collection.capacity(), 1)

        collection[b"b"] = None
        self.assertEqual(collection.size(), 2)
        self.assertEqual(collection.capacity(), 2)

        collection[b"c"] = None
        self.assertEqual(collection.size(), 3)
        self.assertEqual(collection.capacity(), 3)

        collection[b"a"] = Task(1, ClientJobType.Map.value, b"", [b""])
        self.assertEqual(collection.size(), 3)
        self.assertEqual(collection.capacity(), 2)

        collection[b"b"] = Task(2, ClientJobType.Map.value, b"", [b""])
        self.assertEqual(collection.size(), 3)
        self.assertEqual(collection.capacity(), 1)

        collection[b"d"] = None
        self.assertEqual(collection.size(), 4)
        self.assertEqual(collection.capacity(), 2)

        collection[b"a"] = None
        self.assertEqual(collection.size(), 4)
        self.assertEqual(collection.capacity(), 3)

        collection.pop(b"d")
        self.assertEqual(collection.size(), 3)
        self.assertEqual(collection.capacity(), 2)

        collection.pop(b"b")
        self.assertEqual(collection.size(), 2)
        self.assertEqual(collection.capacity(), 2)



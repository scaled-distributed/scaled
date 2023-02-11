import unittest

from scaled.scheduler.worker_manager.allocators.one_to_one import WorkerCollection


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

        collection[b"a"] = b"1"
        self.assertEqual(collection.size(), 3)
        self.assertEqual(collection.capacity(), 2)

        collection[b"b"] = b"2"
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

    def test_worker_collection_in(self):
        collection = WorkerCollection()
        collection[b"a"] = None
        self.assertTrue(collection.has_worker(b"a"))
        self.assertFalse(collection.has_worker(b"w"))

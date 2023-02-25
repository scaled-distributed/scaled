import unittest

from scaled.scheduler.worker_manager.allocators.queued import CapacityPriorityQueue


class TestCapacity(unittest.TestCase):
    def test_capacity(self):
        queue = CapacityPriorityQueue()
        queue.push_worker(0, b"foo")
        queue.push_worker(0, b"bar")

        count, worker = queue.pop_worker()
        queue.push_worker(count + 1, worker)
        self.assertEqual(worker, b"bar")

        count, worker = queue.pop_worker()
        queue.push_worker(count + 1, worker)
        self.assertEqual(worker, b"foo")

        count, worker = queue.pop_worker()
        queue.push_worker(count + 1, worker)
        self.assertEqual(worker, b"bar")

        count, worker = queue.pop_worker()
        queue.push_worker(count + 1, worker)
        self.assertEqual(worker, b"foo")

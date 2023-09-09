import heapq
from asyncio import Queue
from typing import Dict
from typing import List
from typing import Union


class PriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first).

    Entries are typically list of the form: [priority number, data].
    """

    def __len__(self):
        return len(self._queue)

    def _init(self, maxsize):
        self._queue: List[List[Union[int, bytes]]] = []
        self._locator: Dict[bytes, List[Union[int, bytes]]] = {}

    def _put(self, item: List[Union[int, bytes]]):
        heapq.heappush(self._queue, item)
        self._locator[item[1]] = item  # type: ignore

    def _get(self):
        priority, item = heapq.heappop(self._queue)
        self._locator.pop(item)  # type: ignore
        return priority, item

    def remove(self, data):
        # this operation is O(log(n)), first change priority to -1 and pop from top of the heap, mark it as invalid
        # entry in the heap is not good idea as those invalid, entry will never get removed, so we used heapq internal
        # function _siftdown to maintain min heap invariant
        entry = self._locator.pop(data)
        i = self._queue.index(entry)
        entry[0] = -1
        heapq._siftdown(self._queue, 0, i)  # type: ignore # noqa
        assert heapq.heappop(self._queue) == entry

    def decrease_priority(self, item):
        # this operation should be O(log(n)), mark it as invalid entry in the heap is not good idea as those invalid
        # entry will never get removed, so we used heapq internal function _siftdown to maintain min heap invariant
        entry = self._locator[item]
        i = self._queue.index(entry)
        entry[0] -= 1  # type: ignore
        heapq._siftdown(self._queue, 0, i)  # type: ignore # noqa

    def max_priority(self):
        priority, item = heapq.heappop(self._queue)
        heapq.heappush(self._queue, [priority, item])
        return priority

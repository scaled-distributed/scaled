import dataclasses
from asyncio import Queue, QueueEmpty
from typing import Any, Dict, Hashable, Optional


class IndexedQueue(Queue):
    """This should have same set of features as asyncio.Queue, with additional methods like remove
    - it behaves like regular async queue, except:
      - all the items pushed to queue should be hashable
      - those items should be unique in queue
    - IndexedQueue.put(), IndexedQueue.get(), IndexedQueue.remove() should all take O(1) time complexity
    """

    def __contains__(self, item):
        return item in self._queue

    def __len__(self):
        return self._queue.__len__()

    def _init(self, maxsize):
        self._queue = _IndexedDoubleLinkedQueue()

    def _put(self, item: Hashable):
        self._queue.put(item)

    def _get(self):
        return self._queue.get()

    def remove(self, item: Hashable):
        """remove the item in the queue in O(1) time complexity"""
        self._queue.remove(item)


@dataclasses.dataclass
class _Node:
    value: Any
    prev: Optional["_Node"] = None
    next: Optional["_Node"] = None


class _DoubleLinkedList:
    def __init__(self):
        self._head: Optional[_Node] = None
        self._tail: Optional[_Node] = None
        self._size = 0

    def __len__(self):
        return self._size

    def add_to_head(self, node: _Node):
        if self._head is None:
            self._head = node
            self._tail = node
        else:
            node.next = self._head
            self._head.prev = node
            self._head = node

        self._size += 1

    def remove_tail(self):
        if self._tail is None:
            raise QueueEmpty(f"{self.__class__.__name__} queue empty")

        node = self._tail
        if self._tail.prev is None:
            self._head = None
            self._tail = None
        else:
            self._tail = self._tail.prev
            self._tail.next = None

        self._size -= 1
        return node

    def remove(self, node: _Node):
        prev_node = node.prev
        next_node = node.next
        if prev_node and next_node:
            prev_node.next = next_node
            next_node.prev = prev_node

        elif not prev_node and not next_node:
            assert self._head is node
            assert self._tail is node
            self._head = None
            self._tail = None

        elif prev_node and not next_node:
            assert self._tail is node
            prev_node.next = None
            self._tail = prev_node

        elif not prev_node and next_node:
            assert self._head is node
            next_node.prev = None
            self._head = next_node

        self._size -= 1
        del node


class _IndexedDoubleLinkedQueue:
    def __init__(self):
        self._double_linked_list = _DoubleLinkedList()
        self._hash_map: Dict[int, _Node] = {}

    def __contains__(self, item: Hashable):
        key = hash(item)
        return key in self._hash_map

    def __len__(self):
        return self._double_linked_list.__len__()

    def put(self, item: Hashable):
        key = hash(item)
        if key in self._hash_map:
            raise KeyError(f"{self.__class__.__name__} already have item: {item}")

        node = _Node(item)
        self._double_linked_list.add_to_head(node)
        self._hash_map[key] = node

    def get(self):
        node = self._double_linked_list.remove_tail()
        del self._hash_map[hash(node.value)]
        return node.value

    def remove(self, item: Hashable):
        key = hash(item)
        if key not in self._hash_map:
            raise KeyError(f"{self.__class__.__name__} doesn't have item: {item}")

        node = self._hash_map.pop(key)
        self._double_linked_list.remove(node)

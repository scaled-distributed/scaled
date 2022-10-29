from typing import Dict, Generator, Optional, Set

from scaled.protocol.python import WorkerTask


class WorkerCollection:
    def __init__(self):
        self._used_workers: Dict[bytes, WorkerTask] = {}
        self._unused_workers: Set[bytes] = set()

    def __setitem__(self, worker: bytes, task: Optional[WorkerTask]):
        if task is not None:
            if worker in self._used_workers:
                raise ValueError(f"assign {task=} to {worker=} while it still processing {self._used_workers[worker]}")

            self._unused_workers.remove(worker)
            self._used_workers[worker] = task
        else:
            if worker in self._used_workers:
                self._used_workers.pop(worker)

            self._unused_workers.add(worker)

    def __getitem__(self, worker: bytes) -> Optional[WorkerTask]:
        return self._used_workers.get(worker, None)

    def keys(self) -> Generator[bytes, None, None]:
        yield from self._unused_workers
        yield from self._used_workers.keys()

    def pop(self, worker: bytes) -> Optional[WorkerTask]:
        if worker not in self._used_workers and worker not in self._unused_workers:
            raise KeyError(f"worker not exists: {worker}")

        if worker in self._used_workers:
            return self._used_workers.pop(worker)

        self._unused_workers.remove(worker)
        return None

    def size(self) -> int:
        return len(self._used_workers) + len(self._unused_workers)

    def capacity(self) -> int:
        return len(self._unused_workers)

    def full(self) -> bool:
        return self.capacity() == 0

    def get_worker(self) -> Optional[bytes]:
        if self.capacity():
            return next(iter(self._unused_workers))

        return None

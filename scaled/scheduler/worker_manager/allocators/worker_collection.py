from typing import Dict, Generator, Optional, Set


class WorkerCollection:
    def __init__(self):
        self._used_workers: Dict[bytes, bytes] = {}
        self._unused_workers: Set[bytes] = set()

    def __setitem__(self, worker: bytes, task: Optional[bytes]):
        assert isinstance(worker, bytes)
        if task is not None:
            if worker in self._used_workers:
                raise ValueError(f"assign {task=} to {worker=} while it still processing {self._used_workers[worker]}")

            self._unused_workers.remove(worker)
            self._used_workers[worker] = task
        else:
            if worker in self._used_workers:
                self._used_workers.pop(worker)

            self._unused_workers.add(worker)

    def __getitem__(self, worker: bytes) -> Optional[bytes]:
        if not isinstance(worker, bytes):
            raise TypeError(f"Expect worker to be type bytes, got: type={type(worker)} {worker=}")

        return self._used_workers.get(worker, None)

    def keys(self) -> Generator[bytes, None, None]:
        yield from self._unused_workers
        yield from self._used_workers.keys()

    def pop(self, worker: bytes) -> Optional[bytes]:
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

    def has_worker(self, worker: bytes) -> bool:
        if worker in self._used_workers:
            return True

        if worker in self._unused_workers:
            return True

        return False

    def get_one_unused_worker(self) -> Optional[bytes]:
        if self.capacity():
            return next(iter(self._unused_workers))

        return None

    def get_unused_workers(self) -> Set[bytes]:
        return self._unused_workers

    def get_used_workers(self) -> Dict[bytes, bytes]:
        return self._used_workers

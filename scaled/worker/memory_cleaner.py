import ctypes
import gc
import multiprocessing
import threading
import time

import psutil


class MemoryCleaner(threading.Thread):
    def __init__(
        self, stop_event: threading.Event, garbage_collect_interval_seconds: int, trim_memory_threshold_bytes: int
    ):
        threading.Thread.__init__(self)

        self._stop_event = stop_event
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._previous_garbage_collect_time = time.time()
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._process = psutil.Process(multiprocessing.current_process().pid)

        gc.disable()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.clean()

    def clean(self):
        if time.time() - self._previous_garbage_collect_time < self._garbage_collect_interval_seconds:
            time.sleep(0.1)
            return

        self._previous_garbage_collect_time = time.time()

        gc.collect()

        if self._process.memory_info().rss < self._trim_memory_threshold_bytes:
            return

        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)

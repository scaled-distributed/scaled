import ctypes
import gc
import multiprocessing
import platform
import threading
import time
from typing import Callable, Dict, Optional

import psutil

from scaled.io.config import CLEANUP_INTERVAL_SECONDS


class CacheCleaner(threading.Thread):
    def __init__(
        self, garbage_collect_interval_seconds: int, trim_memory_threshold_bytes: int, function_retention_seconds: int
    ):
        threading.Thread.__init__(self)

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._previous_garbage_collect_time = time.time()
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._function_retention_seconds = function_retention_seconds

        self._cached_functions: Dict[bytes, Callable] = {}
        self._cached_functions_alive_since: Dict[bytes, float] = dict()
        self._process = psutil.Process(multiprocessing.current_process().pid)
        self._libc = ctypes.cdll.LoadLibrary("libc.{}".format("so.6" if platform.uname()[0] != "Darwin" else "dylib"))

    def run(self) -> None:
        while True:
            self.__clean_function()
            self.__clean_memory()
            time.sleep(CLEANUP_INTERVAL_SECONDS)

    def add_function(self, function_id: bytes, function: Callable):
        self._cached_functions[function_id] = function
        self._cached_functions_alive_since[function_id] = time.time()

    def del_function(self, function_id: bytes):
        self._cached_functions_alive_since.pop(function_id)
        self._cached_functions.pop(function_id)

    def get_function(self, function_id: bytes) -> Optional[Callable]:
        function = self._cached_functions.get(function_id)
        if function is None:
            return None

        self._cached_functions_alive_since[function_id] = time.time()
        return function

    def __clean_function(self):
        now = time.time()
        idle_functions = [
            function_id
            for function_id, alive_since in self._cached_functions_alive_since.items()
            if now - alive_since > self._function_retention_seconds
        ]
        for function_id in idle_functions:
            self.del_function(function_id)

    def __clean_memory(self):
        if time.time() - self._previous_garbage_collect_time < self._garbage_collect_interval_seconds:
            return

        self._previous_garbage_collect_time = time.time()

        gc.collect()

        if self._process.memory_info().rss < self._trim_memory_threshold_bytes:
            return

        self._libc.malloc_trim(0)

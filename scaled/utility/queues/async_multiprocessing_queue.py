import asyncio
import multiprocessing
from concurrent.futures import ThreadPoolExecutor


class AsyncMultiprocessingQueue:
    def __init__(self, queue: multiprocessing.Queue):
        self._queue = queue
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._loop = None

    async def put(self, item):
        return self._queue.put(item)

    async def get(self):
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        result = await self._loop.run_in_executor(self._executor, self._queue.get)
        return result

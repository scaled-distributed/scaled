import asyncio
import multiprocessing
import unittest

from scaled.io.config import ZMQConfig, ZMQType
from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import Heartbeat, Message
from scaled.protocol.python.objects import MessageType
from scaled.worker.worker import Worker


class TestWorker(unittest.TestCase):
    def test_worker_heartbeat(self):
        async def callback(to: bytes, message_type: MessageType, message: Message):
            print(message)

        async def main(d):
            await asyncio.gather(d.routine())

        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12346)

        stop_event = multiprocessing.get_context("spawn").Event()
        worker = Worker(stop_event=stop_event, address=config, polling_time=1, heartbeat_interval=1)
        worker.start()

        driver = AsyncBinder(stop_event=stop_event, prefix="Backend", address=config)
        driver.register(callback)
        asyncio.run(main(driver))

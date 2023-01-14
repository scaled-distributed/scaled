import asyncio
import multiprocessing
import unittest
from typing import List

from scaled.worker.worker import Worker
from scaled.scheduler.io.zmq_binder import ZMQBinder
from scaled.io.config import ZMQConfig, ZMQType
from scaled.protocol.python.message import Heartbeat



class TestWorker(unittest.TestCase):
    def test_worker_heartbeat(self):
        async def callback(to: bytes, message_type: bytes, data: List[bytes]):
            print(Heartbeat.deserialize(data))

        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12346)

        stop_event = multiprocessing.get_context("spawn").Event()
        worker = Worker(stop_event=stop_event, address=config, polling_time=0, heartbeat_interval=5)
        worker.start()

        async_stop_event = asyncio.Event()
        driver = ZMQBinder(stop_event=async_stop_event, prefix="Backend", address=config)
        driver.register(callback)
        asyncio.run(driver.loop())


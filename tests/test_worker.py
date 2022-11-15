import asyncio
import multiprocessing
import unittest
from typing import List

from scaled.worker.single_worker import SingleWorker
from scaled.scheduler.io.zmq_binder import ZMQBinder
from scaled.io.config import ZMQConfig, ZMQType
from scaled.protocol.python.message import Heartbeat


class TestWorker(unittest.TestCase):
    def test_worker(self):
        async def callback(to: bytes, message_type: bytes, data: List[bytes]):
            print(Heartbeat.deserialize(data))

        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12346)

        stop_event = multiprocessing.get_context("spawn").Event()
        worker = SingleWorker(address=config, stop_event=stop_event, polling_time=0, heartbeat_interval=5)
        worker.start()

        async_stop_event = asyncio.Event()
        driver = ZMQBinder("Backend", address=config, stop_event=async_stop_event)
        driver.register(callback)
        asyncio.run(driver.loop())

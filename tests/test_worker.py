import asyncio
import multiprocessing
import unittest

from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import MessageType, MessageVariant
from scaled.worker.worker import Worker


class TestWorker(unittest.TestCase):
    def test_worker_heartbeat(self):
        async def callback(to: bytes, message_type: MessageType, message: MessageVariant):
            print(message)

        config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=12346)

        stop_event = multiprocessing.get_context("spawn").Event()
        worker = Worker(
            stop_event=stop_event,
            address=config,
            heartbeat_interval_seconds=1,
            event_loop="builtin",
            serializer=DefaultSerializer(),
        )
        worker.start()

        driver = AsyncBinder(prefix="Backend", address=config)
        driver.register(callback)
        asyncio.run(driver.routine())

import asyncio
import threading
import time
import unittest

import zmq
import zmq.asyncio


def worker(context: zmq.Context):
    socket = context.socket(zmq.PAIR)
    socket.setsockopt(zmq.SNDHWM, 0)
    socket.setsockopt(zmq.RCVHWM, 1)
    socket.connect("inproc://example")

    while True:
        print("worker received", socket.recv_multipart())
        time.sleep(5)
        socket.send_multipart([b"worker", b"message", b"server1"])


class TestZMQ(unittest.TestCase):
    def test_high_watermark(self):
        context = zmq.Context()

        def agent():
            socket = context.socket(zmq.PAIR)
            socket.setsockopt(zmq.SNDHWM, 1)
            socket.setsockopt(zmq.RCVHWM, 0)
            socket.bind("inproc://example")

            while True:
                socket.send_multipart([b"agent", b"send"])
                count = socket.poll(0.1)
                if not count:
                    continue

                print("agent received", socket.recv_multipart())

        threading.Thread(target=agent).start()
        threading.Thread(target=worker, args=(context,)).start()

        time.sleep(10)

    def test_high_watermark_async(self):
        context = zmq.asyncio.Context()
        sync_context = zmq.Context.shadow(context)

        async def agent():
            socket = context.socket(zmq.PAIR)
            socket.setsockopt(zmq.SNDHWM, 1)
            socket.setsockopt(zmq.RCVHWM, 0)
            socket.bind("inproc://example")

            while True:
                await socket.send_multipart([b"agent", b"send"])
                count = await socket.poll(0.1)
                if not count:
                    continue

                print("agent received", await socket.recv_multipart())

        threading.Thread(target=worker, args=(sync_context,)).start()
        loop = asyncio.get_event_loop()
        loop.create_task(agent())
        loop.run_forever()

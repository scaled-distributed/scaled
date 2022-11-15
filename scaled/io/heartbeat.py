import threading
import time
import typing

import psutil
import zmq

from scaled.io.config import ZMQConfig
from scaled.protocol.python.objects import MessageType
from scaled.protocol.python.message import Heartbeat


class WorkerHeartbeat(threading.Thread):
    def __init__(self, address: ZMQConfig, worker_identity: bytes, interval: int, stop_event: threading.Event):
        threading.Thread.__init__(self)

        self._address = address
        self._worker_identity = worker_identity
        self._interval = interval
        self._stop_event = stop_event

        self._context: typing.Optional[zmq.Context] = None
        self._socket: typing.Optional[zmq.Socket] = None

    def run(self) -> None:
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.IDENTITY, self._worker_identity + b"|HB")
        self._socket.connect(self._address.to_address())

        try:
            while not self._stop_event.is_set():
                time.sleep(self._interval)
                self._socket.send_multipart(
                    [
                        MessageType.Heartbeat.value,
                        *Heartbeat(self._worker_identity, psutil.cpu_percent() / 100).serialize(),
                    ]
                )
        finally:
            self._socket.close()


def start_heartbeat(address: ZMQConfig, worker_identity: bytes, interval: int, stop_event: threading.Event):
    thread = WorkerHeartbeat(address, worker_identity, interval, stop_event)
    thread.start()
    return thread
